/*-------------------------------------------------------------------------
 *
 * pg_stat_counters.c
 *		statistics of query calls across a whole database cluster.
 *
 * Copyright (c) 2021, Alexey E. Konorev <alexey.konorev@gmail.com>
 *
 * Portions Copyright (c) 2008-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  pg_stat_counters/pg_stat_counters.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/resource.h>

#if PG_VERSION_NUM >= 160000
#ifndef WIN32
#define HAVE_GETRUSAGE
#endif		/* HAVE_GETRUSAGE */
#endif		/* pg16+ */

#if PG_VERSION_NUM < 160000
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif		/* HAVE_SYS_RESOURCE_H */

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif		/* !HAVE_GETRUSAGE */
#endif		/* pg16- */

#if PG_VERSION_NUM >= 160000
#include "utils/pg_rusage.h"
#endif

#if PG_VERSION_NUM >= 150000
#include "jit/jit.h"
#endif          

#if PG_VERSION_NUM >= 140000
#include "access/parallel.h"
#endif

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 130000
#include "optimizer/planner.h"
#endif
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"  /* for check the database and role exists */
#include "utils/timestamp.h"
#include <time.h>
//#include "funcapi.h"


PG_MODULE_MAGIC;

/* Location of permanent stats file (valid when database is shut down) */
#define PGSC_DUMP_FILE PGSTAT_STAT_PERMANENT_DIRECTORY "/pg_stat_counters.stat"

/* Magic number identifying the stats file format */
static const uint32 PGSC_FILE_HEADER = 0x20191219;

/* PostgreSQL major version number, changes in which invalidate all entries */
static const uint32 PGSC_PG_MAJOR_VERSION = PG_VERSION_NUM / 100;

/* PGSC */
#define PGSC_DEALLOC_PERCENT       5            /* free this % of entries at once */

typedef enum pgscStoreKind
{
        PGSC_INVALID = -1,

        /*
         * PGSC_PLAN and PGSC_EXEC must be respectively 0 and 1 as they're used to
         * reference the underlying values in the arrays in the Counters struct,
         * and this order is required in pg_stat_counters_internal().
         */
        PGSC_PLAN = 0,
        PGSC_EXEC,
} pgscStoreKind;

#define PGSC_NUMKIND (PGSC_EXEC + 1)


/*
 * Hashtable key that defines the identity of a hashtable entry.  We separate
 * calls by user, by database and by operation (select, insert, update,..)
 *
 */
typedef struct pgscHashKey
{
	Oid             userid;                 /* user OID */
	Oid             dbid;                   /* database OID */
	CmdType         operation;              /* CMD_SELECT, CMD_UPDATE, etc. */
} pgscHashKey;

/*
 * The actual stats counters kept within pgscEntry.
 */
typedef struct Counters
{
	/* calls statistics */
	int64           calls_1_2s;             /* # of times with execution time 1-2 sec */
	int64           calls_2_3s;             /* 2-3 sec and so on ... */
	int64           calls_3_5s;
	int64           calls_5_10s;
	int64           calls_10_20s;
	int64           calls_gt20s;            /* >20 sec */
        int64           calls[PGSC_NUMKIND];    /* # of times planned/executed */
	double          total_time[PGSC_NUMKIND];       /* total planning/execution time,
                                                                           * in msec */
	int64           rows;                   /* total # of retrieved or affected rows */
	/* blocks statistics */
	int64           shared_blks_hit;        /* # of shared buffer hits */
	int64           shared_blks_read;       /* # of shared disk blocks read */
	int64           shared_blks_dirtied;    /* # of shared disk blocks dirtied */
	int64           shared_blks_written;    /* # of shared disk blocks written */
	int64           local_blks_hit;         /* # of local buffer hits */
	int64           local_blks_read;        /* # of local disk blocks read */
	int64           local_blks_dirtied;     /* # of local disk blocks dirtied */
	int64           local_blks_written;     /* # of local disk blocks written */
	int64           temp_blks_read;         /* # of temp blocks read */
	int64           temp_blks_written;      /* # of temp blocks written */
//#if (PG_VERSION_NUM >= 170000)
        double          shared_blk_read_time;   /* time spent reading shared blocks,
                                                                                 * in msec */
        double          shared_blk_write_time;  /* time spent writing shared blocks,
                                                                                 * in msec */
        double          local_blk_read_time;    /* time spent reading local blocks, in
                                                                                 * msec */
        double          local_blk_write_time;   /* time spent writing local blocks, in
                                                                                 * msec */
//#else
	double          blk_read_time;          /* time spent reading, in msec */
	double          blk_write_time;         /* time spent writing, in msec */
//#endif
//#if (PG_VERSION_NUM >= 130000)
        double          temp_blk_read_time; /* time spent reading temp blocks, in msec */
        double          temp_blk_write_time;    /* time spent writing temp blocks, in
                                                                                * msec */
//#endif
	/* wal statistics. version 13 and above */
//#if (PG_VERSION_NUM >= 130000)
	int64           wal_records;            /* # of WAL records generated */
	int64           wal_fpi;                /* # of WAL full page images generated */
	uint64          wal_bytes;              /* total amount of WAL bytes generated */
//#endif
//#if (PG_VERSION_NUM >= 180000)
	int64		wal_buffers_full;	/* # of times the WAL buffers became full */
//#endif
	/* jit statistics. version 15 and above */
//#if (PG_VERSION_NUM >= 150000)
	int64		jit_functions;		/* total number of JIT functions emitted */
	double		jit_generation_time;	/* total time to generate jit code */
	int64		jit_inlining_count;	/* number of times inlining time has been
						  			* > 0 */
	double		jit_deform_time;	/* total time to deform tuples in jit code */
	int64		jit_deform_count;	/* number of times deform time has been
						 			* > 0 */
	double		jit_inlining_time;	/* total time to inline jit code */
	int64		jit_optimization_count;	/* number of times optimization time has been
						  			* > 0 */
	double		jit_optimization_time;  /* total time to optimize jit code */
	int64		jit_emission_count;	/* number of times emission time has been
						 			* > 0 */
	double		jit_emission_time;	/* total time to emit jit code */
//#endif
	/* version 18 and above */
//#if (PG_VERSION_NUM >= 180000)
	int64		parallel_workers_to_launch;	/* # of parallel workers planned
									* to be launched */
	int64		parallel_workers_launched;	/* # of parallel workers actually
							  		* launched */
//#endif
	/* system statistics */
	double          utime;                  /* CPU user time in msec */
	double          stime;                  /* CPU system time in msec */
#ifdef HAVE_GETRUSAGE
	/* These fields are only used for platform with HAVE_GETRUSAGE defined */
	int64           minflts;                /* page reclaims (soft page faults) */
	int64           majflts;                /* page faults (hard page faults) */
	int64           nswaps;                 /* page faults (hard page faults) */
	int64           reads;                  /* Physical block reads */
	int64           writes;                 /* Physical block writes */
	int64           msgsnds;                /* IPC messages sent */
	int64           msgrcvs;                /* IPC messages received */
	int64           nsignals;               /* signals received */
	int64           nvcsws;                 /* voluntary context witches */
	int64           nivcsws;                /* unvoluntary context witches */
#endif
	/* internal usage */
	TimestampTz     _first_change;
	TimestampTz     _last_change;           /* also use as last_time column */
} Counters;

/*
 * Global statistics for pg_stat_counters
 */
typedef struct pgscGlobalStats
{
	int64           dealloc;                /* # of times entries were deallocated */
	TimestampTz     stats_reset;            /* timestamp with all stats reset */
} pgscGlobalStats;

/*
 * Statistics per key
 *
 */
typedef struct pgscEntry
{
	pgscHashKey     key;                    /* hash key of entry - MUST BE FIRST */
	Counters        counters;               /* the statistics for this key */
	slock_t         mutex;                  /* protects the counters only */
} pgscEntry;

/*
 * Statistics of whole database cluster
 *
 */
typedef struct aggEntry
{
	Counters        counters;               /* the aggregate statistics of all calls */
	slock_t         mutex;                  /* protects the counters only */
} aggEntry;

/*
 * Global shared state
 */
typedef struct pgscSharedState
{
	LWLock          *lock;                  /* protects hashtable search/modification */
	slock_t         mutex;                  /* protects following fields only: */
	int64           last_skipped;
	pgscGlobalStats stats;                  /* global statistics for pgsc */

} pgscSharedState;

typedef struct SysInfo
{
	double          utime;                  /* user cpu time */
	double          stime;                  /* system cpu time */
#ifdef HAVE_GETRUSAGE
	int64           minflts;                /* page reclaims (soft page faults) */
	int64           majflts;                /* page faults (hard page faults) */
	int64           nswaps;                 /* page faults (hard page faults) */
	int64           reads;                  /* Physical block reads */
	int64           writes;                 /* Physical block writes */
	int64           msgsnds;                /* IPC messages sent */
	int64           msgrcvs;                /* IPC messages received */
	int64           nsignals;               /* signals received */
	int64           nvcsws;                 /* voluntary context witches */
	int64           nivcsws;                /* unvoluntary context witches */
#endif
} SysInfo;

/*---- Local variables ----*/

/* Current nesting depth of planner/ExecutorRun/ProcessUtility calls */
static int      nesting_level = 0;

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
static struct rusage rusage_start;
static struct rusage rusage_end;

/* Saved hook values in case of unload */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM >= 130000
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
#endif
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Links to shared memory state */
static pgscSharedState *pgsc = NULL;
static HTAB *pgsc_hash = NULL;
static aggEntry *pgsc_aggs = NULL;


/*---- GUC variables ----*/

typedef enum
{
	PGSC_TRACK_NONE, /* track no statements */
	PGSC_TRACK_TOP,  /* only top level statements */
	PGSC_TRACK_ALL   /* all statements, including nested ones */
} PGSCTrackLevel;

static const struct config_enum_entry track_options[] =
{
	{"none", PGSC_TRACK_NONE, false},
	{"top", PGSC_TRACK_TOP, false},
	{"all", PGSC_TRACK_ALL, false},
	{NULL, 0, false}
};

static int    pgsc_max;            /* max # statements to track */
static int    pgsc_track;          /* tracking level */
#if PG_VERSION_NUM >= 130000 
static bool   pgsc_track_planning; /* whether to track planning duration */
#endif
static bool   pgsc_track_utility;  /* whether to track utility commands */
static bool   pgsc_save;           /* whether to save stats across shutdown */
static int    pgsc_linux_hz;       /* Inform pg_stat_counters of the linux CONFIG_HZ config option,
                                    * This is used by pg_stat_counters to compensate for sampling
                                    * errors in getrusage due to the kernel adhering to its ticks.
                                    * The default value, -1, tries to guess it at startup.
                                    */

#if PG_VERSION_NUM >= 140000 
#define pgsc_enabled(level) \
        (!IsParallelWorker() && \
        (pgsc_track == PGSC_TRACK_ALL || \
        (pgsc_track == PGSC_TRACK_TOP && (level) == 0)))
#else
#define pgsc_enabled(level) \
        (pgsc_track == PGSC_TRACK_ALL || \
        (pgsc_track == PGSC_TRACK_TOP && (level) == 0))
#endif

#define pgsc_reset() \
	do { \
		volatile pgscSharedState *s = (volatile pgscSharedState *)pgsc; \
		TimestampTz stats_reset = GetCurrentTimestamp(); \
		SpinLockAcquire(&s->mutex); \
		s->stats.dealloc = 0; \
		s->stats.stats_reset = stats_reset; \
		s->last_skipped = 0; \
		SpinLockRelease(&s->mutex); \
	} while (0)

#define DIFF_TIME_MILLISEC(e, s) \
        (((double)(e).tv_sec * 1000.0) + ((double)(e).tv_usec) / 1000.0) - \
        (((double)(s).tv_sec * 1000.0) + ((double)(s).tv_usec) / 1000.0)

/* ru_inblock block size is 512 bytes with Linux
 * see http://lkml.indiana.edu/hypermail/linux/kernel/0703.2/0937.html
 */
#define RUSAGE_BLOCK_SIZE 512 /* Size of a block for getrusage() */

/*---- Function declarations ----*/

void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(pg_stat_counters_reset);
PG_FUNCTION_INFO_V1(pg_stat_counters);
PG_FUNCTION_INFO_V1(pg_stat_counters_all);
PG_FUNCTION_INFO_V1(pg_stat_counters_info);
PG_FUNCTION_INFO_V1(get_cmd_name);

#if PG_VERSION_NUM >= 150000
static void pgsc_shmem_request(void);
#endif
static void pgsc_shmem_startup(void);
static void pgsc_shmem_shutdown(int code, Datum arg);
#if PG_VERSION_NUM >= 140000
static void pgsc_post_parse_analyze(ParseState *pstate,
		                    Query *query,
                                    JumbleState *jstate);
static PlannedStmt *pgsc_planner(Query *parse,
                                 const char *query_string,
                                 int cursorOptions,
                                 ParamListInfo boundParams);
#endif
static void pgsc_ExecutorStart(QueryDesc *queryDesc, int eflags);

/* pgsc_ProcessUtility */
#if (PG_VERSION_NUM >= 140000)
static void pgsc_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                bool readOnlyTree,
                                ProcessUtilityContext context, ParamListInfo params,
                                QueryEnvironment *queryEnv,
                                DestReceiver *dest, QueryCompletion *qc);
#elif (PG_VERSION_NUM >= 130000)
static void pgsc_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                ProcessUtilityContext context, ParamListInfo params,
                                QueryEnvironment *queryEnv,
                                DestReceiver *dest, QueryCompletion *qc);
#elif (PG_VERSION_NUM >= 100000)
static void pgsc_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                ProcessUtilityContext context, ParamListInfo params,
                                QueryEnvironment *queryEnv,
                                DestReceiver *dest, char *completionTag);
#else
static void pgsc_ProcessUtility(Node *parsetree, const char *queryString,
                                ProcessUtilityContext context, ParamListInfo params,
                                DestReceiver *dest, char *completionTag);
#endif
/* pgsc_ExecutorRun */
#if (PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000)
static void pgsc_ExecutorRun(QueryDesc *queryDesc,
                             ScanDirection direction,
                             uint64 count, bool execute_once);
#elif (PG_VERSION_NUM >= 90600 || PG_VERSION_NUM >= 180000 )
static void pgsc_ExecutorRun(QueryDesc *queryDesc,
                             ScanDirection direction,
                             uint64 count);
#else
static void pgsc_ExecutorRun(QueryDesc *queryDesc,
                             ScanDirection direction,
                             long count);
#endif
static void pgsc_ExecutorFinish(QueryDesc *queryDesc);
static void pgsc_ExecutorEnd(QueryDesc *queryDesc);


static bool pgsc_assign_linux_hz_check_hook(int *newval, void **extra, GucSource source);
#if (PG_VERSION_NUM < 130000)
static void BufferUsageAccumDiff(BufferUsage* bufusage, BufferUsage* pgBufferUsage, BufferUsage* bufusage_start);
#endif
#if (PG_VERSION_NUM < 90600)
static uint32 pgsc_hash_fn(const void *key, Size keysize);
static int pgsc_match_fn(const void *key1, const void *key2, Size keysize);
#endif
static Size pgsc_memsize(void);
static pgscEntry *entry_alloc(pgscHashKey *key);
static void entry_dealloc(void);
static void entry_reset(void);
static void aggstats_reset(void);
static void pgsc_store(CmdType operation
                       , pgscStoreKind kind
                       , double total_time, uint64 rows
                       , SysInfo *sys_info, const BufferUsage *bufusage
#if (PG_VERSION_NUM >= 130000)
                       , const WalUsage *walusage
#endif
#if (PG_VERSION_NUM >= 150000)
                       , const struct JitInstrumentation *jitusage
#endif
#if (PG_VERSION_NUM >= 180000)
                       , int parallel_workers_to_launch
                       , int parallel_workers_launched
#endif
);

/*
 * Module load callback
 */
void _PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_stat_counters functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomIntVariable("pg_stat_counters.max",
	                        "Sets the maximum number of key tracked by pg_stat_counters.",
	                        NULL,
	                        &pgsc_max,
	                        100,
	                        10,
	                        INT_MAX,
	                        PGC_POSTMASTER,
	                        0,
	                        NULL,
	                        NULL,
	                        NULL);

	DefineCustomBoolVariable("pg_stat_counters.save",
	                         "Save pg_stat_counters statistics across server shutdowns.",
	                         NULL,
	                         &pgsc_save,
	                         true,
	                         PGC_SIGHUP,
	                         0,
	                         NULL,
	                         NULL,
	                         NULL);

	DefineCustomEnumVariable("pg_stat_counters.track",
	                         "Selects which statements are tracked by pg_stat_counters.",
	                         NULL,
	                         &pgsc_track,
	                         PGSC_TRACK_TOP,
	                         track_options,
	                         PGC_SUSET,
	                         0,
	                         NULL,
	                         NULL,
	                         NULL);

	DefineCustomBoolVariable("pg_stat_counters.track_utility",
	                         "Selects whether utility commands are tracked by pg_stat_counters.",
	                         NULL,
	                         &pgsc_track_utility,
	                         true,
	                         PGC_SUSET,
	                         0,
	                         NULL,
	                         NULL,
	                         NULL);
#if PG_VERSION_NUM >= 130000
        DefineCustomBoolVariable("pg_stat_counters.track_planning",
                                 "Selects whether planning duration is tracked by pg_stat_counters.",
                                 NULL,
                                 &pgsc_track_planning,
                                 false,
                                 PGC_SUSET,
                                 0,
                                 NULL,
                                 NULL,
                                 NULL);
#endif
	DefineCustomIntVariable("pg_stat_counters.linux_hz",
	                        "Inform pg_stat_counters of the linux CONFIG_HZ config option",
	                        "This is used by pg_stat_counters to compensate for sampling errors "
	                        "in getrusage due to the kernel adhering to its ticks. The default value, -1, "
	                        "tries to guess it at startup. ",
	                        &pgsc_linux_hz,
	                        -1,
	                        -1,
	                        INT_MAX,
	                        PGC_USERSET,
	                        0,
	                        pgsc_assign_linux_hz_check_hook,
	                        NULL,
	                        NULL);

#if PG_VERSION_NUM >= 150000
	MarkGUCPrefixReserved("pg_stat_counters");
#else
	EmitWarningsOnPlaceholders("pg_stat_counters");
#endif

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.) PG15 uses a shmem_request hook.
	 * We'll allocate or attach to the shared resources in pgsc_shmem_startup().
	 */
#if PG_VERSION_NUM < 150000
	RequestAddinShmemSpace(pgsc_memsize());
#if (PG_VERSION_NUM >= 90600)
	RequestNamedLWLockTranche("pg_stat_counters", 1);
#else
	RequestAddinLWLocks(1);
#endif
#endif /* up to PG15 */

	/* Install hooks. */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgsc_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgsc_shmem_startup;
#if PG_VERSION_NUM >= 140000
        prev_post_parse_analyze_hook = post_parse_analyze_hook;
        post_parse_analyze_hook = pgsc_post_parse_analyze;
        prev_planner_hook = planner_hook;
        planner_hook = pgsc_planner;
#endif
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgsc_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgsc_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgsc_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgsc_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgsc_ProcessUtility;
}

/*
 * Module unload callback
 */
void _PG_fini(void)
{
	/* Uninstall hooks. */
#if PG_VERSION_NUM >= 150000
	shmem_request_hook = prev_shmem_request_hook;
#endif
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	ProcessUtility_hook = prev_ProcessUtility;
}

/*
 * shmem_request hook: request additional shared resources. We'll
 * allocate or attach to the shared resources in pgsc_shmem_startup().
 */
#if PG_VERSION_NUM >= 150000
static void
pgsc_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pgsc_memsize());
	RequestNamedLWLockTranche("pg_stat_counters", 1);
}
#endif

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing statistics from file.
 */
static void
pgsc_shmem_startup(void)
{
	bool     found;
	bool     aggfound;
	HASHCTL  info;
	FILE     *file = NULL;
	uint32   header;
	int32    num;
	int32    pgver;
	int32    i;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgsc = NULL;
	pgsc_hash = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgsc = ShmemInitStruct("pg_stat_counters",
	                       sizeof(pgscSharedState),
	                       &found);

	if (!found)
	{
		/* First time through ... */
#if (PG_VERSION_NUM >= 90600)
		pgsc->lock = &(GetNamedLWLockTranche("pg_stat_counters"))->lock;
#else
		pgsc->lock = LWLockAssign();
#endif
		SpinLockInit(&pgsc->mutex);
		pgsc_reset();
	}

	pgsc_aggs = ShmemInitStruct("pg_stat_counters aggs",
	                            sizeof(aggEntry),
	                            &aggfound);

	if (!aggfound)
	{
		SpinLockInit(&pgsc_aggs->mutex);
		memset(&pgsc_aggs->counters, 0, sizeof(Counters));
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgscHashKey);
	info.entrysize = sizeof(pgscEntry);
#if (PG_VERSION_NUM < 90600)
	info.hash = pgsc_hash_fn;
	info.match = pgsc_match_fn;

	pgsc_hash = ShmemInitHash("pg_stat_counters hash",
	                          pgsc_max, pgsc_max,
	                          &info,
	                          HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
#else
	pgsc_hash = ShmemInitHash("pg_stat_counters hash",
	                          pgsc_max, pgsc_max,
	                          &info,
	                          HASH_ELEM | HASH_BLOBS);
#endif

	LWLockRelease(AddinShmemInitLock);

	/*
	 * If we're in the postmaster (or a standalone backend...), set up a shmem
	 * exit hook to dump the statistics to disk.
	 */
	if (!IsUnderPostmaster)
		on_shmem_exit(pgsc_shmem_shutdown, (Datum)0);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;

	/*
	 * not try to unlink any old dump file in this case.  This seems a bit
	 * questionable but it's the historical behavior.)
	 */
	if (!pgsc_save)
		return;

	/*
	 * Attempt to load old statistics from the dump file.
	 */
	file = AllocateFile(PGSC_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		return;
	}

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
	    fread(&pgver, sizeof(uint32), 1, file) != 1 ||
	    fread(&pgsc_aggs->counters, sizeof(Counters), 1, file) != 1 ||
	    fread(&num, sizeof(int32), 1, file) != 1)
		goto read_error;

	if (header != PGSC_FILE_HEADER ||
	    pgver != PGSC_PG_MAJOR_VERSION)
		goto data_error;

	for (i = 0; i < num; i++)
	{
		pgscEntry temp;
		pgscEntry *entry;

		if (fread(&temp, sizeof(pgscEntry), 1, file) != 1)
			goto read_error;

		/* Skip loading "sticky" entries */
		if (temp.counters.calls == 0)
			continue;

		/* make the hashtable entry (discards old entries if too many) */
		entry = entry_alloc(&temp.key);

		/* copy in the actual stats */
		if (entry)
			entry->counters = temp.counters;
	}

	/* Read global statistics for pg_stat_counters */
	if (fread(&pgsc->stats, sizeof(pgscGlobalStats), 1, file) != 1)
		goto read_error;

	FreeFile(file);

	/*
	 * Remove the persisted stats file so it's not included in
	 * backups/replication slaves, etc.  A new file will be written on next
	 * shutdown.
	 */
	unlink(PGSC_DUMP_FILE);

	return;

read_error:
	ereport(LOG,
	        (errcode_for_file_access(),
	         errmsg("could not read pg_stat_counters file \"%s\": %m",
	                 PGSC_DUMP_FILE)));
	goto fail;
data_error:
	ereport(LOG,
	        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	         errmsg("ignoring invalid data in pg_stat_counters file \"%s\"",
	                 PGSC_DUMP_FILE)));
fail:
	if (file)
		FreeFile(file);

	/* If possible, throw away the bogus file; ignore any error */
	unlink(PGSC_DUMP_FILE);
}

/*
 * shmem_shutdown hook: Dump statistics into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
pgsc_shmem_shutdown(int code, Datum arg)
{
	FILE             *file;
	HASH_SEQ_STATUS  hash_seq;
	int32            num_entries;
	pgscEntry        *entry;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		return;

	/* Don't dump if told not to. */
	if (!pgsc_save)
		return;

	file = AllocateFile(PGSC_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGSC_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;
	if (fwrite(&PGSC_PG_MAJOR_VERSION, sizeof(uint32), 1, file) != 1)
		goto error;
	if (fwrite(&pgsc_aggs->counters, sizeof(Counters), 1, file) != 1)
		goto error;
	num_entries = hash_get_num_entries(pgsc_hash);
	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;
	hash_seq_init(&hash_seq, pgsc_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(pgscEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	/* Dump global statistics for pg_stat_counters */
	if (fwrite(&pgsc->stats, sizeof(pgscGlobalStats), 1, file) != 1)
		goto error;

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

#if (PG_VERSION_NUM >= 90407)
	/* 
	 * Rename file into place, so we atomically replace any old one. 
	 */
	(void)durable_rename(PGSC_DUMP_FILE ".tmp", PGSC_DUMP_FILE, LOG);
#else
	/*
	 * Rename file inplace
	 */
	if (rename(PGSC_DUMP_FILE ".tmp", PGSC_DUMP_FILE) != 0)
		ereport(LOG,
		        (errcode_for_file_access(),
		         errmsg("could not rename pg_stat_counters file \"%s\": %m",
		                        PGSC_DUMP_FILE ".tmp")));
#endif

	return;

error:
	ereport(LOG,
	        (errcode_for_file_access(),
	         errmsg("could not write pg_stat_counters file \"%s\": %m",
	                 PGSC_DUMP_FILE ".tmp")));
	if (file)
		FreeFile(file);
	unlink(PGSC_DUMP_FILE ".tmp");
}

/*
 * ExecutorStart hook: start up tracking if needed
 */
static void
pgsc_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (getrusage(RUSAGE_SELF, &rusage_start) != 0)
		elog(DEBUG1, "pg_stat_counters: %s(): failed to execute getrusage", 
	                        __FUNCTION__);

        if (prev_ExecutorStart)
                prev_ExecutorStart(queryDesc, eflags);
        else
                standard_ExecutorStart(queryDesc, eflags);

        /*
         * If query has queryId zero, don't track it.  This prevents double
         * counting of optimizable statements that are directly contained in
         * utility statements.
         */
        if (pgsc_enabled(nesting_level) && queryDesc->plannedstmt->queryId != UINT64CONST(0))
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
#if (PG_VERSION_NUM >= 140000)
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
#else
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
#endif
			MemoryContextSwitchTo(oldcxt);
		}
	}
}


#if (PG_VERSION_NUM >= 140000)

/*
 * Post-parse-analysis hook: mark query with a queryId
 */
static void
pgsc_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
        if (prev_post_parse_analyze_hook)
                prev_post_parse_analyze_hook(pstate, query, jstate);

        /* Safety check... */
        if (!pgsc || !pgsc_hash || !pgsc_enabled(nesting_level))
                return;

        /*
         * If it's EXECUTE, clear the queryId so that stats will accumulate for
         * the underlying PREPARE.  But don't do this if we're not tracking
         * utility statements, to avoid messing up another extension that might be
         * tracking them.
         */
        if (query->utilityStmt)
        {
                if (pgsc_track_utility && IsA(query->utilityStmt, ExecuteStmt))
                {
                        query->queryId = UINT64CONST(0);
                        return;
                }
        }

        /*
         * If query jumbling were able to identify any ignorable constants, we
         * immediately create a hash table entry for the query, so that we can
         * record the normalized form of the query string.  If there were no such
         * constants, the normalized string would be the same as the query text
         * anyway, so there's no need for an early entry.
         */
        if (jstate && jstate->clocations_count > 0)
                pgsc_store(query->commandType
                           , PGSC_INVALID
                           , 0
                           , 0
                           , NULL       /* sys_info */
                           , NULL
                           , NULL
#if (PG_VERSION_NUM >= 150000)
                           , NULL
#endif
#if (PG_VERSION_NUM >= 180000)
                           , 0
                           , 0
#endif
			  );
}


/*
 * Planner hook: forward to regular planner, but measure planning time
 * if needed.
 */
static PlannedStmt *
pgsc_planner(Query *parse,
                         const char *query_string,
                         int cursorOptions,
                         ParamListInfo boundParams)
{
        PlannedStmt *result;

        /*
         * We can't process the query if no query_string is provided, as
         * pgsc_store needs it.  We also ignore query without queryid, as it would
         * be treated as a utility statement, which may not be the case.
         */
        if (pgsc_enabled(nesting_level)
                && pgsc_track_planning && query_string
                && parse->queryId != UINT64CONST(0))
        {
                instr_time      start;
                instr_time      duration;
                BufferUsage bufusage_start,
                                        bufusage;
                WalUsage        walusage_start,
                                        walusage;

                /* We need to track buffer usage as the planner can access them. */
                bufusage_start = pgBufferUsage;

                /*
                 * Similarly the planner could write some WAL records in some cases
                 * (e.g. setting a hint bit with those being WAL-logged)
                 */
                walusage_start = pgWalUsage;
                INSTR_TIME_SET_CURRENT(start);

                nesting_level++;
                PG_TRY();
                {
                        if (prev_planner_hook)
                                result = prev_planner_hook(parse, query_string, cursorOptions,
                                                                                   boundParams);
                        else
                                result = standard_planner(parse, query_string, cursorOptions,
                                                                                  boundParams);
                }
                PG_FINALLY();
                {
                        nesting_level--;
                }
                PG_END_TRY();

                INSTR_TIME_SET_CURRENT(duration);
                INSTR_TIME_SUBTRACT(duration, start);

                /* calc differences of buffer counters. */
                memset(&bufusage, 0, sizeof(BufferUsage));
                BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

                /* calc differences of WAL counters. */
                memset(&walusage, 0, sizeof(WalUsage));
                WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);

                pgsc_store(parse->commandType
                           , PGSC_PLAN
                           , INSTR_TIME_GET_MILLISEC(duration)
                           , 0
                           , NULL	/* sys_info */
                           , &bufusage
                           , &walusage
#if (PG_VERSION_NUM >= 150000)
                           , NULL
#endif
#if (PG_VERSION_NUM >= 180000)
                           , 0
                           , 0
#endif
                );
        }
        else
        {
                /*
                 * Even though we're not tracking plan time for this statement, we
                 * must still increment the nesting level, to ensure that functions
                 * evaluated during planning are not seen as top-level calls.
                 */
                nesting_level++;
                PG_TRY();
                {
                        if (prev_planner_hook)
                                result = prev_planner_hook(parse, query_string, cursorOptions,
                                                                                   boundParams);
                        else
                                result = standard_planner(parse, query_string, cursorOptions,
                                                                                  boundParams);
                }
                PG_FINALLY();
                {
                        nesting_level--;
                }
                PG_END_TRY();
        }

        return result;
}
#endif


/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
#if (PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000)
static void
pgsc_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
                 bool execute_once)
#elif (PG_VERSION_NUM >= 90600 || PG_VERSION_NUM >= 180000) 
static void
pgsc_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
#else
static void
pgsc_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
#endif
{
	nesting_level++;
	PG_TRY();
	{
#if (PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000)
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pgsc_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: store results if needed
 */
static void
pgsc_ExecutorEnd(QueryDesc *queryDesc)
{
	SysInfo sys_info;

	if (queryDesc->plannedstmt->queryId != UINT64CONST(0) 
                && queryDesc->totaltime && pgsc_enabled(nesting_level))
	{
		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		if (getrusage(RUSAGE_SELF, &rusage_end) != 0)
			elog(DEBUG1, "pg_stat_counters: %s(): failed to execute getrusage", __FUNCTION__);

		sys_info.utime = DIFF_TIME_MILLISEC(rusage_end.ru_utime, rusage_start.ru_utime);
		sys_info.stime = DIFF_TIME_MILLISEC(rusage_end.ru_stime, rusage_start.ru_stime);

		/*
		 * We only consider values greater than 3 * linux tick, otherwise the
		 * bias is too big
		 */
		if (queryDesc->totaltime->total < (3. / pgsc_linux_hz))
		{
			sys_info.stime = 0;
			sys_info.utime = queryDesc->totaltime->total * 1000;
		}

#ifdef HAVE_GETRUSAGE
		/* Compute the rest of the counters */
		sys_info.minflts = rusage_end.ru_minflt - rusage_start.ru_minflt;
		sys_info.majflts = rusage_end.ru_majflt - rusage_start.ru_majflt;
		sys_info.nswaps = rusage_end.ru_nswap - rusage_start.ru_nswap;
		sys_info.reads = rusage_end.ru_inblock - rusage_start.ru_inblock;
		sys_info.writes = rusage_end.ru_oublock - rusage_start.ru_oublock;
		sys_info.msgsnds = rusage_end.ru_msgsnd - rusage_start.ru_msgsnd;
		sys_info.msgrcvs = rusage_end.ru_msgrcv - rusage_start.ru_msgrcv;
		sys_info.nsignals = rusage_end.ru_nsignals - rusage_start.ru_nsignals;
		sys_info.nvcsws = rusage_end.ru_nvcsw - rusage_start.ru_nvcsw;
		sys_info.nivcsws = rusage_end.ru_nivcsw - rusage_start.ru_nivcsw;
#endif

		pgsc_store(queryDesc->operation
                           , PGSC_EXEC
		           , queryDesc->totaltime->total * 1000.0 /* convert to msec */
//                                   queryDesc->estate->es_total_processed,
		           , queryDesc->estate->es_processed
		           , &sys_info
		           , &queryDesc->totaltime->bufusage
#if (PG_VERSION_NUM >= 130000)
		           , &queryDesc->totaltime->walusage
#endif
#if (PG_VERSION_NUM >= 150000)
                           , queryDesc->estate->es_jit ? &queryDesc->estate->es_jit->instr : NULL
#endif
#if (PG_VERSION_NUM >= 180000)
                           , queryDesc->estate->es_parallel_workers_to_launch
                           , queryDesc->estate->es_parallel_workers_launched
#endif
		);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * ProcessUtility hook
 */

#if (PG_VERSION_NUM >= 140000)
static void
pgsc_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                    bool readOnlyTree,
                    ProcessUtilityContext context,
                    ParamListInfo params, QueryEnvironment *queryEnv,
                    DestReceiver *dest, QueryCompletion *qc)
#elif (PG_VERSION_NUM >= 130000)
static void
pgsc_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                    ProcessUtilityContext context,
                    ParamListInfo params, QueryEnvironment *queryEnv,
                    DestReceiver *dest, QueryCompletion *qc)
#elif (PG_VERSION_NUM >= 100000)
static void
pgsc_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                    ProcessUtilityContext context,
                    ParamListInfo params, QueryEnvironment *queryEnv,
                    DestReceiver *dest, char *completionTag)
#else
static void
pgsc_ProcessUtility(Node *parsetree, const char *queryString,
                    ProcessUtilityContext context, ParamListInfo params,
                    DestReceiver *dest, char *completionTag)
#endif
{
#if (PG_VERSION_NUM >= 100000)
	Node *parsetree = pstmt->utilityStmt;
#endif
        bool  enabled = pgsc_track_utility && pgsc_enabled(nesting_level);

        /*
         * Force utility statements to get queryId zero.  We do this even in cases
         * where the statement contains an optimizable statement for which a
         * queryId could be derived (such as EXPLAIN or DECLARE CURSOR).  For such
         * cases, runtime control will first go through ProcessUtility and then
         * the executor, and we don't want the executor hooks to do anything,
         * since we are already measuring the statement's costs at the utility
         * level.
         *
         * Note that this is only done if pg_stat_statements is enabled and
         * configured to track utility statements, in the unlikely possibility
         * that user configured another extension to handle utility statements
         * only.
         */
#if (PG_VERSION_NUM >= 130000)
        if (enabled)
                pstmt->queryId = UINT64CONST(0);
#endif
	/*
	 * If it's an EXECUTE statement, we don't track it and don't increment the
	 * nesting level.  This allows the cycles to be charged to the underlying
	 * PREPARE instead (by the Executor hooks), which is much more useful.
	 *
	 * We also don't track execution of PREPARE.  If we did, we would get one
	 * hash table entry for the PREPARE (with hash calculated from the query
	 * string), and then a different one with the same query string (but hash
	 * calculated from the query tree) would be used to accumulate costs of
	 * ensuing EXECUTEs.  This would be confusing, and inconsistent with other
	 * cases where planning time is not included at all.
	 *
	 * Likewise, we don't track execution of DEALLOCATE.
	 */
	if (enabled &&
	    !IsA(parsetree, ExecuteStmt) &&
	    !IsA(parsetree, PrepareStmt) &&
	    !IsA(parsetree, DeallocateStmt))
	{
		instr_time start;
		instr_time duration;
		uint64 rows;
		BufferUsage bufusage_start,
		            bufusage;
#if (PG_VERSION_NUM >= 130000)
		WalUsage walusage_start,
		         walusage;
#endif

		bufusage_start = pgBufferUsage;
#if (PG_VERSION_NUM >= 130000)
		walusage_start = pgWalUsage;
#endif

		INSTR_TIME_SET_CURRENT(start);

		nesting_level++;
		PG_TRY();
		{
#if (PG_VERSION_NUM >= 140000)
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString, readOnlyTree,
				                    context, params, queryEnv,
				                    dest, qc);
			else
				standard_ProcessUtility(pstmt, queryString, readOnlyTree,
				                        context, params, queryEnv,
				                        dest, qc);
#elif (PG_VERSION_NUM >= 130000)
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString,
				                    context, params, queryEnv,
				                    dest, qc);
			else
				standard_ProcessUtility(pstmt, queryString,
				                        context, params, queryEnv,
				                        dest, qc);
#elif (PG_VERSION_NUM >= 100000)
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString,
				                    context, params, queryEnv,
				                    dest, completionTag);
			else
				standard_ProcessUtility(pstmt, queryString,
				                        context, params, queryEnv,
				                        dest, completionTag);
#else
			if (prev_ProcessUtility)
				prev_ProcessUtility(parsetree, queryString,
				                    context, params,
				                    dest, completionTag);
			else
				standard_ProcessUtility(parsetree, queryString,
				                        context, params,
				                        dest, completionTag);
#endif
			nesting_level--;
		}
		PG_CATCH();
		{
			nesting_level--;
			PG_RE_THROW();
		}
		PG_END_TRY();

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);

#if (PG_VERSION_NUM >= 130000)
		rows = (qc && qc->commandTag == CMDTAG_COPY) ? qc->nprocessed : 0;
		/* calc differences of WAL counters. */
		memset(&walusage, 0, sizeof(WalUsage));
		WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);
#else
		/* parse command tag to retrieve the number of affected rows. */
		if (completionTag &&
			strncmp(completionTag, "COPY ", 5) == 0)
#if (PG_VERSION_NUM >= 90600)
			rows = pg_strtouint64(completionTag + 5, NULL, 10);
#else
#ifdef HAVE_STRTOULL
			rows = strtoull(completionTag + 5, NULL, 10);
#else
			rows = strtoul(completionTag + 5, NULL, 10);
#endif /* HAVE_STRTOULL */
#endif /* PG_VERSION_NUM >= 90600 */
		else
			rows = 0;
#endif /* PG_VERSION_NUM >= 130000 */

		/* calc differences of buffer counters. */
		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

		pgsc_store(CMD_UTILITY                         /* signal that it's a utility stmt */
                           , PGSC_EXEC
		           , INSTR_TIME_GET_MILLISEC(duration) /* total_time */
		           , rows
		           , NULL                              /* sysinfo */
		           , &bufusage
#if (PG_VERSION_NUM >= 130000)
		           , &walusage
#endif
#if (PG_VERSION_NUM >= 150000)
                           , NULL
#endif
#if (PG_VERSION_NUM >= 180000)
                           , 0
                           , 0
#endif
		          );
	}
	else
	{
#if (PG_VERSION_NUM >= 140000)
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
			                    context, params, queryEnv,
			                    dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
			                        context, params, queryEnv,
			                        dest, qc);
#elif (PG_VERSION_NUM >= 130000)
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString,
			                    context, params, queryEnv,
			                    dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
			                        context, params, queryEnv,
			                        dest, qc);
#elif (PG_VERSION_NUM >= 100000)
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString,
			                    context, params, queryEnv,
			                    dest, completionTag);
		else
			standard_ProcessUtility(pstmt, queryString,
			                        context, params, queryEnv,
			                        dest, completionTag);
#else
		if (prev_ProcessUtility)
			prev_ProcessUtility(parsetree, queryString,
			                    context, params,
			                    dest, completionTag);
		else
			standard_ProcessUtility(parsetree, queryString,
			                        context, params,
			                        dest, completionTag);
#endif
	}
}

static bool
pgsc_assign_linux_hz_check_hook(int *newval, void **extra, GucSource source)
{
	int    val = *newval;
	struct rusage myrusage;
	struct timeval previous_value;

	/* In that case we try to guess it */
	if (val == -1)
	{
		elog(LOG, "Auto detecting pg_stat_counters.linux_hz parameter...");
		getrusage(RUSAGE_SELF, &myrusage);
		previous_value = myrusage.ru_utime;
		while (myrusage.ru_utime.tv_usec == previous_value.tv_usec &&
		       myrusage.ru_utime.tv_sec == previous_value.tv_sec)
		{
			getrusage(RUSAGE_SELF, &myrusage);
		}
		*newval = (int) (1 / ((myrusage.ru_utime.tv_sec - previous_value.tv_sec) +
		    (myrusage.ru_utime.tv_usec - previous_value.tv_usec) / 1000000.));
		elog(LOG, "pg_stat_counters.linux_hz is set to %d", *newval);
	}
	return true;
}

#if PG_VERSION_NUM < 130000
static void
BufferUsageAccumDiff(BufferUsage* bufusage, BufferUsage* pgBufferUsage, BufferUsage* bufusage_start)
{
	/* calc differences of buffer counters. */
	bufusage->shared_blks_hit = pgBufferUsage->shared_blks_hit - bufusage_start->shared_blks_hit;
	bufusage->shared_blks_read = pgBufferUsage->shared_blks_read - bufusage_start->shared_blks_read;
	bufusage->shared_blks_dirtied = pgBufferUsage->shared_blks_dirtied - bufusage_start->shared_blks_dirtied;
	bufusage->shared_blks_written = pgBufferUsage->shared_blks_written - bufusage_start->shared_blks_written;
	bufusage->local_blks_hit = pgBufferUsage->local_blks_hit - bufusage_start->local_blks_hit;
	bufusage->local_blks_read = pgBufferUsage->local_blks_read - bufusage_start->local_blks_read;
	bufusage->local_blks_dirtied = pgBufferUsage->local_blks_dirtied - bufusage_start->local_blks_dirtied;
	bufusage->local_blks_written = pgBufferUsage->local_blks_written - bufusage_start->local_blks_written;
	bufusage->temp_blks_read = pgBufferUsage->temp_blks_read - bufusage_start->temp_blks_read;
	bufusage->temp_blks_written = pgBufferUsage->temp_blks_written - bufusage_start->temp_blks_written;
	bufusage->blk_read_time = pgBufferUsage->blk_read_time;
	INSTR_TIME_SUBTRACT(bufusage->blk_read_time, bufusage_start->blk_read_time);
	bufusage->blk_write_time = pgBufferUsage->blk_write_time;
	INSTR_TIME_SUBTRACT(bufusage->blk_write_time, bufusage_start->blk_write_time);
}
#endif

#if (PG_VERSION_NUM < 90600)
/*
 * Calculate hash value for a key
 */
static uint32
pgsc_hash_fn(const void *key, Size keysize)
{
	const pgscHashKey *k = (const pgscHashKey *)key;

	return hash_uint32((uint32)k->userid) ^
	       hash_uint32((uint32)k->dbid) ^
	       hash_uint32((uint32)k->operation);
}

/*
 * Compare two keys - zero means match
 */
static int
pgsc_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgscHashKey *k1 = (const pgscHashKey *)key1;
	const pgscHashKey *k2 = (const pgscHashKey *)key2;

	if (k1->userid == k2->userid &&
	    k1->dbid == k2->dbid &&
	    k1->operation == k2->operation)
		return 0;
	else
		return 1;
}
#endif

/*
 * Estimate shared memory space needed.
 */
static Size
pgsc_memsize(void)
{
	Size size;

	size = MAXALIGN(sizeof(pgscSharedState));
	size = add_size(size, sizeof(aggEntry));
	size = add_size(size, hash_estimate_size(pgsc_max, sizeof(pgscEntry)));

	elog(DEBUG1, "pg_stat_counters: %s(): SharedState: [%lu] max#Entries: [%d] oneEntry: [%lu] Entries: [%lu] aggEntry: [%lu] total: [%lu] ", 
	                __FUNCTION__, sizeof(pgscSharedState), 
	                pgsc_max, sizeof(pgscEntry), hash_estimate_size(pgsc_max, sizeof(pgscEntry)), 
	                sizeof(aggEntry), size);

	return size;
}

/*
 * Allocate a new hashtable entry.
 * Caller must hold an exclusive lock on pgsc->lock
 * 
 */
static pgscEntry *
entry_alloc(pgscHashKey *key)
{
	pgscEntry  *entry;
	bool       found;

	/* Make space if needed */
	while (hash_get_num_entries(pgsc_hash) >= pgsc_max)
		entry_dealloc();

	/* Find or create an entry with desired hash code */
	entry = (pgscEntry *) hash_search(pgsc_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(Counters));
		/* marking the first change */
		entry->counters._first_change = time(NULL);
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	return entry;
}

/*
 * qsort comparator for sorting into increasing timestamp order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	TimestampTz  l_ts = (*(pgscEntry *const *) lhs)->counters._last_change;
	TimestampTz  r_ts = (*(pgscEntry *const *) rhs)->counters._last_change;

	/* first field - timestamp */
	if (l_ts < r_ts)
		return -1;
	else if (l_ts > r_ts)
		return +1;
	else
		return 0;
}

/*
 * Deallocate most oldest entries.
 *
 * Caller must hold an exclusive lock on pgsc->lock.
 */
static void
entry_dealloc(void)
{
	HASH_SEQ_STATUS  hash_seq;
	pgscEntry        **entries;
	pgscEntry        *entry;
	int              nvictims;
	int              i;

	/*
	 * Sort entries by last stat and deallocate PGSC_DEALLOC_PERCENT of them.
	 */
	entries = palloc(hash_get_num_entries(pgsc_hash) * sizeof(pgscEntry *));

	i = 0;

	hash_seq_init(&hash_seq, pgsc_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
	}

	/* Sort into increasing order by last time */
	qsort(entries, i, sizeof(pgscEntry *), entry_cmp);

	/* Now zap an appropriate fraction of lowest-time entries */
	nvictims = Max(2, i * PGSC_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgsc_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);

	/* Increment the number of times entries are deallocated */
	{
		volatile pgscSharedState *s = (volatile pgscSharedState *) pgsc;

		SpinLockAcquire(&s->mutex);
		s->stats.dealloc++;
		SpinLockRelease(&s->mutex);
	}
}


/*
 * Release all entries.
 */
static void
entry_reset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgscEntry *entry;

	LWLockAcquire(pgsc->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgsc_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgsc_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgsc->lock);
}

/*
 * Release whole database cluster statistics
 */
static void
aggstats_reset(void)
{
	/* Safety check ... */
	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		return;

	memset(&pgsc_aggs->counters, 0, sizeof(Counters));
}

/*
 * Update counters
 */
static void
pgsc_update_counters(volatile Counters *c
//#if (PG_VERSION_NUM >= 130000)
                     , pgscStoreKind kind
//#endif
                     , double total_time, uint64 rows, SysInfo *sys_info, const BufferUsage *bufusage
#if (PG_VERSION_NUM >= 130000)
                     , const WalUsage *walusage
#endif
#if (PG_VERSION_NUM >= 150000)
                     , const struct JitInstrumentation *jitusage
#endif
#if (PG_VERSION_NUM >= 180000)
                     , int parallel_workers_to_launch
                     , int parallel_workers_launched
#endif
)
{
	Assert(kind == PGSC_PLAN || kind == PGSC_EXEC);

	//c->calls++;
        c->calls[kind] += 1;
	/*
	 * The histogram is used only for the EXEC stage
	 *
	 */
        if (kind == PGSC_EXEC)
	{
		if (total_time >= 20000)
			c->calls_gt20s++;
		else if (total_time >= 10000)
			c->calls_10_20s++;
		else if (total_time >= 5000)
			c->calls_5_10s++;
		else if (total_time >= 3000)
			c->calls_3_5s++;
		else if (total_time >= 2000)
			c->calls_2_3s++;
		else if (total_time >= 1000)
			c->calls_1_2s++;
	}

	c->total_time[kind] += total_time;
	c->rows += rows;
	if (bufusage)
	{
		c->shared_blks_hit += bufusage->shared_blks_hit;
		c->shared_blks_read += bufusage->shared_blks_read;
		c->shared_blks_dirtied += bufusage->shared_blks_dirtied;
		c->shared_blks_written += bufusage->shared_blks_written;
		c->local_blks_hit += bufusage->local_blks_hit;
		c->local_blks_read += bufusage->local_blks_read;
		c->local_blks_dirtied += bufusage->local_blks_dirtied;
		c->local_blks_written += bufusage->local_blks_written;
		c->temp_blks_read += bufusage->temp_blks_read;
		c->temp_blks_written += bufusage->temp_blks_written;
#if (PG_VERSION_NUM >= 170000)
                c->shared_blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->shared_blk_read_time);
                c->shared_blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->shared_blk_write_time);
                c->local_blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->local_blk_read_time);
                c->local_blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->local_blk_write_time);
#else
		c->blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->blk_read_time);
		c->blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->blk_write_time);
#endif
#if (PG_VERSION_NUM >= 150000)
                c->temp_blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->temp_blk_read_time);
                c->temp_blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->temp_blk_write_time);
#endif  
	}
#if (PG_VERSION_NUM >= 130000)
	if (walusage)
	{
		c->wal_records += walusage->wal_records;
		c->wal_fpi += walusage->wal_fpi;
		c->wal_bytes += walusage->wal_bytes;
#if (PG_VERSION_NUM >= 180000)
                c->wal_buffers_full += walusage->wal_buffers_full;
#endif
	}
#endif

#if (PG_VERSION_NUM >= 150000)
	if (jitusage)
	{
		c->jit_functions += jitusage->created_functions;
		c->jit_generation_time += INSTR_TIME_GET_MILLISEC(jitusage->generation_counter);
#if (PG_VERSION_NUM >= 170000)
		if (INSTR_TIME_GET_MILLISEC(jitusage->deform_counter))
			c->jit_deform_count++;
		c->jit_deform_time += INSTR_TIME_GET_MILLISEC(jitusage->deform_counter);
#endif
		if (INSTR_TIME_GET_MILLISEC(jitusage->inlining_counter))
			c->jit_inlining_count++;
		c->jit_inlining_time += INSTR_TIME_GET_MILLISEC(jitusage->inlining_counter);

		if (INSTR_TIME_GET_MILLISEC(jitusage->optimization_counter))
			c->jit_optimization_count++;
		c->jit_optimization_time += INSTR_TIME_GET_MILLISEC(jitusage->optimization_counter);

		if (INSTR_TIME_GET_MILLISEC(jitusage->emission_counter))
			c->jit_emission_count++;
		c->jit_emission_time += INSTR_TIME_GET_MILLISEC(jitusage->emission_counter);
	}
#endif

#if (PG_VERSION_NUM >= 180000)
	/* parallel worker counters */
	c->parallel_workers_to_launch += parallel_workers_to_launch;
	c->parallel_workers_launched += parallel_workers_launched;
#endif
	if (sys_info)
	{
		c->utime += sys_info->utime;
		c->stime += sys_info->stime;
#ifdef HAVE_GETRUSAGE
		c->minflts += sys_info->minflts;
		c->majflts += sys_info->majflts;
		c->nswaps += sys_info->nswaps;
		c->reads += sys_info->reads;
		c->writes += sys_info->writes;
		c->msgsnds += sys_info->msgsnds;
		c->msgrcvs += sys_info->msgrcvs;
		c->nsignals += sys_info->nsignals;
		c->nvcsws += sys_info->nvcsws;
		c->nivcsws += sys_info->nivcsws;
#endif
	}
	c->_last_change = GetCurrentTimestamp();
}

/*
 * Store some statistics for key and whole database cluster
 */
static void
pgsc_store(CmdType operation
           , pgscStoreKind kind
           , double total_time, uint64 rows, SysInfo *sys_info, const BufferUsage *bufusage
#if (PG_VERSION_NUM >= 130000)
           , const WalUsage *walusage
#endif
#if (PG_VERSION_NUM >= 150000)
           , const struct JitInstrumentation *jitusage
#endif
#if (PG_VERSION_NUM >= 180000)
           , int parallel_workers_to_launch
           , int parallel_workers_launched
#endif

)
{
	pgscHashKey key;
	pgscEntry *entry;

	/* Safety check ... */
	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.operation = operation;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgsc->lock, LW_SHARED);

	entry = (pgscEntry *)hash_search(pgsc_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(pgsc->lock);
		LWLockAcquire(pgsc->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = entry_alloc(&key);
	}

	{
		volatile aggEntry *a = (volatile aggEntry *)pgsc_aggs;

		SpinLockAcquire(&a->mutex);

		if (entry)
		{
			/*
			 * Grab the spinlock while updating the counters (see comment about
			 * locking rules at the head of the file)
			 */
			volatile pgscEntry *e = (volatile pgscEntry *)entry;

			SpinLockAcquire(&e->mutex);
			pgsc_update_counters(&e->counters
                                             , kind
                                             , total_time, rows, sys_info, bufusage
#if (PG_VERSION_NUM >= 130000)
			                     , walusage
#endif
#if (PG_VERSION_NUM >= 150000)
			                     , jitusage
#endif
#if (PG_VERSION_NUM >= 180000)
			                     , parallel_workers_to_launch
			                     , parallel_workers_launched
#endif
			                    );
			SpinLockRelease(&e->mutex);
		}

		/* update aggregate statistics */
		pgsc_update_counters(&a->counters
                                     , kind
                                     , total_time
				     , rows
				     , sys_info
				     , bufusage
#if (PG_VERSION_NUM >= 130000)
		                     , walusage
#endif
#if (PG_VERSION_NUM >= 150000)
                                     , jitusage
#endif
#if (PG_VERSION_NUM >= 180000)
                                     , parallel_workers_to_launch
                                     , parallel_workers_launched
#endif

		                    );
		SpinLockRelease(&a->mutex);
	}

	LWLockRelease(pgsc->lock);
}


/*
 * Reset all statistics of calls.
 */
Datum
pg_stat_counters_reset(PG_FUNCTION_ARGS)
{
	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		         errmsg("pg_stat_counters must be loaded via shared_preload_libraries")));
	entry_reset();
	aggstats_reset();
	pgsc_reset();
	PG_RETURN_VOID();
}


/* Number of output arguments (columns) for pg_stat_counters_info */
#define PG_STAT_COUNTERS_INFO_COLS    2

/*
 * Return statistics of pg_stat_counters.
 */
Datum
pg_stat_counters_info(PG_FUNCTION_ARGS)
{
	pgscGlobalStats stats;
	TupleDesc       tupdesc;
	Datum           values[PG_STAT_COUNTERS_INFO_COLS];
	bool            nulls[PG_STAT_COUNTERS_INFO_COLS];

	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		         errmsg("pg_stat_counters must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	/* Read global statistics for pg_stat_statements */
	{
		volatile pgscSharedState *s = (volatile pgscSharedState *) pgsc;

		SpinLockAcquire(&s->mutex);
		stats = s->stats;
		SpinLockRelease(&s->mutex);
	}

	values[0] = Int64GetDatum(stats.dealloc);
	values[1] = TimestampTzGetDatum(stats.stats_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


#define PG_STAT_COUNTERS_AGG_COLS   55
#define PG_STAT_COUNTERS_COLS       58

/*
 * Retrieve call statistics per key.
 */
Datum
pg_stat_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo    *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc        tupdesc;
	Tuplestorestate  *tupstore;
	MemoryContext    per_query_ctx;
	MemoryContext    oldcontext;
	HASH_SEQ_STATUS  hash_seq;
	pgscEntry        *entry;

	/* hash table must exist already */
	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		         errmsg("pg_stat_counters must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("materialize mode required, but it is not "
		                        "allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * With a large hash table, we might be holding the lock rather longer
	 * than one could wish.  However, this only blocks creation of new hash
	 * table entries, and the larger the hash table the less likely that is to
	 * be needed.  So we can hope this is okay.  Perhaps someday we'll decide
	 * we need to partition the hash table to limit the time spent holding any
	 * one lock.
	 */
	LWLockAcquire(pgsc->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgsc_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum     values[PG_STAT_COUNTERS_COLS];
		bool      nulls[PG_STAT_COUNTERS_COLS];
		int       i = 0;
		Counters  tmp;
#ifdef HAVE_GETRUSAGE		
		int64     reads, writes;	/* RUSAGE */
#endif

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);
		values[i++] = ObjectIdGetDatum(entry->key.operation);

		/* copy counters to a local variable to keep locking time short */
		{
			volatile pgscEntry *e = (volatile pgscEntry *)entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

                values[i++] = Int64GetDatumFast(tmp.calls_1_2s);
                values[i++] = Int64GetDatumFast(tmp.calls_2_3s);
                values[i++] = Int64GetDatumFast(tmp.calls_3_5s);
                values[i++] = Int64GetDatumFast(tmp.calls_5_10s);
                values[i++] = Int64GetDatumFast(tmp.calls_10_20s);
                values[i++] = Int64GetDatumFast(tmp.calls_gt20s);

		for (int kind = 0; kind < PGSC_NUMKIND; kind++)
		{
	                if (kind == PGSC_EXEC || PG_VERSION_NUM >= 130000)
			{
				values[i++] = Int64GetDatumFast(tmp.calls[kind]);
				values[i++] = Float8GetDatumFast(tmp.total_time[kind]);
			}
			else
			{
				nulls[i++] = true;
				nulls[i++] = true;
			}
		}

		values[i++] = Int64GetDatumFast(tmp.rows);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_read);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_dirtied);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_written);
		values[i++] = Int64GetDatumFast(tmp.local_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.local_blks_read);
		values[i++] = Int64GetDatumFast(tmp.local_blks_dirtied);
		values[i++] = Int64GetDatumFast(tmp.local_blks_written);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_read);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_written);
#if (PG_VERSION_NUM >= 170000)
		values[i++] = Float8GetDatumFast(tmp.shared_blk_read_time);
		values[i++] = Float8GetDatumFast(tmp.shared_blk_write_time);
		values[i++] = Float8GetDatumFast(tmp.local_blk_read_time);
		values[i++] = Float8GetDatumFast(tmp.local_blk_write_time);
#else
		values[i++] = Float8GetDatumFast(tmp.blk_read_time);    /* aka shared_blk_read_time */
		values[i++] = Float8GetDatumFast(tmp.blk_write_time);   /* aka shared_blk_write_time */
		nulls[i++] = true; /* local_blk_read_time */
		nulls[i++] = true; /* local_blk_write_time */
#endif
#if (PG_VERSION_NUM >= 150000)
		values[i++] = Float8GetDatumFast(tmp.temp_blk_read_time);
		values[i++] = Float8GetDatumFast(tmp.temp_blk_write_time);
#else
		nulls[i++] = true; /* temp_blk_read_time  */
		nulls[i++] = true; /* temp_blk_write_time */
#endif
#if (PG_VERSION_NUM >= 130000)
		{
			char            buf[256];
			Datum           wal_bytes;

			values[i++] = Int64GetDatumFast(tmp.wal_records);
			values[i++] = Int64GetDatumFast(tmp.wal_fpi);

			snprintf(buf, sizeof buf, UINT64_FORMAT, tmp.wal_bytes);

			/* Convert to numeric. */
			wal_bytes = DirectFunctionCall3(numeric_in,
			                                CStringGetDatum(buf),
			                                ObjectIdGetDatum(0),
			                                Int32GetDatum(-1));
			values[i++] = wal_bytes;
		}
#else
		nulls[i++] = true; /* # of WAL records generated */
		nulls[i++] = true; /* # of WAL full page images generated */
		nulls[i++] = true; /* total amount of WAL bytes generated */
#endif
#if (PG_VERSION_NUM >= 180000)
		values[i++] = Int64GetDatumFast(tmp.wal_buffers_full);
#else
		nulls[i++] = true; /* Number of times the WAL buffers became full */
#endif
#if (PG_VERSION_NUM >= 150000)
                values[i++] = Int64GetDatumFast(tmp.jit_functions);
                values[i++] = Float8GetDatumFast(tmp.jit_generation_time);
                values[i++] = Int64GetDatumFast(tmp.jit_inlining_count);
                values[i++] = Float8GetDatumFast(tmp.jit_inlining_time);
                values[i++] = Int64GetDatumFast(tmp.jit_optimization_count);
                values[i++] = Float8GetDatumFast(tmp.jit_optimization_time);
                values[i++] = Int64GetDatumFast(tmp.jit_emission_count);
                values[i++] = Float8GetDatumFast(tmp.jit_emission_time);
#else
		nulls[i++] = true; /* jit_functions */
		nulls[i++] = true; /* jit_generation_time */
		nulls[i++] = true; /* jit_inlining_count */
		nulls[i++] = true; /* jit_inlining_time */
		nulls[i++] = true; /* jit_optimization_count */
		nulls[i++] = true; /* jit_optimization_time */
		nulls[i++] = true; /* jit_emission_count */
		nulls[i++] = true; /* jit_emission_time */
#endif
#if (PG_VERSION_NUM >= 170000)
                values[i++] = Int64GetDatumFast(tmp.jit_deform_count);
                values[i++] = Float8GetDatumFast(tmp.jit_deform_time);
#else
		nulls[i++] = true; /* jit_deform_count */
		nulls[i++] = true; /* jit_deform_time */
#endif
#if (PG_VERSION_NUM >= 180000)
		values[i++] = Int64GetDatumFast(tmp.parallel_workers_to_launch);
		values[i++] = Int64GetDatumFast(tmp.parallel_workers_launched);
#else
		nulls[i++] = true; /* parallel_workers_to_launch */
		nulls[i++] = true; /* parallel_workers_launched */
#endif
		values[i++] = Float8GetDatumFast(tmp.utime);
		values[i++] = Float8GetDatumFast(tmp.stime);
#ifdef HAVE_GETRUSAGE
		values[i++] = Int64GetDatumFast(tmp.minflts);
		values[i++] = Int64GetDatumFast(tmp.majflts);
		values[i++] = Int64GetDatumFast(tmp.nswaps);
		reads = tmp.reads * RUSAGE_BLOCK_SIZE;
		values[i++] = Int64GetDatumFast(reads);
		writes = tmp.writes * RUSAGE_BLOCK_SIZE;
		values[i++] = Int64GetDatumFast(writes);
		values[i++] = Int64GetDatumFast(tmp.msgsnds);
		values[i++] = Int64GetDatumFast(tmp.msgrcvs);
		values[i++] = Int64GetDatumFast(tmp.nsignals);
		values[i++] = Int64GetDatumFast(tmp.nvcsws);
		values[i++] = Int64GetDatumFast(tmp.nivcsws);
#else
		nulls[i++] = true; /* minflts */
		nulls[i++] = true; /* majflts */
		nulls[i++] = true; /* nswaps */
		nulls[i++] = true; /* reads */
		nulls[i++] = true; /* writes */
		nulls[i++] = true; /* IPC messages sent */
		nulls[i++] = true; /* IPC messages received */
		nulls[i++] = true; /* signals received */
		nulls[i++] = true; /* nvcsws */
		nulls[i++] = true; /* nivcsws */
#endif

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	LWLockRelease(pgsc->lock);

//	tuplestore_donestoring(tupstore);

	return (Datum)0;
}


/*
 * Return statistics of pg_stat_counters_all.
 */
Datum
pg_stat_counters_all(PG_FUNCTION_ARGS)
{
	TupleDesc       tupdesc;
	Datum           values[PG_STAT_COUNTERS_AGG_COLS];
	bool            nulls[PG_STAT_COUNTERS_AGG_COLS];
	Counters        tmp;
        int             i = 0;
#ifdef HAVE_GETRUSAGE
	int64           reads, writes;		/* RUSAGE */
#endif

	if (!pgsc || !pgsc_hash || !pgsc_aggs)
		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		         errmsg("pg_stat_counters must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	/* Read agg statistics for pg_stat_counters */
	{
		volatile aggEntry *a = (volatile aggEntry *)pgsc_aggs;

		SpinLockAcquire(&a->mutex);
		tmp = a->counters;
		SpinLockRelease(&a->mutex);
	}

        values[i++] = Int64GetDatumFast(tmp.calls_1_2s);
        values[i++] = Int64GetDatumFast(tmp.calls_2_3s);
        values[i++] = Int64GetDatumFast(tmp.calls_3_5s);
        values[i++] = Int64GetDatumFast(tmp.calls_5_10s);
        values[i++] = Int64GetDatumFast(tmp.calls_10_20s);
        values[i++] = Int64GetDatumFast(tmp.calls_gt20s);

	for (int kind = 0; kind < PGSC_NUMKIND; kind++)
	{
		if (kind == PGSC_EXEC || PG_VERSION_NUM >= 130000)
		{
			values[i++] = Int64GetDatumFast(tmp.calls[kind]);
			values[i++] = Float8GetDatumFast(tmp.total_time[kind]);
                }
                else
                {
                        nulls[i++] = true; 
                        nulls[i++] = true; 
		}
	}

	values[i++] = Int64GetDatumFast(tmp.rows);
	values[i++] = Int64GetDatumFast(tmp.shared_blks_hit);
	values[i++] = Int64GetDatumFast(tmp.shared_blks_read);
	values[i++] = Int64GetDatumFast(tmp.shared_blks_dirtied);
	values[i++] = Int64GetDatumFast(tmp.shared_blks_written);
	values[i++] = Int64GetDatumFast(tmp.local_blks_hit);
	values[i++] = Int64GetDatumFast(tmp.local_blks_read);
	values[i++] = Int64GetDatumFast(tmp.local_blks_dirtied);
	values[i++] = Int64GetDatumFast(tmp.local_blks_written);
	values[i++] = Int64GetDatumFast(tmp.temp_blks_read);
	values[i++] = Int64GetDatumFast(tmp.temp_blks_written);

#if (PG_VERSION_NUM >= 170000)
	values[i++] = Float8GetDatumFast(tmp.shared_blk_read_time);
	values[i++] = Float8GetDatumFast(tmp.shared_blk_write_time);
	values[i++] = Float8GetDatumFast(tmp.local_blk_read_time);
	values[i++] = Float8GetDatumFast(tmp.local_blk_write_time);
#else
	values[i++] = Float8GetDatumFast(tmp.blk_read_time);	/* aka shared_blk_read_time */
	values[i++] = Float8GetDatumFast(tmp.blk_write_time);	/* aka shared_blk_write_time */
        nulls[i++] = true; /* local_blk_read_time */
        nulls[i++] = true; /* local_blk_write_time */
#endif

#if (PG_VERSION_NUM >= 150000)
	values[i++] = Float8GetDatumFast(tmp.temp_blk_read_time);
	values[i++] = Float8GetDatumFast(tmp.temp_blk_write_time);
#else
	nulls[i++] = true; /* temp_blk_read_time  */
	nulls[i++] = true; /* temp_blk_write_time */
#endif

#if (PG_VERSION_NUM >= 130000)
	{
		char            buf[256];
		Datum           wal_bytes;

		values[i++] = Int64GetDatumFast(tmp.wal_records);
		values[i++] = Int64GetDatumFast(tmp.wal_fpi);

		snprintf(buf, sizeof buf, UINT64_FORMAT, tmp.wal_bytes);

		/* Convert to numeric. */
		wal_bytes = DirectFunctionCall3(numeric_in,
		                                CStringGetDatum(buf),
		                                ObjectIdGetDatum(0),
		                                Int32GetDatum(-1));
		values[i++] = wal_bytes;
	}
#else
	nulls[i++] = true; /* # of WAL records generated */
	nulls[i++] = true; /* # of WAL full page images generated */
	nulls[i++] = true; /* total amount of WAL bytes generated */
#endif
#if (PG_VERSION_NUM >= 180000)
	values[i++] = Int64GetDatumFast(tmp.wal_buffers_full);
#else
	nulls[i++] = true; /* Number of times the WAL buffers became full */
#endif

#if (PG_VERSION_NUM >= 150000)
	values[i++] = Int64GetDatumFast(tmp.jit_functions);
	values[i++] = Float8GetDatumFast(tmp.jit_generation_time);
	values[i++] = Int64GetDatumFast(tmp.jit_inlining_count);
	values[i++] = Float8GetDatumFast(tmp.jit_inlining_time);
	values[i++] = Int64GetDatumFast(tmp.jit_optimization_count);
	values[i++] = Float8GetDatumFast(tmp.jit_optimization_time);
	values[i++] = Int64GetDatumFast(tmp.jit_emission_count);
	values[i++] = Float8GetDatumFast(tmp.jit_emission_time);
#else
	nulls[i++] = true; /* jit_functions */
	nulls[i++] = true; /* jit_generation_time */
	nulls[i++] = true; /* jit_inlining_count */
	nulls[i++] = true; /* jit_inlining_time */
	nulls[i++] = true; /* jit_optimization_count */
	nulls[i++] = true; /* jit_optimization_time */
	nulls[i++] = true; /* jit_emission_count */
	nulls[i++] = true; /* jit_emission_time */
#endif

#if (PG_VERSION_NUM >= 170000)
	values[i++] = Int64GetDatumFast(tmp.jit_deform_count);
	values[i++] = Float8GetDatumFast(tmp.jit_deform_time);
#else
	nulls[i++] = true; /* jit_deform_count */
	nulls[i++] = true; /* jit_deform_time */
#endif

#if (PG_VERSION_NUM >= 180000)
	values[i++] = Int64GetDatumFast(tmp.parallel_workers_to_launch);
	values[i++] = Int64GetDatumFast(tmp.parallel_workers_launched);
#else
        nulls[i++] = true; /* parallel_workers_to_launch */
        nulls[i++] = true; /* parallel_workers_launched */
#endif
	values[i++] = Float8GetDatumFast(tmp.utime);
	values[i++] = Float8GetDatumFast(tmp.stime);

#ifdef HAVE_GETRUSAGE
	values[i++] = Int64GetDatumFast(tmp.minflts);
	values[i++] = Int64GetDatumFast(tmp.majflts);
	values[i++] = Int64GetDatumFast(tmp.nswaps);
	reads = tmp.reads * RUSAGE_BLOCK_SIZE;
	values[i++] = Int64GetDatumFast(reads);
	writes = tmp.writes * RUSAGE_BLOCK_SIZE;
	values[i++] = Int64GetDatumFast(writes);
	values[i++] = Int64GetDatumFast(tmp.msgsnds);
	values[i++] = Int64GetDatumFast(tmp.msgrcvs);
	values[i++] = Int64GetDatumFast(tmp.nsignals);
	values[i++] = Int64GetDatumFast(tmp.nvcsws);
	values[i++] = Int64GetDatumFast(tmp.nivcsws);
#else
	nulls[i++] = true; /* minflts */
	nulls[i++] = true; /* majflts */
	nulls[i++] = true; /* nswaps */
	nulls[i++] = true; /* reads */
	nulls[i++] = true; /* writes */
	nulls[i++] = true; /* IPC messages sent */
	nulls[i++] = true; /* IPC messages received */
	nulls[i++] = true; /* signals received */
	nulls[i++] = true; /* nvcsws */
	nulls[i++] = true; /* nivcsws */
#endif

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


/*
 * Return the name of the operation. It is used to correct output 
 * a text representation of the operation in various versions of
 * postgresql.
 */
Datum
get_cmd_name(PG_FUNCTION_ARGS)
{
	Oid cmdid = PG_GETARG_OID(0);

#if (PG_VERSION_NUM <= 140000)
        const char* cmd_name[] = {"" /* UNKNOWN */ , "SELECT", "UPDATE", "INSERT", "DELETE", "UTILITY", "NOTHING"};
#else
        const char* cmd_name[] = {"" /* UNKNOWN */, "SELECT", "UPDATE", "INSERT", "DELETE", "MERGE", "UTILITY", "NOTHING"};
#endif
	int max_cmdid = sizeof(cmd_name)/sizeof(cmd_name[0]);

        if (cmdid < 0 || cmdid >= max_cmdid)
		elog(ERROR, "invalid operationid");


	PG_RETURN_TEXT_P(cstring_to_text(cmd_name[cmdid]));
}

