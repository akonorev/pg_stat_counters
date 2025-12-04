/* pg_stat_counters/pg_stat_counters--1.2.sql */

\echo Use "CREATE EXTENSION pg_stat_counters" to load this file. \quit

-- Register functions.
CREATE FUNCTION pg_stat_counters_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;


CREATE FUNCTION _fullversion()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;


CREATE FUNCTION get_cmd_name(Oid)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;


--- Define pg_stat_counters_info
CREATE FUNCTION pg_stat_counters_info(
    OUT dealloc               bigint,
    OUT stats_reset           timestamp with time zone
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE VIEW pg_stat_counters_info AS
  SELECT * FROM pg_stat_counters_info();

GRANT SELECT ON pg_stat_counters_info TO PUBLIC;


/* pg_stat_counters */
/* Now redefine */
CREATE FUNCTION pg_stat_counters(
    OUT userid                      oid,
    OUT dbid                        oid,
    OUT operationid                 oid,
    OUT calls_1_2s                  bigint,
    OUT calls_2_3s                  bigint,
    OUT calls_3_5s                  bigint,
    OUT calls_5_10s                 bigint,
    OUT calls_10_20s                bigint,
    OUT calls_gt20s                 bigint,
    ouT plans                       bigint,
    OUT total_plan_time             double precision,   /* in msec */
    ouT calls                       bigint,
    OUT total_exec_time             double precision,   /* in msec */
    OUT rows                        bigint,
    OUT shared_blks_hit             bigint,
    OUT shared_blks_read            bigint,
    OUT shared_blks_dirtied         bigint,
    OUT shared_blks_written         bigint,
    OUT local_blks_hit              bigint,
    OUT local_blks_read             bigint,
    OUT local_blks_dirtied          bigint,
    OUT local_blks_written          bigint,
    OUT temp_blks_read              bigint,
    OUT temp_blks_written           bigint,
    OUT shared_blk_read_time        double precision,
    OUT shared_blk_write_time       double precision,
    OUT local_blk_read_time         double precision,
    OUT local_blk_write_time        double precision,
    OUT temp_blk_read_time          double precision,
    OUT temp_blk_write_time         double precision,
    OUT wal_records                 bigint,
    OUT wal_fpi                     bigint,
    OUT wal_bytes                   numeric,
    OUT wal_buffers_full            bigint,
    OUT jit_functions               bigint,
    OUT jit_generation_time         double precision,
    OUT jit_inlining_count          bigint,
    OUT jit_inlining_time           double precision,
    OUT jit_optimization_count      bigint,
    OUT jit_optimization_time       double precision,
    OUT jit_emission_count          bigint,
    OUT jit_emission_time           double precision,
    OUT jit_deform_count            bigint,
    OUT jit_deform_time             double precision,
    OUT parallel_workers_to_launch  bigint,
    OUT parallel_workers_launched   bigint,
    OUT cpu_user_time               double precision,   /* total user CPU time used, in msec  */
    OUT cpu_sys_time                double precision,   /* total system CPU time used, in msec */
    OUT minflts                     bigint,             /* total page reclaims (soft page faults) */
    OUT majflts                     bigint,             /* total page faults (hard page faults) */
    OUT nswaps                      bigint,             /* total swaps */
    OUT reads                       bigint,             /* total reads, in bytes */
    OUT writes                      bigint,             /* total writes, in bytes */
    OUT msgsnds                     bigint,             /* IPC messages sent */
    OUT msgrcvs                     bigint,             /* IPC messages received */
    OUT nsignals                    bigint,             /* signals received */
    OUT nvcsws                      bigint,             /* total voluntary context switches */
    OUT nivcsws                     bigint              /* total involuntary context switches */
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE VIEW pg_stat_counters AS
  SELECT * FROM pg_stat_counters();

GRANT SELECT ON pg_stat_counters TO PUBLIC;


/* pg_stat_counters_all */
CREATE FUNCTION pg_stat_counters_all(
    OUT calls_1_2s                  bigint,
    OUT calls_2_3s                  bigint,
    OUT calls_3_5s                  bigint,
    OUT calls_5_10s                 bigint,
    OUT calls_10_20s                bigint,
    OUT calls_gt20s                 bigint,
    ouT plans                       bigint,
    OUT total_plan_time             double precision,   /* in msec */
    ouT calls                       bigint,
    OUT total_exec_time             double precision,   /* in msec */
    OUT rows                        bigint,
    OUT shared_blks_hit             bigint,
    OUT shared_blks_read            bigint,
    OUT shared_blks_dirtied         bigint,
    OUT shared_blks_written         bigint,
    OUT local_blks_hit              bigint,
    OUT local_blks_read             bigint,
    OUT local_blks_dirtied          bigint,
    OUT local_blks_written          bigint,
    OUT temp_blks_read              bigint,
    OUT temp_blks_written           bigint,
    OUT shared_blk_read_time        double precision,
    OUT shared_blk_write_time       double precision,
    OUT local_blk_read_time         double precision,
    OUT local_blk_write_time        double precision,
    OUT temp_blk_read_time          double precision,
    OUT temp_blk_write_time         double precision,
    OUT wal_records                 bigint,
    OUT wal_fpi                     bigint,
    OUT wal_bytes                   numeric,
    OUT wal_buffers_full            bigint,
    OUT jit_functions               bigint,
    OUT jit_generation_time         double precision,
    OUT jit_inlining_count          bigint,
    OUT jit_inlining_time           double precision,
    OUT jit_optimization_count      bigint,
    OUT jit_optimization_time       double precision,
    OUT jit_emission_count          bigint,
    OUT jit_emission_time           double precision,
    OUT jit_deform_count            bigint,
    OUT jit_deform_time             double precision,
    OUT parallel_workers_to_launch  bigint,
    OUT parallel_workers_launched   bigint,
    OUT cpu_user_time               double precision,   /* total user CPU time used, in msec  */
    OUT cpu_sys_time                double precision,   /* total system CPU time used, in msec */
    OUT minflts                     bigint,             /* total page reclaims (soft page faults) */
    OUT majflts                     bigint,             /* total page faults (hard page faults) */
    OUT nswaps                      bigint,             /* total swaps */
    OUT reads                       bigint,             /* total reads, in bytes */
    OUT writes                      bigint,             /* total writes, in bytes */
    OUT msgsnds                     bigint,             /* IPC messages sent */
    OUT msgrcvs                     bigint,             /* IPC messages received */
    OUT nsignals                    bigint,             /* signals received */
    OUT nvcsws                      bigint,             /* total voluntary context switches */
    OUT nivcsws                     bigint              /* total involuntary context switches */
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE VIEW pg_stat_counters_all AS
        SELECT * FROM pg_stat_counters_all();

GRANT SELECT ON pg_stat_counters_all TO PUBLIC;


/* dba_stat_counters */
CREATE VIEW dba_stat_counters AS
SELECT 
    pg_stat_counters.userid,
    ( SELECT pg_user.usename
        FROM pg_user
       WHERE pg_user.usesysid = pg_stat_counters.userid) AS usename,
    pg_stat_counters.dbid,
    ( SELECT pg_database.datname
        FROM pg_database
       WHERE pg_database.oid = pg_stat_counters.dbid) AS datname,
    pg_stat_counters.operationid,
    get_cmd_name(pg_stat_counters.operationid) AS operation,
    pg_stat_counters.calls_1_2s,
    pg_stat_counters.calls_2_3s,
    pg_stat_counters.calls_3_5s,
    pg_stat_counters.calls_5_10s,
    pg_stat_counters.calls_10_20s,
    pg_stat_counters.calls_gt20s,
    pg_stat_counters.plans,
    pg_stat_counters.total_plan_time,    /* in msec */
    pg_stat_counters.calls,
    pg_stat_counters.total_exec_time,    /* in msec */
    pg_stat_counters.rows,
    pg_stat_counters.shared_blks_hit,
    pg_stat_counters.shared_blks_read,
    pg_stat_counters.shared_blks_dirtied,
    pg_stat_counters.shared_blks_written,
    pg_stat_counters.local_blks_hit,
    pg_stat_counters.local_blks_read,
    pg_stat_counters.local_blks_dirtied,
    pg_stat_counters.local_blks_written,
    pg_stat_counters.temp_blks_read,
    pg_stat_counters.temp_blks_written,
    pg_stat_counters.shared_blk_read_time,
    pg_stat_counters.shared_blk_write_time,
    pg_stat_counters.local_blk_read_time,
    pg_stat_counters.local_blk_write_time,
    pg_stat_counters.temp_blk_read_time,
    pg_stat_counters.temp_blk_write_time,
    pg_stat_counters.wal_records,
    pg_stat_counters.wal_fpi,
    pg_stat_counters.wal_bytes,
    pg_stat_counters.wal_buffers_full,
    pg_stat_counters.jit_functions,
    pg_stat_counters.jit_generation_time,
    pg_stat_counters.jit_inlining_count,
    pg_stat_counters.jit_inlining_time,
    pg_stat_counters.jit_optimization_count,
    pg_stat_counters.jit_optimization_time,
    pg_stat_counters.jit_emission_count,
    pg_stat_counters.jit_emission_time,
    pg_stat_counters.jit_deform_count,
    pg_stat_counters.jit_deform_time,
    pg_stat_counters.parallel_workers_to_launch,
    pg_stat_counters.parallel_workers_launched,
    pg_stat_counters.cpu_user_time,      /* total user CPU time used, in msec  */
    pg_stat_counters.cpu_sys_time,       /* total system CPU time used, in msec */
    pg_stat_counters.minflts,            /* total page reclaims (soft page faults) */
    pg_stat_counters.majflts,            /* total page faults (hard page faults) */
    pg_stat_counters.nswaps,             /* total swaps */
    pg_stat_counters.reads,              /* total reads, in bytes */
    pg_stat_counters.writes,             /* total writes, in bytes */
    pg_stat_counters.msgsnds,            /* IPC messages sent */
    pg_stat_counters.msgrcvs,            /* IPC messages received */
    pg_stat_counters.nsignals,           /* signals received */
    pg_stat_counters.nvcsws,             /* total voluntary context switches */
    pg_stat_counters.nivcsws             /* total involuntary context switches */
FROM pg_stat_counters;

GRANT SELECT ON dba_stat_counters TO PUBLIC;

