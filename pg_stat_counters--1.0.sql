/* pg_stat_counters/pg_stat_counters--1.0.sql */

\echo Use "CREATE EXTENSION pg_stat_counters" to load this file. \quit

-- Register functions.
CREATE FUNCTION pg_stat_counters_reset()
RETURNS void
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
CREATE FUNCTION pg_stat_counters(
    OUT userid                oid,
    OUT dbid                  oid,
    OUT operation             oid,
    ouT calls                 bigint,
    OUT calls_1_2s            bigint,
    OUT calls_2_3s            bigint,
    OUT calls_3_5s            bigint,
    OUT calls_5_10s           bigint,
    OUT calls_10_20s          bigint,
    OUT calls_gt20s           bigint,
    OUT total_time            double precision,   /* in msec */
    OUT rows                  bigint,
    OUT shared_blks_hit       bigint,
    OUT shared_blks_read      bigint,
    OUT shared_blks_dirtied   bigint,
    OUT shared_blks_written   bigint,
    OUT local_blks_hit        bigint,
    OUT local_blks_read       bigint,
    OUT local_blks_dirtied    bigint,
    OUT local_blks_written    bigint,
    OUT temp_blks_read        bigint,
    OUT temp_blks_written     bigint,
    OUT blk_read_time         double precision,
    OUT blk_write_time        double precision,
    OUT wal_records           bigint,
    OUT wal_fpi               bigint,
    OUT wal_bytes             numeric,
    OUT cpu_user_time         double precision,   /* total user CPU time used, in msec  */
    OUT cpu_sys_time          double precision,   /* total system CPU time used, in msec */
    OUT minflts               bigint,             /* total page reclaims (soft page faults) */
    OUT majflts               bigint,             /* total page faults (hard page faults) */
    OUT nswaps                bigint,             /* total swaps */
    OUT reads                 bigint,             /* total reads, in bytes */
    OUT writes                bigint,             /* total writes, in bytes */
    OUT msgsnds               bigint,             /* IPC messages sent */
    OUT msgrcvs               bigint,             /* IPC messages received */
    OUT nsignals              bigint,             /* signals received */
    OUT nvcsws                bigint,             /* total voluntary context switches */
    OUT nivcsws               bigint              /* total involuntary context switches */
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE VIEW pg_stat_counters AS
  SELECT * FROM pg_stat_counters();

GRANT SELECT ON pg_stat_counters TO PUBLIC;


/* pg_stat_counters_all */
CREATE FUNCTION pg_stat_counters_all(
    OUT calls                 bigint,
    OUT calls_1_2s            bigint,
    OUT calls_2_3s            bigint,
    OUT calls_3_5s            bigint,
    OUT calls_5_10s           bigint,
    OUT calls_10_20s          bigint,
    OUT calls_gt20s           bigint,
    OUT total_time            double precision,   /* in msec */
    OUT rows                  bigint,
    OUT shared_blks_hit       bigint,
    OUT shared_blks_read      bigint,
    OUT shared_blks_dirtied   bigint,
    OUT shared_blks_written   bigint,
    OUT local_blks_hit        bigint,
    OUT local_blks_read       bigint,
    OUT local_blks_dirtied    bigint,
    OUT local_blks_written    bigint,
    OUT temp_blks_read        bigint,
    OUT temp_blks_written     bigint,
    OUT blk_read_time         double precision,
    OUT blk_write_time        double precision,
    OUT wal_records           bigint,
    OUT wal_fpi               bigint,
    OUT wal_bytes             numeric,
    OUT cpu_user_time         double precision,   /* in msec */
    OUT cpu_sys_time          double precision,   /* in msec */
    OUT minflts               bigint,             /* total page reclaims (soft page faults) */
    OUT majflts               bigint,             /* total page faults (hard page faults) */
    OUT nswaps                bigint,             /* total swaps */
    OUT reads                 bigint,             /* total reads, in bytes */
    OUT writes                bigint,             /* total writes, in bytes */
    OUT msgsnds               bigint,             /* IPC messages sent */
    OUT msgrcvs               bigint,             /* IPC messages received */
    OUT nsignals              bigint,             /* signals received */
    OUT nvcsws                bigint,             /* total voluntary context switches */
    OUT nivcsws               bigint              /* total involuntary context switches */
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
    CASE pg_stat_counters.operation
        WHEN 0::oid THEN ''::text          /* unknown */
        WHEN 1::oid THEN 'SELECT'::text
        WHEN 2::oid THEN 'UPDATE'::text
        WHEN 3::oid THEN 'INSERT'::text
        WHEN 4::oid THEN 'DELETE'::text
        WHEN 5::oid THEN 'UTILITY'::text
        WHEN 6::oid THEN 'NOTHING'::text   /* nothing */
        ELSE NULL::text
    END AS operation,
    pg_stat_counters.calls,
    pg_stat_counters.calls_1_2s,
    pg_stat_counters.calls_2_3s,
    pg_stat_counters.calls_3_5s,
    pg_stat_counters.calls_5_10s,
    pg_stat_counters.calls_10_20s,
    pg_stat_counters.calls_gt20s,
    pg_stat_counters.total_time,
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
    pg_stat_counters.blk_read_time,
    pg_stat_counters.blk_write_time,
    pg_stat_counters.wal_records,
    pg_stat_counters.wal_fpi,
    pg_stat_counters.wal_bytes,
    pg_stat_counters.cpu_user_time,
    pg_stat_counters.cpu_sys_time,
    pg_stat_counters.minflts,
    pg_stat_counters.majflts,
    pg_stat_counters.nswaps,
    pg_stat_counters.reads,
    pg_stat_counters.writes,
    pg_stat_counters.msgsnds,
    pg_stat_counters.msgrcvs,
    pg_stat_counters.nsignals,
    pg_stat_counters.nvcsws,
    pg_stat_counters.nivcsws
FROM pg_stat_counters;

GRANT SELECT ON dba_stat_counters TO PUBLIC;


