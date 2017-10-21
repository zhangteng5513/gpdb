-- start_ignore
DROP SCHEMA IF EXISTS QUERY_METRICS CASCADE;
CREATE SCHEMA QUERY_METRICS;
SET SEARCH_PATH=QUERY_METRICS;

CREATE EXTERNAL WEB TABLE __gp_localid
(
    localid    int
)
EXECUTE E'echo $GP_SEGMENT_ID' FORMAT 'TEXT';
GRANT SELECT ON TABLE __gp_localid TO public;

CREATE EXTERNAL WEB TABLE __gp_masterid
(
    masterid    int
)
EXECUTE E'echo $GP_SEGMENT_ID' ON MASTER FORMAT 'TEXT';
GRANT SELECT ON TABLE __gp_masterid TO public;

CREATE FUNCTION gp_instrument_shmem_summary_f()
RETURNS SETOF RECORD
AS '$libdir/gp_instrument_shmem', 'gp_instrument_shmem_summary'
LANGUAGE C IMMUTABLE;
GRANT EXECUTE ON FUNCTION gp_instrument_shmem_summary_f() TO public;

CREATE VIEW gp_instrument_shmem_summary AS
WITH all_entries AS (
  SELECT C.*
    FROM __gp_localid, gp_instrument_shmem_summary_f() as C (
      segid int, num_free bigint, num_used bigint
    )
  UNION ALL
  SELECT C.*
    FROM __gp_masterid, gp_instrument_shmem_summary_f() as C (
      segid int, num_free bigint, num_used bigint
    ))
SELECT segid, num_free, num_used
FROM all_entries
ORDER BY segid;
GRANT SELECT ON gp_instrument_shmem_summary TO public;

CREATE TABLE a (id int) DISTRIBUTED BY (id);
INSERT INTO a SELECT * FROM generate_series(1, 50);
SET OPTIMIZER=OFF;
ANALYZE a;
-- end_ignore

SELECT * FROM gp_instrument_shmem_summary WHERE segid = -1;
SELECT * FROM gp_instrument_shmem_summary WHERE segid = 0;
SELECT * FROM gp_instrument_shmem_summary WHERE segid = 1;

create table foo as select i a, i b from generate_series(1, 10) i;

-- expect this query terminated by 'test pg_terminate_backend'
1&:explain analyze create temp table t as select count(*) from foo where pg_sleep(20) is null;
-- extract the pid for the previous query
SELECT pg_terminate_backend(procpid,'test pg_terminate_backend')
FROM pg_stat_activity WHERE current_query like 'explain analyze create temp table t as select%' ORDER BY procpid LIMIT 1;
1<:
1q:

-- query backend to ensure no PANIC on postmaster
select count(*) from foo;

-- ensure no instrument slot leak on fatal
SELECT * FROM gp_instrument_shmem_summary WHERE segid = -1;
SELECT * FROM gp_instrument_shmem_summary WHERE segid = 0;
SELECT * FROM gp_instrument_shmem_summary WHERE segid = 1;

-- expect this query cancelled by 'test pg_cancel_backend'
2&:explain analyze select * from pg_sleep(20);
-- extract the pid for the previous query
SELECT * from pg_sleep(2);
SELECT pg_cancel_backend(procpid,'test pg_cancel_backend')
FROM pg_stat_activity WHERE current_query like 'explain analyze select * from pg_sleep%' ORDER BY procpid LIMIT 1;
2<:
2q:

-- query backend to ensure no PANIC on postmaster
select count(*) from foo;

-- ensure no instrument slot leak
SELECT * FROM gp_instrument_shmem_summary WHERE segid = -1;
SELECT * FROM gp_instrument_shmem_summary WHERE segid = 0;
SELECT * FROM gp_instrument_shmem_summary WHERE segid = 1;

-- start_ignore
DROP SCHEMA IF EXISTS QUERY_METRICS CASCADE;
-- end_ignore
