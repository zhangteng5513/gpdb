LOAD '/home/gpadmin/workspace_wh/gpdb_metrics/src/test/regress/query_info_hook_test/query_info_hook_test.so';
SET client_min_messages='log';

-- Test normal case
SELECT 1 AS a;

--Test Error case
SELECT 1/0 AS a;
