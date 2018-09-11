set log_min_messages to ERROR;
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
select gp_inject_fault ('dtm_broadcast_prepare', 'reset', 1);
SELECT gp_inject_fault ('dtm_broadcast_prepare', 'error', 1);
create table abc1 (id int, ctx text);
select gp_inject_fault ('dtm_broadcast_prepare', 'reset', 1);
select session_id AS session_not_in_stat_activity from (select paramvalue AS session_id from gp_toolkit.gp_param_settings() where paramname='gp_session_id' and paramsegment = 0) right_id where right_id.session_id not in (select sess_id::text from pg_stat_activity);
set log_min_messages to default;
