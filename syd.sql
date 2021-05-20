col stat_name for a50 trunc
col waits_sec for 999999.9
col av_wait_ms for 999999.9
col delta for 999999999999
col mb for 999999
col host_name for a12 trunc
col instance for a10
col snap_id for 99999999
col snap_int for a25
set lines 166


select
	host_name
	, instance
	, stat_name
	, snap_id
	, snap_int
	, inst
	, delta
	, delta/1048576 mb
--	, waits_Sec
--	, decode(waits_delta, 0, 0, waits_sec*1000/waits_delta) av_wait_ms
from (
select s1.stat_name, s1.snap_id
	, i.host_name
	, i.instance_name instance
        , to_char(h.begin_interval_time, 'dd-mon-yy hh24:mi') || ' - ' || to_char(h.end_interval_time,  'hh24:mi') snap_int
        , to_char(h.begin_interval_time, 'dd-mon-yy') hday, to_char(h.begin_interval_time, 'hh24') h_hr,  to_char(h.begin_interval_time, 'mi') h_min
	, h.instance_number inst
        , lag(value) over(order by s1.snap_id) prev_waits
        , value - lag(value) over(partition by stat_name, s1.instance_number, h.startup_time order by s1.snap_id) as delta
        --, (TIME_WAITED_MICRO - lag(TIME_WAITED_MICRO) over(partition by stat_name, s1.instance_number, h.startup_time order by s1.snap_id))/1000000 as waits_sec
from dba_hist_sysstat s1, dba_hist_snapshot h, dba_hist_database_instance i
where h.begin_interval_time between trunc(sysdate-1) - .02 and trunc(sysdate)
  and s1.stat_name like '&1' 
  and h.snap_id = s1.snap_id
  and h.instance_number = s1.instance_number
  and h.dbid = i.dbid and h.instance_number = i.instance_number
  and h.dbid = s1.dbid 
  and h.startup_time = i.startup_time
) where prev_waits is not null and delta >= 0
order by stat_name
	, snap_id
	, inst
/
