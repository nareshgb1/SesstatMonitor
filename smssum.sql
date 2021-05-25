col METRIC_UNIT for a30 trunc
col dbname for a10
col sday for a10
col metric_name for a50
col avg_avg for 9999999999.99
col max_max for 99999999999.99
col max_avg for 9999999999.99
set lines 166
set pages 40
col avg_mb for 9999.99
col max_avg_mb for 9999.99
col max_max_mb for 9999.99
col inst for 99
col DOW for a3
set verify off


select d.db_unique_name dbname, to_char(begin_time, 'yyyy/mm/dd') sday, upper(substr(to_char(begin_time, 'day'), 1, 3)) dow,
  m.instance_number inst, metric_name, avg(average) avg_avg , max(average) max_avg , max(maxval) max_max,
	avg(average)/1024/1024 avg_mb, max(average)/1024/1024 max_avg_mb, max(maxval)/1024/1024 max_max_mb
from v$database d, DBA_HIST_SYSMETRIC_summary m, dba_hist_snapshot h
where h.begin_interval_time > trunc(sysdate) - 30 and h.begin_interval_time < sysdate + 4/24
 and h.snap_id = m.snap_id
 and h.instance_number = m.instance_number
 and lower(metric_name) like lower('&1')
 and m.dbid = h.dbid
group by d.db_unique_name, to_char(begin_time, 'yyyy/mm/dd') , m.instance_number , metric_name, upper(substr(to_char(begin_time, 'day'), 1, 3))
order by metric_name, 1,2,3
/