col METRIC_UNIT for a30 trunc
col metric_name for a50
col avg_avg for 999999999999
col max_max for 999999999999
col max_avg for 999999999999
set lines 166
col avg_mb for 9999999.9
col max_avg_mb for 9999999.9
col max_max_mb for 9999999.9
col inst for 99
col DOW for a3

select d.name dbname, to_char(begin_time, 'yyyy/mm/dd') sday, upper(substr(to_char(begin_time, 'day'), 1, 3)) dow,
  m.instance_number inst, metric_name, avg(average) avg_avg , max(average) max_avg , max(maxval) max_max,
	avg(average)/1024/1024 avg_mb, max(average)/1024/1024 max_avg_mb, max(maxval)/1024/1024 max_max_mb
from v$database d, DBA_HIST_SYSMETRIC_summary m, dba_hist_snapshot h
where h.begin_interval_time > trunc(sysdate) - 30 and h.begin_interval_time < sysdate + 4/24
 and h.snap_id = m.snap_id
 and h.instance_number = m.instance_number
 and (metric_name) in ('I/O Requests per Second', 
	'Physical Read Total IO Requests Per Sec', 'Physical Write Total IO Requests Per Sec',
	'Physical Read IO Requests Per Sec', 'Physical Write IO Requests Per Sec',
	'Physical Read Total Bytes Per Sec', 'Physical Write Total Bytes Per Sec', 
	'Physical Read Bytes Per Sec', 'Physical Write Bytes Per Sec', 
	'Background CPU Usage Per Sec', 'CPU Usage Per Sec', 'Host CPU Usage Per Sec',
	'I/O Megabytes per Second', 'I/O Requests per Second')
 and m.dbid = h.dbid
group by d.name, to_char(begin_time, 'yyyy/mm/dd') , m.instance_number , metric_name, upper(substr(to_char(begin_time, 'day'), 1, 3))
order by 1,2,3
/