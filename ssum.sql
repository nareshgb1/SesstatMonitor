set lines 166 pages 100
col delta for 999999999999999
col max_delta for 999999999999999
col GB for 9999999.9
col value_M for 999999999.99 head "Millions"
col sday for a10
col stat_name for a40 trunc
col max_pct for 99.99
col dow for a3
col dbname for a10

select dbname, 
	sday, dow,
	stat_name,
	min(e.snap_id) min_snap, max(e.snap_id) max_snap,
	sum(value) delta,
	max(value) max_delta,
	100*max(value)/sum(value) max_pct,
	sum(value)/1024/1024/1024 GB,
	sum(value)/1000000 value_M 
from
	(select substr(i.instance_name,1, length(i.instance_name)-1) dbname,
		s.snap_id,
		stat_name,
		s.instance_number,
		to_char(begin_interval_time, 'yyyy/mm/dd') sday,
		upper(substr(to_char(begin_interval_time, 'day'), 1,3)) dow,
		value - lag(value) over(partition by s.stat_name, s.instance_number, h.startup_time order by s.snap_id) value
	from dba_hist_sysstat s, dba_hist_snapshot h, dba_hist_database_instance i
	where stat_name like '&1' and h.snap_id = s.snap_id and begin_interval_time > trunc(sysdate-30)
   		and h.instance_number = s.instance_number
   		and h.instance_number = i.instance_number
   		and h.startup_time = i.startup_time
		and s.dbid = h.dbid  and h.dbid = i.dbid
	) e,
	v$database
where value > 0
group by dbname, stat_name, sday, dow
order by stat_name, sday
/
