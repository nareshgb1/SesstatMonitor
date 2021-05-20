clear breaks
col sday for a10
col DB_time for 9999999999
col DB_CPU for 9999999999
col BG_time for 9999999999
col BG_CPU for 9999999999
col sql_ela for 9999999999
col rman_CPU for 9999999999
col parse_ela for 9999999999
col hard_parse_ela for 9999999999
col pl_ela for 999999
col conn_ela for 999999
col failprs_ela for 99999
col hprs_shr_ela for 99999
col instance for a10
col weekno for a4 head wkno
col DOW for a3

set lines 166

with w as (
select 
	to_char(begin_interval_time, 'yyyy/mm/dd') sday, 
	upper(substr(to_char(begin_interval_time, 'day') , 1,3)) dow,
	to_char(begin_interval_time, 'ww') weekno, 
	instance_name instance, stat_name, 
	(value - lag(value) over(partition by t.instance_number, t.stat_name, s.startup_time order by t.snap_id))/1000000 value
from dba_hist_sys_time_model t, dba_hist_snapshot s, dba_hist_database_instance i
where stat_name in ('DB time', 'DB CPU', 'background elapsed time', 'background cpu time', 'sql execute elapsed time', 'RMAN cpu time (backup/restore)',
	'parse time elapsed', 'hard parse elapsed time', 'PL/SQL execution elapsed time', 'connection management call elapsed time',
	'failed parse elapsed time', 'hard parse (sharing criteria) elapsed time' )
  and s.instance_number = t.instance_number and s.snap_id = t.snap_id
  and s.dbid = t.dbid
  and s.dbid = i.dbid and s.instance_number = i.instance_number and s.startup_time = i.startup_time
  and s.begin_interval_time < sysdate + 1 and s.begin_interval_time >= trunc(sysdate) - 30
),
w2 as (
select sday, dow, weekno, instance, stat_name, sum(value) value
from w --, gv$instance i
--where w.inst = i.inst_id
group by sday, dow, weekno, instance, stat_name
)
select instance, sday, dow, weekno, DB_time, DB_CPU, sql_ela, bg_time, bg_cpu, rman_cpu, parse_ela, hard_parse_ela,
	        hprs_shr_ela, pl_ela, conn_ela
from w2 
	pivot (sum(value) for stat_name in ('DB time' as DB_time, 'DB CPU' as db_cpu,  'background elapsed time' as bg_time,
		'background cpu time' as bg_cpu, 'sql execute elapsed time' sql_ela, 'RMAN cpu time (backup/restore)' as rman_cpu,
		'parse time elapsed' parse_ela, 'hard parse elapsed time' as hard_parse_ela, 'PL/SQL execution elapsed time' as pl_ela,
                'connection management call elapsed time' as conn_ela, 'failed parse elapsed time' as failprs_ela,
                'hard parse (sharing criteria) elapsed time' as hprs_shr_ela)
	)
order by sday, dow, weekno, instance
/

