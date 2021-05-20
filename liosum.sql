set lines 166 pages 100
clear breaks
col logical_reads for 999,999.99 head lrd_m
col cons_chg for 999,999.99 head cons_chg_m
col physical_reads for 999,999.99 head prds_m
col prds_direct for 999,999.99 head prds_dir_m
col prds_cache for 999,999.99 head prds_cach_m
col prds_temp for 999,999.99 head prds_tmp_m
col pwts_temp for 999,999.99 head pwts_tmp_m
col prds_lob for 999,999.99 head prds_lob_m
col pwts_lob for 999,999.99 head pwts_lob_m
col pwts for 999,999.99 head pwts_m
col sday for a10
col stat_name for a40 trunc
col dow for a3
col weekno for a5 head wkno


with g as (
select name dbname, stat_name,
	sday, dow, weekno,
	min(e.snap_id) min_snap, max(e.snap_id) max_snap,
	sum(value) delta
from
	(select s.snap_id,
		stat_name,
		s.instance_number,
		to_char(begin_interval_time, 'yyyy/mm/dd') sday,
		upper(substr(to_char(begin_interval_time, 'day'),1,3)) dow,
		to_char(begin_interval_time, 'ww') weekno,
		value - lag(value) over(partition by s.stat_name, s.instance_number, startup_time order by s.snap_id) value
	from dba_hist_sysstat s, dba_hist_snapshot h
	where stat_name in ('session logical reads', 'physical reads', 'physical reads direct', 'consistent changes',
		'physical reads direct temporary tablespace', 'physical writes direct temporary tablespace',
		'physical reads direct (lob)', 'physical writes direct (lob)', 'physical writes'
		)
   	  and h.dbid = s.dbid and h.instance_number = s.instance_number
          and h.snap_id = s.snap_id
	  and h.begin_interval_time > sysdate - 30) e,
	v$database
where value > 0
group by name, stat_name, sday, dow, weekno
order by 1
),
h as (
select dbname, sday, dow, weekno,
	max(decode(stat_name, 'session logical reads', delta, 0))/1000000 logical_reads,
	max(decode(stat_name, 'consistent changes', delta, 0))/1000000 cons_chg,
	max(decode(stat_name, 'physical reads', delta, 0))/1000000 physical_reads,
	max(decode(stat_name, 'physical reads direct', delta, 0))/1000000 prds_direct,
	max(decode(stat_name, 'physical reads direct temporary tablespace', delta, 0))/1000000 prds_temp,
	max(decode(stat_name, 'physical writes', delta, 0))/1000000 pwts,
	max(decode(stat_name, 'physical writes direct temporary tablespace', delta, 0))/1000000 pwts_temp,
	max(decode(stat_name, 'physical reads direct (lob)', delta, 0))/1000000 prds_lob,
	max(decode(stat_name, 'physical writes direct (lob)', delta, 0))/1000000 pwts_lob
from g
group by dbname, sday, dow, weekno
order by 1, 2
)
select dbname, sday, dow, weekno, 
	logical_reads, cons_chg, physical_reads, prds_direct, 
	physical_reads-prds_direct prds_cache, prds_temp , pwts, pwts_temp, prds_lob, pwts_lob
from h order by 1, 2
/

