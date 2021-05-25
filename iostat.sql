with ios as (
select snap_id, INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME,
   SMALL_READ_MEGABYTES - lag(SMALL_READ_MEGABYTES) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as srd_mb,
   SMALL_READ_REQS - lag(SMALL_READ_REQS) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as srd_req,
   LARGE_READ_MEGABYTES - lag(LARGE_READ_MEGABYTES) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as lrd_mb,
   LARGE_READ_REQS - lag(LARGE_READ_REQS) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as lrd_req,
   SMALL_WRITE_MEGABYTES - lag(SMALL_WRITE_MEGABYTES) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as swt_mb,
   SMALL_WRITE_REQS - lag(SMALL_WRITE_REQS) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as swt_req,
   LARGE_WRITE_MEGABYTES - lag(LARGE_WRITE_MEGABYTES) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as lwt_mb,
   LARGE_WRITE_REQS - lag(LARGE_WRITE_REQS) over (partition by INSTANCE_NUMBER, FUNCTION_NAME, FILETYPE_NAME order by snap_id) as lwt_req
from dba_hist_iostat_detail
where snap_id > 165035
order by snap_id, instance_number
), ios2 as (
select to_char(begin_interval_time, 'yyyy/mm/dd') hday, to_char(begin_interval_time, 'hh24:mi') hmin,
	'RMAN' as iofunction,
	sum(srd_mb + lrd_mb) rd_mb, sum(swt_mb + lwt_mb) wt_mb,
	sum(srd_req + lrd_req) rd_req, sum(swt_req + lwt_req) wt_req
from ios s, dba_hist_snapshot h
where s.snap_id = h.snap_id and s.INSTANCE_NUMBER = h.INSTANCE_NUMBER
  and FUNCTION_NAME = 'RMAN'
group by to_char(begin_interval_time, 'yyyy/mm/dd') , to_char(begin_interval_time, 'hh24:mi')
union
select to_char(begin_interval_time, 'yyyy/mm/dd') hday, to_char(begin_interval_time, 'hh24:mi') hmin,
	'Non-RMAN' as iofunction,
	sum(srd_mb + lrd_mb) rd_mb, sum(swt_mb + lwt_mb) wt_mb,
	sum(srd_req + lrd_req) rd_req, sum(swt_req + lwt_req) wt_req
from ios s, dba_hist_snapshot h
where s.snap_id = h.snap_id and s.INSTANCE_NUMBER = h.INSTANCE_NUMBER
  and FUNCTION_NAME != 'RMAN'
group by to_char(begin_interval_time, 'yyyy/mm/dd') , to_char(begin_interval_time, 'hh24:mi')
)
select s.*, s.rd_mb+wt_mb tot_mb, rd_req+wt_req tot_req
from ios2 s
where rd_mb+wt_mb>0 or rd_req+wt_req>0
/