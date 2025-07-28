select 
time_key,
market_key,
last_subs_key,
last_ban_key,
segment_key,
total_rev,
service_margin,
data_mb,

dense_rank() over (order by segment_key desc,  total_rev desc, service_margin  desc) row_num,
--В числителе сумма накопительным итогом, в знаменателе тотальная сумма даты
sum(data_mb) over (partition by segment_key order by time_key  rows between unbounded preceding and current row) data_mb_nakopitelno,
round(sum(data_mb) over (partition by segment_key order by time_key  rows between unbounded preceding and current row)/sum(data_mb) over (partition by segment_key order by time_key)*100,8) data_percent,
lag(total_rev) over (partition by segment_key order by time_key) lag,
lead(total_rev) over (partition by segment_key order by time_key) lead,
PERCENT_RANK() over(partition by segment_key order by time_key) cume_dist,
PERCENTILE_DISC(1) WITHIN GROUP (ORDER BY total_rev) OVER (PARTITION BY time_key) Percentile_Disc,

/*FIRST_VALUE(total_rev)  over(partition by segment_key order by time_key ) First_Value,*/
/*last_value(total_rev) over (partition by segment_key order by time_key) last_value, *\*/

case
when round(sum(data_mb) over (partition by segment_key order by time_key rows between unbounded preceding and current row)/sum(data_mb) over (partition by segment_key order by time_key)*100,8)<10 then 1 
  else 0 end type_ind
 
 from dwh.fct_act_clust_subs_m@dwhprd t1
  

 
 
 where  t1.time_key=date'2021-11-01' and t1.market_key='ABK' and aab_eop1m>0 and data_mb>0
 
 
 group by 
time_key,
market_key,
last_subs_key,
last_ban_key,
segment_key,
total_rev,
service_margin,
data_mb
