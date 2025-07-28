SELECT  date_trunc('quarter',main_date) AS quarter,
round(sum(total_sum_by_day), 0) AS payment,
count(DISTINCT user_id)
FROM datamarts.financial_activity_distr 
WHERE main_date>='2023-10-01'
GROUP BY 1





----------Watchtime ---------------

SELECT  
date_trunc('quarter',date) AS quarter,
sum(watchtime) AS watchtime,
uniq(profile_id) AS cnt_user,
watchtime/cnt_user AS watchtime_per_user,
SUM(watchtime_session_watch)/uniq(case when session_watch>=1 then profile_id end) AS watchtime_per_watch_user
FROM datamarts.mekhtiev_watchtime_by_day_distr
WHERE date>='2023-10-01'
GROUP BY 1



-- Сессия ----
SELECT 
date_trunc('quarter',date) AS quarter,
uniq(profile_id) AS cnt_user,
sum(session_cnt) AS session_cnt,
session_cnt/cnt_user AS session_per_user,
sum(session_watch_cnt)  AS session_watch_cnt,
session_watch_cnt/session_cnt AS cr_watch_session
FROM 
(SELECT 
date,
profile_id,
b2c_b2b,
max(session_cnt_ttl) AS session_cnt,
max(session_watch_ttl) AS session_watch_cnt
FROM datamarts.mekhtiev_watchtime_by_day_distr
WHERE date>='2023-10-01'
GROUP BY 1,2,3
)
GROUP BY 1



---- Новые зареги --------
SELECT 
date_trunc('quarter',reg_date) AS quarter,
uniq(profile_id) AS cnt_user
FROM 
(SELECT t1.*,
uniq(profile_id) OVER (PARTITION BY created_at::date) AS cnt_day_user,
CASE WHEN promo_type='cards' THEN 'b2b' ELSE 'b2c' END AS b2c_b2b
FROM datamarts.marketing_dash_distr AS t1
)
WHERE reg_date>='2023-10-01'
GROUP BY 1


------ Новые посетители ----------
SELECT 
date_trunc('quarter',min_date_visitor) AS quarter,
count(DISTINCT visitor_id) cnt_visitor,
uniq(profile_id)/uniq(visitor_id) AS cr_to_reg
FROM datamarts.mekhtiev_min_date_visitor_profile_distr
WHERE min_date_visitor>='2023-10-01'
GROUP BY 1

---- Новые триал --------
SELECT 
date_trunc('quarter',created_at) AS quarter,
uniq(profile_id) AS cnt_user
FROM 
(SELECT t1.*,
uniq(profile_id) OVER (PARTITION BY created_at::date) AS cnt_day_user,
CASE WHEN promo_type='cards' THEN 'b2b' ELSE 'b2c' END AS b2c_b2b
FROM datamarts.marketing_dash_distr AS t1
)
WHERE created_at>='2023-10-01'
GROUP BY 1


---- Новые подписчики --------
SELECT 
date_trunc('quarter',first_prolong_date) AS quarter,
uniq(profile_id) AS cnt_user
FROM 
(SELECT t1.*,
uniq(profile_id) OVER (PARTITION BY created_at::date) AS cnt_day_user,
CASE WHEN promo_type='cards' THEN 'b2b' ELSE 'b2c' END AS b2c_b2b
FROM datamarts.marketing_dash_distr AS t1
)
WHERE first_prolong_date>='2023-10-01'
GROUP BY 1

