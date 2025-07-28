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


---- Новые платные подписчики --------
SELECT 
date_trunc('quarter',first_prolong_date) AS quarter,
uniq(profile_id) AS cnt_user
FROM 
(SELECT t1.*,
uniq(profile_id) OVER (PARTITION BY created_at::date) AS cnt_day_user,
CASE WHEN promo_type='cards' THEN 'b2b' ELSE 'b2c' END AS b2c_b2b
FROM datamarts.marketing_dash_distr AS t1
)
WHERE created_at>='2023-10-01'
GROUP BY 1

----Конверсия в повторную подписку --------
WITH t1 AS (SELECT 
	         paid_date,
	         user_id,
	         offer_duration,
	         row_number() OVER (PARTITION BY user_id ORDER BY paid_date) AS rn_num
	         FROM datamarts.finance AS t1
	         LEFT JOIN datamarts.marketing_dash AS t2 on t1.user_id = t2.user_id
	         )
	         
SELECT date_trunc('quarter',paid_date),
uniq(CASE WHEN rn_num=1 THEN user_id END) AS month_1,
uniq(CASE WHEN rn_num=2 THEN user_id END) AS month_2,
month_2/month_1
FROM t1
WHERE offer_duration='1 month' AND paid_date<= now() - INTERVAL '32' DAY
GROUP BY 1

------Отток в рамках квартала для офферов 1 месяц и 3 месяца --------
WITH info_all AS (SELECT 
                paid_at::date AS paid_date,
                s.user_id AS user_id,
                i.subscription_id AS subscription_id,
                s.created_at::date AS created_date,
                t2.first_prolong_date::date AS first_prolong_date,
                t2.ends_at AS ends_at,
                t2.reg_source AS reg_source,
                t2.state AS state,
                t2.reg_medium AS reg_medium,
                t2.bonus_title AS bonus_title,
                t2.offer_duration AS offer_duration,
                CASE WHEN t2.promo_type='cards' THEN 'b2b' ELSE 'b2c' END AS b2c_b2b,
                i.price_currency AS price_currency,
                sum(CASE WHEN i.price_currency='USD' THEN i.price_cents*90/100
                         WHEN i.price_currency='AMD' THEN i.price_cents*0.25/100
                         ELSE i.price_cents/100
                         END) AS payment
                FROM  product_x.invoices i
                LEFT JOIN product_x.subscriptions s ON s.id = i.subscription_id
                LEFT JOIN product_x.users u ON u.id = s.user_id
                INNER JOIN (SELECT 
                           user_id,
                           first_prolong_date::date AS first_prolong_date,
                           reg_source,
                           state,
                           reg_medium,
                           ends_at,
                           bonus_title,
                           offer_duration,
                           promo_type
                           FROM datamarts.marketing_dash
                           --WHERE first_prolong_date BETWEEN '2024-10-01' AND '2024-11-30'
                           WHERE first_prolong_date BETWEEN '2024-06-01' AND '2024-09-31'
                           GROUP BY 1,2,3,4,5,6,7,8,9
                           ) AS t2 
                        ON s.user_id=t2.user_id
                WHERE u.user_type = 'regular'
                AND u.vipplay = FALSE
                AND s.state in ('normal_period','trial','canceled','grace_period')
              --  AND i.paid_at BETWEEN '2024-10-01' AND '2024-12-31'
                AND i.paid_at BETWEEN '2024-06-01' AND '2024-12-31'
                AND i.state in ('success')
                AND ((u.email NOT ILIKE '%%@test%%' AND u.email NOT ILIKE '%%@viasat%%') OR (u.email IS NULL AND u.phone_number IS NOT NULL))
                AND i.price_cents > 100
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
 )
 
 
 			  
SELECT 
offer_duration, --,state,user_id,first_prolong_date,ends_at,
CASE WHEN ends_at<='2024-12-31' THEN 0 ELSE 1 END type_prolong,uniq(user_id),count(user_id)
FROM info_all
WHERE offer_duration='3 month' 
GROUP BY 1,2
ORDER BY 2