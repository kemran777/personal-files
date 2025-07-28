---- Новые Посетители -------
SELECT 
toStartOfMonth(min_date_visitor) AS date_month,
count(DISTINCT visitor_id) AS cnt_us
FROM datamarts.mekhtiev_min_date_visitor_profile_distr
GROUP BY 1


------ Новые Зареги -----
SELECT 
toStartOfMonth(reg_date) AS date_month,
uniq(profile_id) AS cnt_user
FROM datamarts.marketing_dash_distr
WHERE reg_date>='2024-03-01' AND reg_date!='1970-01-01'
GROUP BY 1


------- Новые Триалы -------
SELECT 
toStartOfMonth(created_at) AS date_month,
--CASE WHEN free_days=3 AND price_cents/100 IN (399,499) THEN '3_month3'
--	 WHEN free_days=3 AND price_cents/100=1190 THEN '3_month12'
--	 ELSE toString(free_days)
--END AS free_days,
uniq(profile_id) AS cnt_user
FROM datamarts.marketing_dash_distr x
LEFT JOIN product_x.users AS u ON x.user_id=u.id
WHERE created_at::date>='2024-03-01' AND created_at::date!='1970-01-01' AND reg_date!='1970-01-01'
AND (u.email NOT LIKE '%@test%' OR u.email IS NULL)
--AND free_days IN ('3_month3','3_month12','14','30','35','45')
GROUP BY 1
--,2

------- Новые Subs -------
SELECT 
toStartOfMonth(first_prolong_date) AS date_month,
CASE WHEN free_days=3 AND price_cents/100 IN (399,499) THEN '3_month3'
	 WHEN free_days=3 AND price_cents/100=1190 THEN '3_month12'
	 ELSE toString(free_days)
END AS free_days,
uniq(profile_id) AS cnt_user
FROM datamarts.marketing_dash_distr AS x
LEFT JOIN product_x.users AS u ON x.user_id=u.id
WHERE first_prolong_date::date>='2024-03-01' 
AND first_prolong_date::date!='1970-01-01' AND reg_date!='1970-01-01'
AND (u.email NOT LIKE '%@test%' OR u.email IS NULL)
AND free_days IN ('3_month3','3_month12','14','30','35','45')
GROUP BY 1,2


----- ARPPU --------
SELECT *
FROM 
(SELECT
toStartOfMonth(paid_at) AS date_month,
CASE WHEN free_days=3 AND i.price_cents/100 IN (399,499) THEN '3_month3'
	 WHEN free_days=3 AND i.price_cents/100=1190 THEN '3_month12'
	 ELSE toString(free_days)
END AS free_days,
uniq(s.user_id) cnt_user,
sum(i.price_cents)/100 AS payment,
round(payment/cnt_user,0)::int AS ARPPU
FROM 
(SELECT * FROM  product_x.invoices
 WHERE state in ('success')
 AND price_cents > 100
 AND price_currency = 'RUB'
 AND paid_at::date BETWEEN '2024-03-01' AND '2024-08-31'
 ) AS i
LEFT JOIN product_x.subscriptions s ON s.id = i.subscription_id
LEFT JOIN product_x.users u ON u.id = s.user_id
LEFT JOIN datamarts.marketing_dash_distr m ON m.user_id=s.user_id
WHERE u.user_type = 'regular' 
AND u.vipplay = FALSE
AND s.state in ('normal_period','canceled')
AND payer=1
--AND (u.email NOT LIKE '%@test%' OR u.email IS NULL)
GROUP BY 1,2)
WHERE free_days IN ('3_month3','3_month12','14','30','35','45')
--WHERE free_days = '0'

------- Retention в подписку --------
     
SELECT 
delta_month,
CASE WHEN free_days=3 AND price_cents/100 IN (399,499) THEN '3_month3'
	 WHEN free_days=3 AND price_cents/100=1190 THEN '3_month12'
	 ELSE toString(free_days)
END AS free_days,
uniq(user_id) AS cnt_user
FROM 
(SELECT
paid_at::date AS paid_date,
free_days,
s.user_id  AS user_id,
i.price_cents AS price_cents,
min(paid_date) OVER (PARTITION BY s.user_id) AS min_paid_date,
(EXTRACT(YEAR FROM paid_date) - EXTRACT(YEAR FROM min_paid_date)) * 12 +  
(EXTRACT(MONTH FROM paid_date) - EXTRACT(MONTH FROM min_paid_date)) AS delta_month 
FROM 
(SELECT * FROM  product_x.invoices
 WHERE state in ('success')
 AND price_cents > 100
 AND price_currency = 'RUB'
 AND paid_at::date BETWEEN '2024-03-01' AND '2024-08-31'
 ) AS i
LEFT JOIN product_x.subscriptions s ON s.id = i.subscription_id
LEFT JOIN product_x.users u ON u.id = s.user_id
LEFT JOIN datamarts.marketing_dash_distr m ON m.user_id=s.user_id
WHERE u.user_type = 'regular' 
AND u.vipplay = FALSE
AND s.state in ('normal_period','canceled')
AND (u.email NOT LIKE '%@test%' OR u.email IS NULL)
AND payer=1
)
--WHERE free_days IN ('3_month3','3_month12','14','30','35','45')
WHERE free_days IN '0'
GROUP BY 1,2

---------- Watchtime -------
SELECT 
toStartOfMonth(date) AS date_month,
CASE WHEN free_days='3' AND m.price_cents/100 IN (399,499) THEN '3_month3'
	 WHEN free_days='3' AND m.price_cents/100=1190 THEN '3_month12'
	 ELSE free_days
END AS free_days,
sum(watchtime)/60 AS watchtime,
uniq(profile_id) AS cnt_user,
round(watchtime/cnt_user,0)::int AS watchtime_per_user
FROM datamarts.mekhtiev_watchtime_by_day_distr AS x
LEFT JOIN datamarts.marketing_dash_distr m ON x.profile_id=m.profile_id
WHERE date BETWEEN '2024-03-01' AND '2024-08-31'
AND free_days IN ('3_month3','3_month12','14','30','35','45')
GROUP BY 1,2
ORDER BY 2,1 ASC 