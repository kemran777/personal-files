--
--DROP TABLE sandbox.mekhtiev_plan_fact_local ON CLUSTER 'viasat_cluster' sync
--DROP TABLE sandbox.mekhtiev_plan_fact ON CLUSTER 'viasat_cluster' sync
--
CREATE TABLE sandbox.mekhtiev_plan_fact_local ON CLUSTER 'viasat_cluster'
            (
			date_month Date,
			type String,
			new_subs Int,
			all_subs Int,
			arppu Float,
		    revenue Int,
			revenue_partner Int
			)
             ENGINE = ReplicatedMergeTree('/clickhouse/tables/v1/{shard}/mekhtiev_plan_fact_local', '{replica}')
             ORDER BY date_month
--             
--             
--             
-- 
-- 
CREATE TABLE sandbox.mekhtiev_plan_fact ON CLUSTER 'viasat_cluster'
            (
			date_month Date,
			type String,
			new_subs Int,
			all_subs Int,
			arppu Float,
			revenue Int,
			revenue_partner Int
                )
             
            ENGINE = Distributed(viasat_cluster, sandbox, mekhtiev_plan_fact_local, rand())


----- Новые подписчики ------
 SELECT date_trunc('month',first_prolong_date) AS date,count(profile_id) AS cnt_user
 FROM datamarts.marketing_dash
 WHERE first_prolong_date::date>='2025-01-01' --AND promo_type='cards'
 GROUP BY 1
 
 ----- Все подписчики -----
WITH dates AS (
SELECT 
    toDate('2024-01-01') AS start_date,
    toDate(now()) AS end_date,
    arrayJoin(range(toUInt32(end_date - start_date) + 1)) AS offset,
    addDays(start_date, offset) AS dt
),
df AS (
	SELECT *
	FROM datamarts.marketing_dash AS df1
	WHERE
		payer = 1)
SELECT
    dates.dt AS check_date, 
    COUNT(*) AS subs
FROM
    dates
CROSS JOIN df
WHERE 
    toDate(df.first_prolong_date) <= dates.dt
    AND toDate(df.ends_at) >= dates.dt
GROUP BY
    check_date
ORDER BY 
    check_date ASC

------ Выручка ---------
SELECT date_trunc('month',paid_date) AS date,sum(payment) AS income
FROM  datamarts.finance
GROUP BY 1
ORDER BY 1

SELECT * FROM sandbox.mekhtiev_plan_fact_local

--- ARPPU смотрим в файле, который Андрей заполняет---------
https://docs.google.com/spreadsheets/d/1Hap7W-XtZYW4rsRCZkUxoz8M3r3tWQwkdX5uklGsOXk/edit?gid=0#gid=0

TRUNCATE TABLE sandbox.mekhtiev_plan_fact_local ON CLUSTER 'viasat_cluster' sync 


INSERT INTO sandbox.mekhtiev_plan_fact (date_month, type, new_subs, all_subs, arppu, revenue, revenue_partner) 
VALUES 
    ('2025-01-01', 'fact', 2909, 10350, 209, 1946374, 3377054),
    ('2025-01-01', 'plan', 2808, 8583, 312, 2678037, 4709000),
    
    ('2025-02-01', 'fact', 2779, 11411, 204, 1744357, 2719639),
    ('2025-02-01', 'plan', 3005, 9614, 312, 2999720, 4833000),
    
    ('2025-03-01', 'fact', 2610, 11872, 201, 1998147, 3557108),
    ('2025-03-01', 'plan', 3077, 10480, 312, 3269894, 5417000),
    
    ('2025-04-01', 'fact', 3327, 13118, 231, 2502382, 2864097),
    ('2025-04-01', 'plan', 3314, 11489, 312, 3584586, 6725000),
    
    ('2025-05-01', 'fact', 3254, 14016, 235, 2469983, 2567352),
    ('2025-05-01', 'plan', 3346, 12308, 312, 3840069, 7243000),
    
    ('2025-06-01', 'fact', 2190, 14208, 239, 1426694, 0),
    ('2025-06-01', 'plan', 3688, 13288, 312, 4146002, 7495000),
    
    ('2025-07-01', 'plan', 3826, 14324, 312, 4469116, 7765000),
    ('2025-08-01', 'plan', 4003, 15319, 312, 4779569, 8113000),
    ('2025-09-01', 'plan', 4306, 16408, 312, 5119355, 8475500),
    ('2025-10-01', 'plan', 4329, 17291, 312, 5394944, 8909000),
    ('2025-11-01', 'plan', 4585, 18246, 312, 5692677, 9217000),
    ('2025-12-01', 'plan', 4608, 19023, 312, 5935044, 9621000);