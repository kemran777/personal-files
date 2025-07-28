
----visitor cnt -------
SELECT 
client_type_general,
uniq(visitor_id),
uniq(profile_id),
uniq(profile_id)/uniq(visitor_id)
FROM datamarts.mekhtiev_min_date_visitor_profile_distr
WHERE min_date_visitor BETWEEN '2024-08-01' AND '2024-08-31' --AND client_type_general =''
GROUP BY 1

SELECT * FROM datamarts.mekhtiev_min_date_visitor_profile_distr



------trial/subs/reg-------
SELECT
device,
--free_days,
uniq(CASE WHEN reg_date::date!='1970-01-01' THEN profile_id END) AS  reg_cnt,
uniq(CASE WHEN created_at::date!='1970-01-01' THEN profile_id END) AS trial_cnt,
uniq(CASE WHEN first_prolong_date::date <> '1970-01-01' THEN profile_id END) AS subs_cnt
FROM datamarts.marketing_dash_distr AS t1
WHERE  reg_date BETWEEN '2024-08-01' AND '2024-08-31'
GROUP BY 1

--- Churn rate ---
WITH t1 AS (SELECT     
			reg_date,
			device,
			day_0,
			day_1_30,
			day_31_60,
			day_91_120,
			day_121_150,
			day_151_180,
			month_1,
			month_2,
			month_3,
			month_4,
			month_5,
			month_6,
			CASE WHEN reg_date BETWEEN now() - INTERVAL '30' DAY AND now() THEN NULL ELSE AVG(month_1) OVER (ORDER BY reg_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS month_1_avg,
			CASE WHEN reg_date BETWEEN now() - INTERVAL '60' DAY AND now() THEN NULL ELSE AVG(month_2) OVER (ORDER BY reg_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS month_2_avg,
			CASE WHEN reg_date BETWEEN now() - INTERVAL '90' DAY AND now() THEN NULL ELSE AVG(month_3) OVER (ORDER BY reg_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS month_3_avg,
			CASE WHEN reg_date BETWEEN now() - INTERVAL '120' DAY AND now() THEN NULL ELSE AVG(month_4) OVER (ORDER BY reg_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS month_4_avg,
			CASE WHEN reg_date BETWEEN now() - INTERVAL '150' DAY AND now() THEN NULL ELSE AVG(month_5) OVER (ORDER BY reg_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS month_5_avg,
			CASE WHEN reg_date BETWEEN now() - INTERVAL '180' DAY AND now() THEN NULL ELSE AVG(month_6) OVER (ORDER BY reg_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS month_6_avg
			FROM 
				(SELECT
				t1.*,
				CASE WHEN reg_date BETWEEN now() - INTERVAL '30' DAY AND now() THEN NULL ELSE  day_1_30::float/day_0 END AS month_1,
				CASE WHEN reg_date BETWEEN now() - INTERVAL '60' DAY AND now() THEN NULL ELSE day_31_60::float/day_0 END AS month_2,
				CASE WHEN reg_date BETWEEN now() - INTERVAL '90' DAY AND now() THEN NULL ELSE day_61_90::float/day_0 END AS month_3,
				CASE WHEN reg_date BETWEEN now() - INTERVAL '120' DAY AND now() THEN NULL ELSE day_91_120::float/day_0 END AS month_4,
				CASE WHEN reg_date BETWEEN now() - INTERVAL '150' DAY AND now() THEN NULL ELSE day_121_150::float/day_0 END AS month_5,
				CASE WHEN reg_date BETWEEN now() - INTERVAL '180' DAY AND now() THEN NULL ELSE day_151_180::float/day_0 END AS month_6
				FROM
					(SELECT
					reg_date,
					device,
					uniqExactIf(profile_id,retention_day=0) AS day_0,
					uniqExactIf(profile_id,retention_day BETWEEN 1 AND 30) AS day_1_30,
					uniqExactIf(profile_id,retention_day BETWEEN 31 AND 60) AS day_31_60,
					uniqExactIf(profile_id,retention_day BETWEEN 61 AND 90) AS day_61_90,
					uniqExactIf(profile_id,retention_day BETWEEN 91 AND 120) AS day_91_120,
					uniqExactIf(profile_id,retention_day BETWEEN 121 AND 150) AS day_121_150,
					uniqExactIf(profile_id,retention_day BETWEEN 151 AND 180) AS day_151_180
					FROM (SELECT
			                    utc_timestamp::date AS date,
			                    profile_id,
			                    reg_date,
			                    device,
			                    date - reg_date AS retention_day,
			                    count(event_page) AS cnt_event
			                    FROM datamarts.sandbox_data_distr AS t1
			                    LEFT JOIN product_x.users AS u ON t1.user_id=u.id
			                    WHERE event_name IN ('auto_player_streaming','auto_kinom_streaming')
			                    AND utc_timestamp::date BETWEEN '2024-01-01' AND '2024-10-01'
			                    AND reg_date::date BETWEEN '2023-09-01' AND yesterday()
			                    AND reg_date != '1970-01-01'
			                    AND profile_id IS NOT NULL
			                    AND (u.email NOT LIKE '%@test%' OR u.email IS NULL)
			                    GROUP BY 1,2,3,4,5
			                    )
					WHERE reg_date>='2024-01-01'
					GROUP BY 1,2
					)  AS t1
					)
			WHERE reg_date NOT IN ('2024-04-28','2024-04-29') AND day_0!=0
			)
			
			SELECT 
			1 - avg(month_1) AS m_1,
			1 - avg(month_2) AS m_2,
			1 - avg(month_3) AS m_3,
			1 - avg(month_4) AS m_4,
			1 - avg(month_5) AS m_5,
			1 - avg(month_6) AS m_6
			FROM t1
			WHERE device='tv'
			
			
-----  LT --------
SELECT 
device,
avg(lt) AS lt
FROM 
(SELECT
device,
profile_id,
first_prolong_date::date AS first_prolong_date,
ends_at,
ends_at - first_prolong_date AS lt
FROM datamarts.marketing_dash_distr AS t1
WHERE first_prolong_date BETWEEN '2024-01-01' AND '2024-10-31'
)
GROUP BY 1
------- ARPU/ ARPPU ----
			
SELECT reg_device,sum(total_sum_by_day)/uniq(user_id) AS ARPU,
countIf(DISTINCT user_id, payer_day_flag = 1) != 0,sum(total_sum_by_day) / countIf(DISTINCT user_id, payer_day_flag = 1) AS ARPPU
FROM datamarts.financial_activity_distr 
WHERE main_date>='2024-09-01'
GROUP BY 1

    