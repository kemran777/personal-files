--------------------------------Шаг 1 ---------------------------------------------
@set dt_start = '2024-02-09'
@set dt_end = '2024-02-21' 

--DROP TABLE sandbox.mekhtiev_ab_users	
CREATE TABLE sandbox.mekhtiev_ab_users AS 	    
WITH users_ab AS ( 
		SELECT 
		event_json->>'yappy_device_id' AS yappy_device_uuid,
		appmetrica_device_id,
		event_json->>'group' AS experiment_group
		FROM appmetrica.fct_ods_appm_export_events t1 
		WHERE event_date_msk BETWEEN ${dt_start}::date AND ${dt_end}::date
		AND event_name='experiment' 
		AND event_json->>'group' IN ('lenta_weight_test_A','lenta_weight_control_B')
		GROUP BY 1,2,3
),

users_w_experiment_empty AS (
		SELECT 
		DISTINCT 
		event_json->>'yappy_device_id' AS yappy_device_uuid
		FROM appmetrica.fct_ods_appm_export_events t1 
		WHERE event_date_msk BETWEEN ${dt_start}::date AND ${dt_end}::date
		AND event_name='experiment_empty' 
		GROUP BY 1

		),

ab_users_dublicate AS  (---------Дедупликация ------------------
		SELECT yappy_device_uuid
		FROM users_ab
		GROUP BY 1
		HAVING count(yappy_device_uuid)>1
),

ab_users_dublicate_2 AS ( --------Дедупликация -----------
		SELECT yappy_device_uuid
		FROM users_ab
		GROUP BY 1
		HAVING count(appmetrica_device_id)>1

),

ab_users_wo_dublicate AS  (-------Убираем пользователей, которые попали в несколько групп и получили событие experiment_empty
		SELECT 
		t1.*,
		CASE WHEN t3.yappy_device_uuid IS NULL THEN 0 ELSE 1 END AS empty_user
		FROM users_ab AS t1
		LEFT JOIN ab_users_dublicate AS t2
		ON t1.yappy_device_uuid=t2.yappy_device_uuid
		LEFT JOIN users_w_experiment_empty  AS t3
		ON t1.yappy_device_uuid=t3.yappy_device_uuid	
		LEFT JOIN ab_users_dublicate_2  AS t4
		ON t1.yappy_device_uuid=t4.yappy_device_uuid	
        WHERE t2.yappy_device_uuid IS NULL AND t4.yappy_device_uuid IS NULL 
)


SELECT 

t1.date_msk,
t1.device_id,
t3.empty_user,
t1.profile_type,
t1.bot_flag,
t1.os_name,
t2.os_version,
t2.app_version_name,
t2.device_manufacturer,
t2.device_model,
t2.country_type,
t3.experiment_group,
t1.watchtime,
t1.broadcast
FROM
(SELECT
    date_msk,
    lower(device_id) AS device_id,
    CASE WHEN profile_uuid IS NOT NULL THEN 'Зарег' ELSE 'Незарег' END profile_type,
    client AS os_name,
    bot_flag,
    sum(ttl_depth_ms)/1000 AS watchtime,
    sum(cnt_video) AS broadcast
	FROM dds.fct_dds_backend_streams_watchtime_by_user
	WHERE reference=1 AND date_msk BETWEEN ${dt_start}::date AND ${dt_end}::date
	GROUP BY 1,2,3,4,5
) AS t1

LEFT JOIN (SELECT 
	lower(yappy_device_uuid::TEXT) AS yappy_device_uuid,
	max(device_model) AS device_model,
	max(device_manufacturer) AS device_manufacturer,
	max(country_type) AS country_type,
	max(app_version_name) AS app_version_name,
	max(os_version) AS os_version
	FROM dds.fct_dds_clear_appm_events
	WHERE event_date_msk BETWEEN ${dt_start}::date AND ${dt_end}::date
	AND event_name IN ('video_visible','video_watch','video_watched','video_add_like','video_add_comment','video_share')
	AND reference='tape'
	GROUP BY 1
	) t2
ON t1.device_id=t2.yappy_device_uuid	
	    
	    
LEFT JOIN (SELECT lower(yappy_device_uuid::TEXT) AS yappy_device_uuid,empty_user,experiment_group 
		   FROM  ab_users_wo_dublicate
		   ) AS  t3
ON t1.device_id=t3.yappy_device_uuid


SELECT max(date_msk) FROM sandbox.mekhtiev_ab_users
--
--SELECT
--
--bot_flag,os_name,profile_type,empty_user,count(device_id),count(DISTINCT device_id)
--FROM 
--(SELECT device_id,empty_user,os_name,bot_flag,profile_type,experiment_group,sum(watchtime) AS watchtime
--FROM sandbox.mekhtiev_ab_users_2
--WHERE experiment_group IS NOT NULL 
--GROUP BY 1,2,3,4,5,6
--) AS t1
--GROUP BY 1,2,3,4



--------------------------------Проверка -----------------------------------
SELECT

experiment_group,
bot_flag,
profile_type,
cnt_user,
sum(cnt_user) over(PARTITION BY profile_type),
cnt_user/sum(cnt_user) over(PARTITION BY profile_type)
FROM 
(SELECT 

experiment_group,bot_flag,profile_type,count(DISTINCT device_id) AS cnt_user
FROM 
(SELECT device_id,bot_flag,experiment_group,profile_type FROM sandbox.mekhtiev_ab_users 
 GROUP BY 1,2,3,4
) t1
GROUP BY 1,2,3
) AS t1
GROUP BY 1,2,3,4
ORDER BY 2,1


----------Проверка 2 ------------------------------
SELECT

bot_flag,profile_type,empty_user,cnt_user,sum(cnt_user) OVER (PARTITION BY bot_flag,profile_type),
cnt_user::NUMERIC/sum(cnt_user) OVER (PARTITION BY bot_flag,profile_type) AS frac
FROM 
 (SELECT bot_flag,profile_type,empty_user, count(DISTINCT device_id) AS cnt_user FROM sandbox.mekhtiev_ab_users 
  WHERE experiment_group IS NOT NULL 
GROUP BY 1,2,3
) AS t1
ORDER BY 2 ASC,1 ASC,3,4

----------------Retention -----------------------------------------------
SELECT 

device_id,  
first_date_in_app,
count(CASE WHEN retention_day=0 THEN device_id END) AS d_0,
count(CASE WHEN retention_day=1 THEN device_id END) AS d_1,
count(CASE WHEN retention_day=3 THEN device_id END) AS d_3
FROM(
	SELECT
	t2.event_date_msk as first_date_in_app, --дата нулевого дня
	t1.date_msk, 
	t1.date_msk-t2.event_date_msk as retention_day,  -- число дней, когда пользователь вернулся
	t1.device_id
	FROM (SELECT date_msk,device_id::TEXT,sum(cnt_video) AS cnt_video FROM dds.fct_dds_backend_streams_watchtime_by_user
		  WHERE date_msk BETWEEN  ${dt_start}::date AND ${dt_end}::date AND reference=1
		  GROUP BY 1,2
		  ) AS t1
	INNER JOIN (SELECT event_date_msk,yappy_device_uuid::TEXT FROM dds.fct_dds_appm_first_event_device_uuid_app_open
				WHERE event_date_msk BETWEEN  ${dt_start}::date AND ${dt_end}::date
				)AS t2
	ON t1.device_id=t2.yappy_device_uuid		
	)t1
GROUP BY 1,2