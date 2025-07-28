SELECT 
date,
event_page,
profile_id,
client_type,
sum(viewing_time) OVER (
						PARTITION BY profile_id,client_type
						ORDER BY date 
						RANGE BETWEEN 6 PRECEDING AND CURRENT ROW 
						) AS viewing_time_7_days,
viewing_time
FROM 
	(SELECT 
	date,
	client_type,
	profile_id,
	event_page,
	sum(viewing_time) AS viewing_time
	FROM 
		(SELECT 
		date,
		profile_id,
		client_type,
		event_page,
		CASE WHEN event_page<>'tvchannel'AND JSONExtractInt(payload,'viewing_time')<= JSONExtractInt(payload,'duration') THEN viewing_time
			 WHEN event_page='tvchannel' AND viewing_time <86400 THEN viewing_time 
		END AS viewing_time,
		duration,
		item_type,
		payload
			FROM 
			(SELECT
				utc_timestamp::date AS date,
				profile_id,
				client_type,
				event_page,
				JSONExtractInt(payload,'viewing_time') AS viewing_time,
				JSONExtractInt(payload,'duration') AS duration,
				JSONExtractString(payload,'item_type') AS item_type,
				payload,
				utc_timestamp
			FROM
			superset.events_distr x 
			WHERE event_name='auto_player_streaming' AND utc_timestamp::date>='2024-05-13'
		    ) AS t1
		WHERE item_type IN ('series','movie','tvchannel')
		) AS t2
	GROUP BY 1,2,3,4
	) AS t3
--WHERE profile_id='8d319855-b55b-416c-aa58-c9505666e8f3'