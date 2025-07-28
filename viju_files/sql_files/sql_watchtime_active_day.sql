explain
SELECT 
date,
count(DISTINCT CASE WHEN viewing_time<>0 THEN profile_id END)
FROM 
(SELECT 
t1.date,
t1.profile_id,
count(date) OVER (
                    PARTITION BY profile_id,client_type_general
                   	ORDER BY date
                    RANGE BETWEEN 6 PRECEDING AND CURRENT ROW 
                   ) AS active_days,
count(CASE WHEN viewing_time<>0 THEN date END) OVER (               
                    PARTITION BY profile_id,client_type_general
                   	ORDER BY date
                    RANGE BETWEEN 6 PRECEDING AND CURRENT ROW 
                   ) AS watch_days,     
t1.client_type_general,
COALESCE(t2.viewing_time_7_days,0) AS viewing_time_7_days,
COALESCE(t2.viewing_time,0) AS viewing_time
FROM 
(SELECT
                    utc_timestamp::date AS date,
                    profile_id,
                    CASE WHEN client_type='android_tv' THEN 'smart_tv'
                         WHEN client_type='web_mobile' THEN 'web'
                         WHEN client_type='web_desktop' THEN 'web'
                         WHEN client_type='Smart TV' THEN 'smart_tv'
                         ELSE lower(client_type)
                    END AS client_type_general
                FROM
                superset.events_distr x 
                WHERE client_type!='backend'
                AND utc_timestamp::date>='2024-04-01'
                AND profile_id IS NOT NULL  
                GROUP BY 1,2,3
                ) AS t1

LEFT JOIN 
(SELECT 
                date,
                profile_id,
                client_type_general,
                sum(viewing_time) OVER (
                                        PARTITION BY profile_id,client_type_general
                                        ORDER BY date 
                                        RANGE BETWEEN 6 PRECEDING AND CURRENT ROW 
                                        ) AS viewing_time_7_days,
                viewing_time
                FROM 
                    (SELECT 
                    date,
                    CASE WHEN client_type='android_tv' THEN 'smart_tv'
                     WHEN client_type='web_mobile' THEN 'web'
                     WHEN client_type='web_desktop' THEN 'web'
                     WHEN client_type='Smart TV' THEN 'smart_tv'
                     ELSE lower(client_type)
                    END AS client_type_general,
                    profile_id,
                    sum(viewing_time) AS viewing_time
                    FROM 
                        (SELECT 
                        date,
                        profile_id,
                        client_type,
                        event_page,
                        CASE WHEN event_page<>'tvchannel'AND JSONExtractInt(payload,'viewing_time')<= JSONExtractInt(payload,'duration') THEN viewing_time
                             WHEN event_page='tvchannel' AND viewing_time <18000 THEN viewing_time 
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
                            WHERE event_name='auto_player_streaming' AND utc_timestamp::date>='2024-04-01'
                            ) AS t1
                        WHERE item_type IN ('series','movie','tvchannel') AND viewing_time IS NOT NULL
                        ) AS t2
                    GROUP BY 1,2,3
                    ) AS t3
                    ) AS t2
   ON t1.date=t2.date AND t1.profile_id=t2.profile_id AND t1.client_type_general=t2.client_type_general
   )
   GROUP BY 1