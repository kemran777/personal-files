SELECT count(*),count(parameter_id),count(parameter_id)::NUMERIC/count(*)
FROM 
(SELECT t1.id,parameter_id,title,t1.created_at FROM users AS t1
LEFT JOIN (SELECT * FROM 
				(SELECT user_id,parameter_id,title,b1.created_at,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b1.created_at ASC) AS row_num 
					FROM user_parameters b1
					LEFT JOIN (SELECT * FROM dictionaries_parameters) AS b2
					ON b1.parameter_id=b2.id
				 WHERE title IN ('utm_source','utm_content','utm_medium','utm_campaign') AND b1.created_at>='2024-03-01'
				) AS t1
			WHERE row_num=1
			) AS t2
ON t1.id=t2.user_id
WHERE user_type='regular' AND t1.created_at>='2024-03-01'
) AS t1









SELECT title,param_value,count(DISTINCT t1.id) AS cnt_user
FROM users AS t1
LEFT JOIN  (SELECT * FROM 
				(SELECT user_id,parameter_id,title,param_value,b1.created_at,ROW_NUMBER() OVER (PARTITION BY user_id,title ORDER BY b1.created_at ASC) AS row_num 
					FROM user_parameters b1
					LEFT JOIN (SELECT * FROM dictionaries_parameters) AS b2
					ON b1.parameter_id=b2.id
				 WHERE title IN ('utm_source','utm_content','utm_medium','utm_campaign') AND b1.created_at>='2024-03-01'
				) AS t1
			WHERE row_num=1
			) AS t2
ON t1.id=t2.user_id
WHERE user_type='regular' AND t1.created_at>='2024-03-01' AND title IS NOT NULL 
GROUP BY 1,2

