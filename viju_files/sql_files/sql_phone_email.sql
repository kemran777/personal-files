SELECT len,t2.email,t2.phone_number
FROM (SELECT * FROM datamarts.dash_table_distr
	  WHERE len>=2
	  )AS t1
LEFT JOIN product_x.users  AS t2
ON t1.user_id=t2.id
--WHERE created_at::date BETWEEN '2024-07-08' AND '2024-07-14'
GROUP BY 1,2,3





SELECT created_at,t2.email,t2.phone_number
FROM (SELECT * FROM datamarts.dash_table_distr
	  )AS t1
LEFT JOIN product_x.users  AS t2
ON t1.user_id=t2.id
WHERE created_at::date BETWEEN '2024-07-15' AND '2024-07-21'
GROUP BY 1,2,3
