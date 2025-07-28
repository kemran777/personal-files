SELECT 
t4.title AS bonus_title,
t1.title AS title,
JSONExtractInt(t4.metadata,'duration') AS duration,
JSONExtractString(t4.metadata,'duration_unit') AS duration_unit,
t3.activated_at AS bonus_activated_at,
t3.activated_at::date AS bonus_start_at,
t3.activated_at::date + JSONExtractInt(t4.metadata,'duration') AS bonus_end_at

FROM product_x.promo_codes AS t1

LEFT JOIN (SELECT * FROM product_x.bonus_programs bp 
			) t4
ON t1.bonus_program_id=t4.id

LEFT JOIN (SELECT * FROM product_x.promo_code_activations
		   ) AS t2
ON 	t1.id=t2.promo_code_id


LEFT JOIN (SELECT * FROM  product_x.user_bonuses
		  ) AS t3
ON t2.user_bonus_id=t3.id

WHERE bonus_activated_at BETWEEN'2024-01-01' AND '2024-08-01'




