
--------Конверсия в триал ---------------
SELECT 
date_trunc('quarter',reg_date) AS date,

uniq(profile_id) AS cnt_all_reg,
uniq(CASE WHEN reg_source='none' THEN profile_id END) AS cnt_organic_reg,

uniq(CASE WHEN created_at::date>=reg_date THEN profile_id END) AS cnt_all_trial,
uniq(CASE WHEN created_at::date>=reg_date AND reg_source='none' THEN profile_id END) AS cnt_organic_trial,
uniq(CASE WHEN created_at::date>=reg_date AND reg_source='none' AND free_days IN ('3','14') THEN profile_id END) AS cnt_organic_trial_general_offer,

cnt_all_trial/cnt_all_reg AS cr_trail_all,
cnt_organic_trial/cnt_organic_reg AS cr_trial_organic,
cnt_organic_trial_general_offer/cnt_organic_reg AS cr_trial_organic_general_offer

FROM datamarts.marketing_dash
WHERE  date IN ('2024-01-01','2024-04-01','2024-07-01','2024-10-01','2025-01-01')
AND promo_type!='cards'
GROUP BY 1



-----Конверсия в подписку-----------

SELECT 
date_trunc('quarter',created_at::date) AS date,

uniq(profile_id) AS cnt_all_trial,
uniq(CASE WHEN reg_source='none' THEN profile_id END) AS cnt_organic_trial,
uniq(CASE WHEN reg_source='none' AND free_days IN ('3','14') THEN profile_id END) AS cnt_organic_trial_general_offer,


uniq(CASE WHEN first_prolong_date::date>=created_at::date THEN profile_id END) AS cnt_all_subs,
uniq(CASE WHEN first_prolong_date::date>=created_at::date AND reg_source='none' THEN profile_id END) AS cnt_organic_subs,
uniq(CASE WHEN first_prolong_date::date>=created_at::date AND reg_source='none' AND free_days IN ('3','14') THEN profile_id END) AS cnt_organic_subs_general_offer,

cnt_all_subs/cnt_all_trial AS cr_subs_all,
cnt_organic_subs/cnt_organic_trial AS cr_subs_organic,
cnt_organic_subs_general_offer/cnt_organic_trial_general_offer AS cr_trial_organic_general_offer

FROM datamarts.marketing_dash
WHERE  date IN ('2024-01-01','2024-04-01','2024-07-01','2024-10-01','2025-01-01')
AND promo_type!='cards'
GROUP BY 1



SELECT reg_date,user_id,created_at::date,first_prolong_date::date,free_days,created_at::date+free_days AS ch, ch=first_prolong_date
FROM datamarts.marketing_dash
WHERE created_at>='2024-08-01' AND first_prolong_date!='1970-01-01'
