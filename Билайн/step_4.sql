truncate table mkn_ek_nov_2021_month_GROUPS;
declare


   arpu_1m_075 number;   
   arpu_1m_050 number;  
   arpu_1m_025 number;
   

     
   voice_in_traf_1m_075 number;   
   voice_in_traf_1m_050 number;  
   voice_in_traf_1m_025 number;


   
   
begin
   
   select percentile_disc(0.75) within group(order by ARPU_1M) into arpu_1m_075 from mkn_ek_nov_2021_kpis_agg   where ARPU_1M>0;   
   select percentile_disc(0.50) within group(order by ARPU_1M) into arpu_1m_050 from mkn_ek_nov_2021_kpis_agg   where ARPU_1M>0;  
   select percentile_disc(0.25) within group(order by ARPU_1M) into arpu_1m_025 from mkn_ek_nov_2021_kpis_agg   where ARPU_1M>0;


    
   select percentile_disc(0.75) within group(order by voice_in_traf_1m) into voice_in_traf_1m_075 from mkn_ek_nov_2021_kpis_agg   where voice_in_traf_1m>0;   
   select percentile_disc(0.50) within group(order by voice_in_traf_1m) into voice_in_traf_1m_050 from mkn_ek_nov_2021_kpis_agg   where voice_in_traf_1m>0;  
   select percentile_disc(0.25) within group(order by voice_in_traf_1m) into voice_in_traf_1m_025 from mkn_ek_nov_2021_kpis_agg    where voice_in_traf_1m>0;

   ------          
    for i in 
      (select distinct market_key from dim_market where market_key not in ('-99','ALL')  order by market_key) loop 
         

          insert --+ append 
          into mkn_ek_nov_2021_month_GROUPS
          /**/


          select --+ materialize no_merge
                    market_key
                  , cluster_ind
                  , subs_key
                  , ban_key --вс€ прокластеризованна€ јјЅ с ручным фильтром-ограничителем (к которой привешиваетс€ индекс: 
                            --÷√ - проверка по списку номеров,  √ - по остаточному принципу)
                  , price_plan_key
                  , group_ind
                  , ROW_NUMBER() OVER (PARTITION BY group_ind, CLUSTER_IND ORDER BY ORA_HASH(subs_key)) RN_GROUP_CLUSTER 
                                 --нумеруем строки (абонентов) в каждом наборе групп KPIs
                  , subs_cg  CG_COUNT --количество абонентов  √ в текущем наборе KPIs(в cluster_ind)
                  , subs_tg  TG_COUNT --количество абонентов ÷√ в текущем наборе KPIs(в cluster_ind)

          from 
              (select
                 

                  paid_ind_1m || out_traf_ind_1m ||  --јктивность в 1  мес€це
                    case --LT
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=2  then 'm01-03' 
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=11  then 'm04-12'                                                                                  
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=23  then 'y1-2'
                        else 'y2+'
                          end ||
                         case 
                           when ARPU_1M<=0 then 0
                           when ARPU_1M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_1M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_1M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                         end  ||--ARPU в 1 мес€це
                        /* case 
                           when voice_out_traf_1m < voice_out_traf_m1_025 then ceil(voice_out_traf_1m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_1m < voice_out_traf_m1_050 then ceil(voice_out_traf_1m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_1m < voice_out_traf_m1_075 then ceil(voice_out_traf_1m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_1m < voice_out_traf_m1_097 then ceil(voice_out_traf_1m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 1 неделе*/
                         case 
                           when voice_in_traf_1m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_1m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_1m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end  ||--voice_in_traf в 1 мес€це
                  paid_ind_2m || out_traf_ind_2m ||  --јктивность в 2 мес€це
                         case 
                           when ARPU_2M<=0 then 0
                           when ARPU_2M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_2M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_2M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                             end||
                        /* case 
                           when voice_out_traf_2m < voice_out_traf_m1_025 then ceil(voice_out_traf_2m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_2m < voice_out_traf_m1_050 then ceil(voice_out_traf_2m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_2m < voice_out_traf_m1_075 then ceil(voice_out_traf_2m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_2m < voice_out_traf_m1_097 then ceil(voice_out_traf_2m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 2 неделе*/
                         case 
                           when voice_in_traf_2m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_2m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_2m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end  ||--voice_in_traf в 2 мес€це
                   paid_ind_3m || out_traf_ind_3m ||  --јктивность в 3 мес€це
                         case 
                           when ARPU_3M<=0 then 0
                           when ARPU_3M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_3M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_3M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                         end  ||--ARPU в 3 мес€це
                         /* case 
                           when voice_out_traf_3m < voice_out_traf_m1_025 then ceil(voice_out_traf_3m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_3m < voice_out_traf_m1_050 then ceil(voice_out_traf_3m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_3m < voice_out_traf_m1_075 then ceil(voice_out_traf_3m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_3m < voice_out_traf_m1_097 then ceil(voice_out_traf_3m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 3 неделе*/
                          case 
                           when voice_in_traf_3m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_3m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_3m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end--voice_in_traf в 3 мес€це                            
                           
                          
                as cluster_ind
                
              , a.subs_key as subs_key
              , a.ban_key as ban_key
              , a.market_key
              , a.pp_init_soc as price_plan_key
              , decode(b.subs_key,null,'CG','TG') group_ind
              
              , sum(decode(b.subs_key,null,1,0)) over (partition by
                 paid_ind_1m || out_traf_ind_1m ||  --јктивность в 1  мес€це
                  paid_ind_1m || out_traf_ind_1m ||  --јктивность в 1  мес€це
                    case --LT
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=2  then 'm01-03' 
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=11  then 'm04-12'                                                                                  
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=23  then 'y1-2'
                        else 'y2+'
                          end ||
                         case 
                           when ARPU_1M<=0 then 0
                           when ARPU_1M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_1M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_1M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                         end  ||--ARPU в 1 мес€це
                        /* case 
                           when voice_out_traf_1m < voice_out_traf_m1_025 then ceil(voice_out_traf_1m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_1m < voice_out_traf_m1_050 then ceil(voice_out_traf_1m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_1m < voice_out_traf_m1_075 then ceil(voice_out_traf_1m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_1m < voice_out_traf_m1_097 then ceil(voice_out_traf_1m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 1 неделе*/
                         case 
                           when voice_in_traf_1m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_1m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_1m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end  ||--voice_in_traf в 1 мес€це
                  paid_ind_2m || out_traf_ind_2m ||  --јктивность в 2 мес€це
                         case 
                           when ARPU_2M<=0 then 0
                           when ARPU_2M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_2M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_2M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                             end||
                        /* case 
                           when voice_out_traf_2m < voice_out_traf_m1_025 then ceil(voice_out_traf_2m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_2m < voice_out_traf_m1_050 then ceil(voice_out_traf_2m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_2m < voice_out_traf_m1_075 then ceil(voice_out_traf_2m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_2m < voice_out_traf_m1_097 then ceil(voice_out_traf_2m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 2 неделе*/
                         case 
                           when voice_in_traf_2m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_2m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_2m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end  ||--voice_in_traf в 2 мес€це
                   paid_ind_3m || out_traf_ind_3m ||  --јктивность в 3 мес€це
                         case 
                           when ARPU_3M<=0 then 0
                           when ARPU_3M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_3M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_3M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                         end  ||--ARPU в 3 мес€це
                         /* case 
                           when voice_out_traf_3m < voice_out_traf_m1_025 then ceil(voice_out_traf_3m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_3m < voice_out_traf_m1_050 then ceil(voice_out_traf_3m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_3m < voice_out_traf_m1_075 then ceil(voice_out_traf_3m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_3m < voice_out_traf_m1_097 then ceil(voice_out_traf_3m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 3 неделе*/
                          case 
                           when voice_in_traf_3m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_3m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_3m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end--voice_in_traf в 3 мес€це                            
                           
                           
                  ) subs_cg
              
              , sum(decode(b.subs_key,null,0,1)) over (partition by 
              paid_ind_1m || out_traf_ind_1m ||  --јктивность в 1  мес€це
                    case --LT
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=2  then 'm01-03' 
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=11  then 'm04-12'                                                                                  
                        when months_between (date'2021-10-01', trunc(subs_activation_date_key,'mm')) <=23  then 'y1-2'
                        else 'y2+'
                          end ||
                         case 
                           when ARPU_1M<=0 then 0
                           when ARPU_1M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_1M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_1M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                         end  ||--ARPU в 1 мес€це
                        /* case 
                           when voice_out_traf_1m < voice_out_traf_m1_025 then ceil(voice_out_traf_1m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_1m < voice_out_traf_m1_050 then ceil(voice_out_traf_1m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_1m < voice_out_traf_m1_075 then ceil(voice_out_traf_1m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_1m < voice_out_traf_m1_097 then ceil(voice_out_traf_1m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 1 неделе*/
                         case 
                           when voice_in_traf_1m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_1m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_1m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end  ||--voice_in_traf в 1 мес€це
                  paid_ind_2m || out_traf_ind_2m ||  --јктивность в 2 мес€це
                         case 
                           when ARPU_2M<=0 then 0
                           when ARPU_2M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_2M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_2M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                             end||
                        /* case 
                           when voice_out_traf_2m < voice_out_traf_m1_025 then ceil(voice_out_traf_2m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_2m < voice_out_traf_m1_050 then ceil(voice_out_traf_2m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_2m < voice_out_traf_m1_075 then ceil(voice_out_traf_2m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_2m < voice_out_traf_m1_097 then ceil(voice_out_traf_2m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 2 неделе*/
                         case 
                           when voice_in_traf_2m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_2m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_2m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end  ||--voice_in_traf в 2 мес€це
                   paid_ind_3m || out_traf_ind_3m ||  --јктивность в 3 мес€це
                         case 
                           when ARPU_3M<=0 then 0
                           when ARPU_3M < ARPU_1M_025 then ARPU_1M_025 
                           when ARPU_3M < ARPU_1M_050 then ARPU_1M_050
                           when ARPU_3M < ARPU_1M_075 then ARPU_1M_075
                           else ARPU_1M_075+1
                         end  ||--ARPU в 3 мес€це
                         /* case 
                           when voice_out_traf_3m < voice_out_traf_m1_025 then ceil(voice_out_traf_3m/voice_out_traf_n1)*voice_out_traf_n1
                           when voice_out_traf_3m < voice_out_traf_m1_050 then ceil(voice_out_traf_3m/voice_out_traf_n2)*voice_out_traf_n2
                           when voice_out_traf_3m < voice_out_traf_m1_075 then ceil(voice_out_traf_3m/voice_out_traf_n3)*voice_out_traf_n3
                           when voice_out_traf_3m < voice_out_traf_m1_097 then ceil(voice_out_traf_3m/voice_out_traf_n4)*voice_out_traf_n4
                           else voice_out_traf_m1_097
                         end  ||--voice_out_traf в 3 неделе*/
                          case 
                           when voice_in_traf_3m < voice_in_traf_1m_025 then voice_in_traf_1m_025
                           when voice_in_traf_3m < voice_in_traf_1m_050 then voice_in_traf_1m_050
                           when voice_in_traf_3m < voice_in_traf_1m_075 then voice_in_traf_1m_075
                           else voice_in_traf_1m_075+1
                         end--voice_in_traf в 3 мес€це                            
                                       
                           
                ) subs_tg
 
                        from
                    --- “абличка с кластерами ---
                         mkn_ek_nov_2021_kpis_agg a     
                         
                    --- “абличка с абонентами, которые ÷√ ----------------------------------
                           left join 
                           (
                             select distinct subs_key as subs_key, ban_key as ban_key
                             from  mkn_ek_nov_2021_kpis_agg  a
                             where   group_type = 'TG'
                             and market_key = i.market_key
                             --and a.pp_init_soc in (select price_plan_key from bay_vse_info t where t.charge_type='month') 
                           ) b 
                           on a.subs_key=b.subs_key and a.ban_key = b.ban_key
                    ------------------------------------------------------------------------------------           
                           --left join bay_vse_info c on a.pp_init_soc=c.price_plan_key
                      where 1=1
                      and a.market_key = i.market_key
                      and VOICE_IN_TRAF_1M>0 and VOICE_IN_TRAF_2M>0 and VOICE_IN_TRAF_3M>0
                      and  PAID_IND_1M+PAID_IND_2M+PAID_IND_3M=3 
                       
                     
             )

          where 1=1
          and market_key = i.market_key
               and subs_cg>=3 and subs_tg>=3; 

          commit;
    end loop;
end;

--CONTROL
/*select * from agn_ek_nakat_month_kpis_agg;
alter table agn_ek_nakat_month_kpis_agg rename column OUT_TRAF_IND_1M TO offnet_traf_ind_1m;
alter table agn_ek_nakat_month_kpis_agg rename column OUT_TRAF_IND_2M TO offnet_traf_ind_2m;
alter table agn_ek_nakat_month_kpis_agg rename column OUT_TRAF_IND_3M TO offnet_traf_ind_3m;*/


select a.group_type, count (*) as base, count (distinct a.market_key) as markets
 from mkn_ek_nov_2021_month_GROUPS a
where 1=1
group by  a.group_type
order by 
a.group_type;




-------------------создание таблицы --------------------------------
create table mkn_ek_nov_2021_month_GROUPS_v
(
  market_key       VARCHAR2(3),
  cluster_ind      VARCHAR2(256),
  subs_key         VARCHAR2(10),
  ban_key          NUMBER(9),
  price_plan_key   VARCHAR2(9),
  group_type       VARCHAR2(2),
  rn_group_cluster NUMBER,
  cg_count         NUMBER,
  tg_count         NUMBER
)
 compress parallel(degree 8) nologging 
PARTITION BY LIST (market_key)
---PARTITION TEMPLATE
(
PARTITION P_ABK VALUES ('ABK'),
PARTITION P_ANR VALUES ('ANR'),
PARTITION P_ARH VALUES ('ARH'),
PARTITION P_AST VALUES ('AST'),
PARTITION P_BAR VALUES ('BAR'),
PARTITION P_BGK VALUES ('BGK'),
PARTITION P_BIR VALUES ('BIR'),
PARTITION P_BLG VALUES ('BLG'),
PARTITION P_BRN VALUES ('BRN'),
PARTITION P_BUR VALUES ('BUR'),
PARTITION P_CHB VALUES ('CHB'),
PARTITION P_CHL VALUES ('CHL'),
PARTITION P_CHT VALUES ('CHT'),
PARTITION P_DTI VALUES ('DTI'),
PARTITION P_EKT VALUES ('EKT'),
PARTITION P_EST VALUES ('EST'),
PARTITION P_EXT VALUES ('EXT'),
PARTITION P_GAL VALUES ('GAL'),
PARTITION P_GRZ VALUES ('GRZ'),
PARTITION P_HBR VALUES ('HBR'),
PARTITION P_HMS VALUES ('HMS'),
PARTITION P_IGK VALUES ('IGK'),
PARTITION P_IKO VALUES ('IKO'),
PARTITION P_IRK VALUES ('IRK'),
PARTITION P_IVN VALUES ('IVN'),
PARTITION P_KC1 VALUES ('KC1'),
PARTITION P_KCH VALUES ('KCH'),
PARTITION P_KIR VALUES ('KIR'),
PARTITION P_KLG VALUES ('KLG'),
PARTITION P_KMR VALUES ('KMR'),
PARTITION P_KRD VALUES ('KRD'),
PARTITION P_KRG VALUES ('KRG'),
PARTITION P_KRL VALUES ('KRL'),
PARTITION P_KRS VALUES ('KRS'),
PARTITION P_KSK VALUES ('KSK'),
PARTITION P_KSM VALUES ('KSM'),
PARTITION P_KUR VALUES ('KUR'),
PARTITION P_KZL VALUES ('KZL'),
PARTITION P_KZN VALUES ('KZN'),
PARTITION P_LPK VALUES ('LPK'),
PARTITION P_MAH VALUES ('MAH'),
PARTITION P_MGD VALUES ('MGD'),
PARTITION P_MGN VALUES ('MGN'),
PARTITION P_MUR VALUES ('MUR'),
PARTITION P_NA1 VALUES ('NA1'),
PARTITION P_NAL VALUES ('NAL'),
PARTITION P_NNG VALUES ('NNG'),
PARTITION P_NOR VALUES ('NOR'),
PARTITION P_NSK VALUES ('NSK'),
PARTITION P_NTG VALUES ('NTG'),
PARTITION P_NZR VALUES ('NZR'),
PARTITION P_OMS VALUES ('OMS'),
PARTITION P_ORB VALUES ('ORB'),
PARTITION P_ORL VALUES ('ORL'),
PARTITION P_PNZ VALUES ('PNZ'),
PARTITION P_PPK VALUES ('PPK'),
PARTITION P_PRM VALUES ('PRM'),
PARTITION P_PSK VALUES ('PSK'),
PARTITION P_RND VALUES ('RND'),
PARTITION P_RZN VALUES ('RZN'),
PARTITION P_SAM VALUES ('SAM'),
PARTITION P_SCH VALUES ('SCH'),
PARTITION P_SHL VALUES ('SHL'),
PARTITION P_SKH VALUES ('SKH'),
PARTITION P_SMF VALUES ('SMF'),
PARTITION P_SML VALUES ('SML'),
PARTITION P_SPB VALUES ('SPB'),
PARTITION P_SRN VALUES ('SRN'),
PARTITION P_SRT VALUES ('SRT'),
PARTITION P_STK VALUES ('STK'),
PARTITION P_STR VALUES ('STR'),
PARTITION P_STV VALUES ('STV'),
PARTITION P_TMB VALUES ('TMB'),
PARTITION P_TMS VALUES ('TMS'),
PARTITION P_TOL VALUES ('TOL'),
PARTITION P_TUL VALUES ('TUL'),
PARTITION P_TUM VALUES ('TUM'),
PARTITION P_TVR VALUES ('TVR'),
PARTITION P_UFA VALUES ('UFA'),
PARTITION P_ULN VALUES ('ULN'),
PARTITION P_USH VALUES ('USH'),
PARTITION P_VIP VALUES ('VIP'),
PARTITION P_VLA VALUES ('VLA'),
PARTITION P_VLD VALUES ('VLD'),
PARTITION P_VLG VALUES ('VLG'),
PARTITION P_VLK VALUES ('VLK'),
PARTITION P_VNG VALUES ('VNG'),
PARTITION P_VOL VALUES ('VOL'),
PARTITION P_VRN VALUES ('VRN'),
PARTITION P_YAK VALUES ('YAK'),
PARTITION P_YAM VALUES ('YAM'),
PARTITION P_YRL VALUES ('YRL'),
PARTITION P_DEF VALUES (default)
)
;

