truncate table mkn_ek_nov_2021_month_cg_samp;
--17 min
begin
      for i in (
          select distinct market_key 
          from dim_market 
          where market_key not in ('-99','ALL')
          --and market_key='VIP'
          order by market_key
       ) loop 

          insert --+ append 
          into mkn_ek_nov_2021_month_cg_samp
          /*Из абонентов КГ набираются абоненты по принципу: для каждого абонента ЦГ - по 9 из КГ. 
          Но суммарно для каждого набора KPIs - не более количества из КГ.*/

          SELECT --+ materialize no_merge
                    A.MARKET_KEY
                  , A.CLUSTER_IND
                  , A.TG_COUNT
                  , A.CG_COUNT
                  , A.RN_GROUP_CLUSTER AS TG_NUM
                  , C.SAMPLE_N
                  , B.RN_GROUP_CLUSTER AS CG_NUM
                  , B.ban_key
                  , B.subs_key
                  , B.PRICE_PLAN_KEY
                  , ROW_NUMBER() OVER ( ORDER BY 1) REC_ID

            FROM mkn_ek_nov_2021_month_GROUPS A
            CROSS JOIN (SELECT LEVEL SAMPLE_N
                        FROM DUAL
                        CONNECT BY LEVEL<10) C --каждую строку из А (каждого абонента) маркирует с 1 по 9, т.е. делается 9 выборок
            INNER JOIN mkn_ek_nov_2021_month_GROUPS B
                       On A.CLUSTER_IND=B.CLUSTER_IND --одинаковые кластеры KPIs
                       AND A.MARKET_KEY=B.MARKET_KEY
                       AND ORA_HASH(A.subs_key||C.SAMPLE_N, --хэшируемая строка
                                    A.CG_COUNT-1, --количество бакетов (=количеству абонентов КГ в заданном наборе KPIs Cluster_ind)
                                    TO_NUMBER(TO_CHAR(SYSDATE,'SS')))--ceed для генерации
                                    +1=B.RN_GROUP_CLUSTER

           WHERE 1=1
                 AND a.group_type='TG'
                 AND b.group_type='CG'
                 and a.market_key = i.market_key
                 and b.market_key = i.market_key
                 ;
          commit;
        end loop;
end;


-----------------
truncate table mkn_ek_nov_2021_month_tg_samp;
begin --14min
      for i in (
          select distinct market_key 
          from dim_market 
          where market_key not in ('-99','ALL')
          --and market_key='VIP'
          order by market_key
       ) loop 


          insert --+ append 
          into mkn_ek_nov_2021_month_tg_samp
          /**/

          SELECT --+ materialize no_merge
                 A.MARKET_KEY
               , A.CLUSTER_IND
               , B.ban_key
               , B.subs_key
               , B.PRICE_PLAN_KEY
               , C.SAMPLE_N
               , ROW_NUMBER() OVER ( ORDER BY 1) REC_ID

            FROM mkn_ek_nov_2021_month_GROUPS A
            CROSS JOIN (SELECT LEVEL SAMPLE_N
                        FROM DUAL
                        CONNECT BY LEVEL<10) C
            INNER JOIN mkn_ek_nov_2021_month_GROUPS B
                       ON A.CLUSTER_IND=B.CLUSTER_IND
                       AND A.MARKET_KEY=B.MARKET_KEY
                       AND ORA_HASH(A.subs_key||C.SAMPLE_N,A.TG_COUNT-1,TO_NUMBER(TO_CHAR(SYSDATE,'SS')))+1=B.RN_GROUP_CLUSTER
            WHERE 1=1
                  AND a.group_type='TG'
                  AND b.group_type='TG'
                  and a.market_key = i.market_key
                  and b.market_key = i.market_key
                  ;
          commit;
        end loop;
end;

--CONTROL

select group_ind, sample_n, count (subs_key) as base
from 
       (
       SELECT * FROM (
                SELECT MARKET_KEY, BAN_KEY, SUBS_KEY, PRICE_PLAN_KEY, 'CG' AS GROUP_IND, SAMPLE_N, REC_ID
                FROM mkn_ek_nov_2021_month_cg_samp
                UNION ALL
                SELECT MARKET_KEY, BAN_KEY, SUBS_KEY, PRICE_PLAN_KEY, 'TG' AS GROUP_IND, SAMPLE_N , REC_ID
                FROM mkn_ek_nov_2021_month_tg_samp
                     )
        )
group by group_ind, sample_n
order by sample_n, group_ind
;






----------Создание таблицы -----------------

create table mkn_ek_nov_2021_month_cg_samp
(
  MARKET_KEY CHAR(3),
  CLUSTER_IND VARCHAR2(256),
  TG_COUNT NUMBER,
  CG_COUNT NUMBER,
  TG_NUM NUMBER,
  SAMPLE_N NUMBER,
  CG_NUM NUMBER,
  BAN_KEY NUMBER(9),
  SUBS_KEY   VARCHAR2(10),
  PRICE_PLAN_KEY   VARCHAR2(9),
  REC_ID NUMBER
)
 compress parallel(degree 8) nologging 
PARTITION BY LIST (market_key)
--PARTITION TEMPLATE
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
------------------------------------------------------
--drop table DKH_fu_pilot_tg_samp purge;
create table mkn_ek_nov_2021_month_tg_samp
(
  MARKET_KEY CHAR(3),
  CLUSTER_IND VARCHAR2(256),
  BAN_KEY NUMBER(9),
  SUBS_KEY   VARCHAR2(10),
  PRICE_PLAN_KEY   VARCHAR2(9),
  SAMPLE_N NUMBER,
  REC_ID NUMBER
)
 compress parallel(degree 8) nologging 
PARTITION BY LIST (market_key)
--PARTITION TEMPLATE
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
