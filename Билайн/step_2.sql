----Подтягивание KPIs----- 
select * from agn_ek_nakat_month_kpis; 
drop table agn_ek_nakat_month_kpis purge;  
truncate table mkn_ek_nov_2021_kpis;

declare 
   d date := date '2021-08-01';
begin
  while (d <= date '2021-11-01') loop
    for i in (select distinct market_key from dwh.dim_market where market_key not in ('ALL','-99')/* and market_key>'SPB' */order by 1) loop
      insert --+append
      into mkn_ek_nov_2021_kpis
        select
           t1.ban_key,
           t1.subs_key,
           t1.rtc_segment_key,
           t1.market_key,
           t1.price_plan_key as pp_init_soc,       
           t1.subs_activation_date_key,
           t1.group_type,
           
           t2.time_key,
           t2.last_price_plan_key as pp_curr_soc,
           t2.total_rev,
           
         --Offnet voice--
           (t2.out_voice_offnet_local_other + t2.out_voice_offnet_local_mts +
           t2.out_voice_offnet_local_mgf + t2.out_voice_offnet_local_tl2) as out_voice_offnet_local,
           
           (t2.in_voice_offnet_local_other + t2.in_voice_offnet_local_mts +
           t2.in_voice_offnet_local_mgf + t2.in_voice_offnet_local_tl2) as in_voice_offnet_local,
           
           t2.out_voice_fix_local,
           t2.out_voice_offnet_mg,
           t2.out_voice_fix_mg,
           t2.out_voice_offnet_mn,
           t2.out_voice_fix_mn,
           t2.in_voice_fix_local,
           t2.in_voice_offnet_mg,
           t2.in_voice_fix_mg,
           t2.in_voice_offnet_mn,
           t2.in_voice_fix_mn
           
       
        from  mkn_ek_nov_2021 t1
        inner join  dwh.fct_act_clust_subs_m@dwhprd t2
              on t1.ban_key = t2.last_ban_key and t1.subs_key = t2.last_subs_key
              and t1.market_key = t2.market_key and t2.aab_eop1m > 0
              and t2.time_key = d
              and t2.market_key = i.market_key
        
        where t1.market_key = i.market_key
        ;
      commit;
    end loop;
  d := add_months(d,1);
  end loop;
end;

--control
select GROUP_TYPE, time_key, count(*),count(distinct subs_key)  from mkn_ek_nov_2021_kpis
group by GROUP_TYPE,time_key
order by 2





---------------Создание таблицы ------------------
 create table  mkn_ek_nov_2021_kpis_v2
    --drop table dkh_index2020_kpis purge;
       (  
       ban_key number(9),
       subs_key char(10),
       rtc_segment_key char(3),
       market_key char(3),
       pp_init_soc varchar2(20),        
       subs_activation_date_key date,
       group_type char(2),
           
       time_key date,
       pp_curr_soc varchar2(20),
       TOTAL_REV number,
       out_voice_offnet_local number,
       in_voice_offnet_local number,
       out_voice_fix_local number,
       out_voice_offnet_mg number,
       out_voice_fix_mg number,
       out_voice_offnet_mn number,
       out_voice_fix_mn number,
       in_voice_fix_local number,
       in_voice_offnet_mg number,
       in_voice_fix_mg number,
       in_voice_offnet_mn number,
       in_voice_fix_mn number   
          
       ) SEGMENT CREATION IMMEDIATE 
      PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255 
     COMPRESS BASIC NOLOGGING
    partition by range (time_key)

    --Для партицирования помесячно. Оптимальный вариант. В этом случае даты 
    INTERVAL(NUMTOYMINTERVAL(1, 'MONTH'))

    --Для партицирования подневно
    --INTERVAL (NUMTODSINTERVAL(1,'day'))
      
      SUBPARTITION BY LIST (market_key)
       SUBPARTITION TEMPLATE(
         SUBPARTITION P_ABK VALUES ('ABK'),
         SUBPARTITION P_ANR VALUES ('ANR'),
         SUBPARTITION P_ARH VALUES ('ARH'),
         SUBPARTITION P_AST VALUES ('AST'),
         SUBPARTITION P_BAR VALUES ('BAR'),
         SUBPARTITION P_BGK VALUES ('BGK'),
         SUBPARTITION P_BIR VALUES ('BIR'),
         SUBPARTITION P_BLG VALUES ('BLG'),
         SUBPARTITION P_BRN VALUES ('BRN'),
         SUBPARTITION P_BUR VALUES ('BUR'),
         SUBPARTITION P_CHB VALUES ('CHB'),
         SUBPARTITION P_CHL VALUES ('CHL'),
         SUBPARTITION P_CHT VALUES ('CHT'),
         SUBPARTITION P_DTI VALUES ('DTI'),
         SUBPARTITION P_EKT VALUES ('EKT'),
         SUBPARTITION P_EST VALUES ('EST'),
         SUBPARTITION P_EXT VALUES ('EXT'),
         SUBPARTITION P_GAL VALUES ('GAL'),
         SUBPARTITION P_GRZ VALUES ('GRZ'),
         SUBPARTITION P_HBR VALUES ('HBR'),
         SUBPARTITION P_HMS VALUES ('HMS'),
         SUBPARTITION P_IGK VALUES ('IGK'),
         SUBPARTITION P_IKO VALUES ('IKO'),
         SUBPARTITION P_IRK VALUES ('IRK'),
         SUBPARTITION P_IVN VALUES ('IVN'),
         SUBPARTITION P_KC1 VALUES ('KC1'),
         SUBPARTITION P_KCH VALUES ('KCH'),
         SUBPARTITION P_KIR VALUES ('KIR'),
         SUBPARTITION P_KLG VALUES ('KLG'),
         SUBPARTITION P_KMR VALUES ('KMR'),
         SUBPARTITION P_KRD VALUES ('KRD'),
         SUBPARTITION P_KRG VALUES ('KRG'),
         SUBPARTITION P_KRL VALUES ('KRL'),
         SUBPARTITION P_KRS VALUES ('KRS'),
         SUBPARTITION P_KSK VALUES ('KSK'),
         SUBPARTITION P_KSM VALUES ('KSM'),
         SUBPARTITION P_KUR VALUES ('KUR'),
         SUBPARTITION P_KZL VALUES ('KZL'),
         SUBPARTITION P_KZN VALUES ('KZN'),
         SUBPARTITION P_LPK VALUES ('LPK'),
         SUBPARTITION P_MAH VALUES ('MAH'),
         SUBPARTITION P_MGD VALUES ('MGD'),
         SUBPARTITION P_MGN VALUES ('MGN'),
         SUBPARTITION P_MUR VALUES ('MUR'),
         SUBPARTITION P_NA1 VALUES ('NA1'),
         SUBPARTITION P_NAL VALUES ('NAL'),
         SUBPARTITION P_NNG VALUES ('NNG'),
         SUBPARTITION P_NOR VALUES ('NOR'),
         SUBPARTITION P_NSK VALUES ('NSK'),
         SUBPARTITION P_NTG VALUES ('NTG'),
         SUBPARTITION P_NZR VALUES ('NZR'),
         SUBPARTITION P_OMS VALUES ('OMS'),
         SUBPARTITION P_ORB VALUES ('ORB'),
         SUBPARTITION P_ORL VALUES ('ORL'),
         SUBPARTITION P_PNZ VALUES ('PNZ'),
         SUBPARTITION P_PPK VALUES ('PPK'),
         SUBPARTITION P_PRM VALUES ('PRM'),
         SUBPARTITION P_PSK VALUES ('PSK'),
         SUBPARTITION P_RND VALUES ('RND'),
         SUBPARTITION P_RZN VALUES ('RZN'),
         SUBPARTITION P_SAM VALUES ('SAM'),
         SUBPARTITION P_SCH VALUES ('SCH'),
         SUBPARTITION P_SHL VALUES ('SHL'),
         SUBPARTITION P_SKH VALUES ('SKH'),
         SUBPARTITION P_SMF VALUES ('SMF'),
         SUBPARTITION P_SML VALUES ('SML'),
         SUBPARTITION P_SPB VALUES ('SPB'),
         SUBPARTITION P_SRN VALUES ('SRN'),
         SUBPARTITION P_SRT VALUES ('SRT'),
         SUBPARTITION P_STK VALUES ('STK'),
         SUBPARTITION P_STR VALUES ('STR'),
         SUBPARTITION P_STV VALUES ('STV'),
         SUBPARTITION P_TMB VALUES ('TMB'),
         SUBPARTITION P_TMS VALUES ('TMS'),
         SUBPARTITION P_TOL VALUES ('TOL'),
         SUBPARTITION P_TUL VALUES ('TUL'),
         SUBPARTITION P_TUM VALUES ('TUM'),
         SUBPARTITION P_TVR VALUES ('TVR'),
         SUBPARTITION P_UFA VALUES ('UFA'),
         SUBPARTITION P_ULN VALUES ('ULN'),
         SUBPARTITION P_USH VALUES ('USH'),
         SUBPARTITION P_VIP VALUES ('VIP'),
         SUBPARTITION P_VLA VALUES ('VLA'),
         SUBPARTITION P_VLD VALUES ('VLD'),
         SUBPARTITION P_VLG VALUES ('VLG'),
         SUBPARTITION P_VLK VALUES ('VLK'),
         SUBPARTITION P_VNG VALUES ('VNG'),
         SUBPARTITION P_VOL VALUES ('VOL'),
         SUBPARTITION P_VRN VALUES ('VRN'),
         SUBPARTITION P_YAK VALUES ('YAK'),
         SUBPARTITION P_YAM VALUES ('YAM'),
         SUBPARTITION P_YRL VALUES ('YRL'),
         SUBPARTITION P_DEF VALUES (default))
    (
    PARTITION p_first VALUES LESS THAN (to_date('01/01/2019', 'dd/mm/yyyy'))
    );

