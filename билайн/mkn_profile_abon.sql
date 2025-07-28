declare d date;

begin
     d := to_date('01.12.2020','dd.mm.yyyy'); 
   while (d <= to_date('01.12.2020','dd.mm.yyyy')) loop 
      for i in 
      (select market_key from dwh.dim_market where market_key not in ('-99','ALL') and market_key>'VIP' order by 1)
      loop
      insert into mkn_profile_abon

select
d,
t1.market_key,
case when segment_key in('N01','N02','N03','N04','N05','N06','N07','N08','N09','N10','N11','N12','AGN','EXS') then 'B2C' 
  else 'B2B' end segment_key,
t1.account_type_key,
case when t2.rtc_active_ind>0
        and t2.account_type_key <> 341
        and t2.dw_status_key in ('A','S')
        and c.activity_type_key not in ('9','12','19') then 1 else 0 end paid_ind, 
bay.line_ind,
bay.type_vse_old,
 case when OS_Type not in ('Android','iOS') then 'other' else OS_Type end OS_Type,  
t3.soc_effective_date,
t3.soc_expiration_date,
soc_code,   

 case
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=2  then 'LT 01-03' --0,1,2
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=5  then 'LT 04-06' --3,4,5
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=11  then 'LT 07-12' --6,7,8,9,10,11
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=23  then 'LT 13-24'
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=35  then 'LT 25-36'
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=47  then 'LT 37-48'
                else 'LT 49+' 
end lt_group,
                



sum(t1.total_rev),
      
      sum(t1.out_voice_fix_local),
      sum(t1.out_voice_fix_mg),
      sum(t1.out_voice_fix_mn),
                 
      sum(t1.out_voice_offnet_local_other+t1.out_voice_offnet_local_mts+t1.out_voice_offnet_local_mgf+t1.out_voice_offnet_local_tl2),
      sum(t1.out_voice_offnet_mg),
      sum(t1.out_voice_offnet_mn),
                 
      sum((t1.out_voice_h_local + t1.out_voice_r_local)/60 - 
      (t1.out_voice_offnet_local_other+t1.out_voice_offnet_local_mts+t1.out_voice_offnet_local_mgf+t1.out_voice_offnet_local_tl2) - 
      t1.out_voice_fix_local),          
      sum((t1.out_voice_h_mg + t1.out_voice_r_mg)/60 -
      t1.out_voice_offnet_mg - 
      t1.out_voice_fix_mg),          
      sum((t1.out_voice_h_mn + t1.out_voice_r_mn)/60 -
      t1.out_voice_offnet_mn - 
      t1.out_voice_fix_mn),
      
      sum(t1.in_voice_fix_local),
      sum(t1.in_voice_fix_mg),
      sum(t1.in_voice_fix_mn),
                 
      sum(t1.in_voice_offnet_local_other+t1.in_voice_offnet_local_mts+t1.in_voice_offnet_local_mgf+t1.in_voice_offnet_local_tl2),
      sum(t1.in_voice_offnet_mg),
      sum(t1.in_voice_offnet_mn),
                 
      sum((t1.in_voice_h_local + t1.in_voice_r_local)/60 - 
      (t1.in_voice_offnet_local_other+t1.in_voice_offnet_local_mts+t1.in_voice_offnet_local_mgf+t1.in_voice_offnet_local_tl2) - 
      t1.in_voice_fix_local),          
      sum((t1.in_voice_h_mg + t1.in_voice_r_mg)/60 -
      t1.in_voice_offnet_mg - 
      t1.in_voice_fix_mg),          
      sum((t1.in_voice_h_mn + t1.in_voice_r_mn)/60 -
      t1.in_voice_offnet_mn - 
      t1.in_voice_fix_mn),
      
      sum(data_mb),
      sum(data_4g_mb),
      count(distinct t1.last_ban_key||t1.last_subs_key)
         
        
 from (select * from  DWH.FCT_ACT_CLUST_SUBS_M@DWHPRD 
 where time_key=d and market_key=i.market_key and   aab_eop1m > 0) t1
 
  left join (select * from dwh.fct_rtc_monthly@dwhprd where time_key = d and market_key = i.market_key 
      and rtc_active_ind > 0) t2
  on t1.last_subs_key = t2.subs_key and t1.last_ban_key = t2.ban_key  
 
   left join     DWH.STG_AAB_ACTIVITY_TYPE@DWHPRD c
  on c.ACTIVITY_TYPE_MASK = t2.ACTIVITY_TYPE_MASK  
  
   left join bay_vse_info bay
   on t1.last_price_plan_key = bay.price_plan_key
  

  
  inner join (select * from SEL_NY_CAMPAIGN_SOC
  where trunc(soc_effective_date,'month')=d) t3
  
  on   t1.last_subs_key = t3.subs_key and t1.last_ban_key = t3.ban_key
  
  group by 
  d,
t1.market_key,
case when segment_key in('N01','N02','N03','N04','N05','N06','N07','N08','N09','N10','N11','N12','AGN','EXS') then 'B2C' 
  else 'B2B' end,
t1.account_type_key,
case when t2.rtc_active_ind>0
        and t2.account_type_key <> 341
        and t2.dw_status_key in ('A','S')
        and c.activity_type_key not in ('9','12','19') then 1 else 0 end, 
bay.line_ind,
bay.type_vse_old,
 case when OS_Type not in ('Android','iOS') then 'other' else OS_Type end,  
t3.soc_effective_date,
t3.soc_expiration_date,
soc_code,   

 case
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=2  then 'LT 01-03' --0,1,2
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=5  then 'LT 04-06' --3,4,5
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=11  then 'LT 07-12' --6,7,8,9,10,11
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=23  then 'LT 13-24'
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=35  then 'LT 25-36'
                when months_between (t1.TIME_KEY, nvl(t1.first_month_aab,t1.init_activation_date))<=47  then 'LT 37-48'
                else 'LT 49+' 
end;
  
      commit;
insert into MKN_LOG values(sysdate,'mkn_profile_abon', d||i.market_key);
commit;
      end loop;
   d:=add_months(d, 1);
   end loop;

end;    
  

  
  
------------------------Создание Таблицы------------------------------ 
  
truncate table mkn_profile_abon
/*drop table mkn_internet_pro purge;*/
create table mkn_profile_abon
           (        
      
      
      time_key date,
      market_key varchar2(3),
      segment_key varchar2(3),
      account_type_key number,
      paid_ind number,
      line_ind varchar2(256),
      type_vse_old number,
      OS_Type varchar2(256),
      soc_effective_date date,
      soc_expiration_date date,
      soc_code varchar2(256),
      lt_group varchar2(256),
                
      
      total_rev number,
      out_voice_fix_local number,
      out_voice_fix_mg number,
      out_voice_fix_mn number,
      out_voice_offnet_local number,
      out_voice_offnet_mg number,
      out_voice_offnet_mn number,
      out_voice_onnet_local number,
      out_voice_onnet_mg number,
      out_voice_onnet_mn number,
      in_voice_fix_local number,
      in_voice_fix_mg number,
      in_voice_fix_mn number,
      in_voice_offnet_local number,
      in_voice_offnet_mg number,
      in_voice_offnet_mn number,
      in_voice_onnet_local number,
      in_voice_onnet_mg number,
      in_voice_onnet_mn number,
      data_mb number,
      data_4g_mb number,
      subs number 
      
      
      ) SEGMENT CREATION IMMEDIATE 
          PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255 
         COMPRESS BASIC NOLOGGING
        partition by range (time_key) --Отчетная дата!
         

        INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
          
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
        PARTITION p_first VALUES LESS THAN (to_date('01/12/2019', 'dd/mm/yyyy'))
        );

 
