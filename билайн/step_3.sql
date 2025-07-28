----Агрегация KPIs-----  
select * from mkn_ek_nov_2021_kpis_agg;
truncate table mkn_ek_nov_2021_kpis_agg;
begin
  for i in (select distinct market_key from dwh.dim_market where market_key not in ('ALL','-99') /*and market_key>'ULN'*/ order by 1) loop
    insert --+append
    into mkn_ek_nov_2021_kpis_agg
      select 
          t1.ban_key,
          t1.subs_key,
          t1.rtc_segment_key,
          t1.market_key,
          t1.pp_init_soc,       
          t1.subs_activation_date_key,
          t1.group_type,
          
          /* -1 month */ 
          case 
            when t2.total_rev> 0 then 1 
            else 0 
          end paid_ind_1m,
          case 
            when t2.out_voice_offnet_local+
                 t2.out_voice_fix_local+
                 t2.out_voice_offnet_mg+
                 t2.out_voice_fix_mg+
                 t2.out_voice_offnet_mn+
                 t2.out_voice_fix_mn+
                 t2.in_voice_offnet_local+
                 t2.in_voice_fix_local+
                 t2.in_voice_offnet_mg+
                 t2.in_voice_fix_mg+
                 t2.in_voice_offnet_mn+
                 t2.in_voice_fix_mn >0
                 then 1 
            else 0 
          end out_traf_ind_1m,        
          t2.total_rev as ARPU_1m,
          nvl(t2.out_voice_offnet_local+
                 t2.out_voice_fix_local+
                 t2.out_voice_offnet_mg+
                 t2.out_voice_fix_mg+
                 t2.out_voice_offnet_mn+
                 t2.out_voice_fix_mn,0) as voice_out_traf_1m,
          
          nvl(t2.in_voice_offnet_local+
                 t2.in_voice_fix_local+
                 t2.in_voice_offnet_mg+
                 t2.in_voice_fix_mg+
                 t2.in_voice_offnet_mn+
                 t2.in_voice_fix_mn,0) as voice_in_traf_1m,
          
          
          
          /* -2 month */
           case 
            when  t3.total_rev> 0 then 1 
            else 0 
          end paid_ind_2m,
          case 
            when  t3.out_voice_offnet_local+
                  t3.out_voice_fix_local+
                  t3.out_voice_offnet_mg+
                  t3.out_voice_fix_mg+
                  t3.out_voice_offnet_mn+
                  t3.out_voice_fix_mn+
                  t3.in_voice_offnet_local+
                  t3.in_voice_fix_local+
                  t3.in_voice_offnet_mg+
                  t3.in_voice_fix_mg+
                  t3.in_voice_offnet_mn+
                  t3.in_voice_fix_mn >0
                 then 1 
            else 0 
          end out_traf_ind_1m,        
           t3.total_rev as ARPU_2m,
          nvl( t3.out_voice_offnet_local+
                  t3.out_voice_fix_local+
                  t3.out_voice_offnet_mg+
                  t3.out_voice_fix_mg+
                  t3.out_voice_offnet_mn+
                  t3.out_voice_fix_mn,0) as voice_out_traf_2m,
          
          nvl( t3.in_voice_offnet_local+
                  t3.in_voice_fix_local+
                  t3.in_voice_offnet_mg+
                  t3.in_voice_fix_mg+
                  t3.in_voice_offnet_mn+
                  t3.in_voice_fix_mn,0) as voice_in_traf_2m,
          
          /* -3 month */
          case 
            when  t4.total_rev> 0 then 1 
            else 0 
          end paid_ind_3m,
          case 
            when  t4.out_voice_offnet_local+
                  t4.out_voice_fix_local+
                  t4.out_voice_offnet_mg+
                  t4.out_voice_fix_mg+
                  t4.out_voice_offnet_mn+
                  t4.out_voice_fix_mn+
                  t4.in_voice_offnet_local+
                  t4.in_voice_fix_local+
                  t4.in_voice_offnet_mg+
                  t4.in_voice_fix_mg+
                  t4.in_voice_offnet_mn+
                  t4.in_voice_fix_mn >0
                 then 1 
            else 0 
          end out_traf_ind_1m,        
           t4.total_rev as ARPU_3m,
          nvl( t4.out_voice_offnet_local+
                  t4.out_voice_fix_local+
                  t4.out_voice_offnet_mg+
                  t4.out_voice_fix_mg+
                  t4.out_voice_offnet_mn+
                  t4.out_voice_fix_mn,0) as voice_out_traf_3m,
          
          nvl( t4.in_voice_offnet_local+
                  t4.in_voice_fix_local+
                  t4.in_voice_offnet_mg+
                  t4.in_voice_fix_mg+
                  t4.in_voice_offnet_mn+
                  t4.in_voice_fix_mn,0) as voice_in_traf_3m
          
      
      from mkn_ek_nov_2021_kpis t1
      
      left join mkn_ek_nov_2021_kpis t2
           on t1.ban_key = t2.ban_key and t1.subs_key = t2.subs_key and t1.market_key = t2.market_key
           and t2.market_key = i.market_key
           and t2.time_key = add_months(t1.time_key,-1)
      left join mkn_ek_nov_2021_kpis t3
           on t1.ban_key = t3.ban_key and t1.subs_key = t3.subs_key and t1.market_key = t3.market_key
           and t3.market_key = i.market_key
           and t3.time_key = add_months(t1.time_key,-2)
      left join mkn_ek_nov_2021_kpis t4
           on t1.ban_key = t4.ban_key and t1.subs_key = t4.subs_key and t1.market_key = t4.market_key
           and t4.market_key = i.market_key
           and t4.time_key = add_months(t1.time_key,-3)     
      
           
      where t1.time_key = date '2021-11-01'
      and   t1.market_key = i.market_key  
      ;
    commit;
  end loop;
end;


-------------check
  select
        group_type,  sum(1), count(distinct ban_key||subs_key)
        from mkn_ek_nov_2021_kpis_agg
        group by group_type 
        order by 1 ;










    create table mkn_ek_nov_2021_kpis_agg_v2
    (
      ban_key number(9),
      subs_key char(10),
      rtc_segment_key char(3),
      market_key char(3),
      pp_init_soc varchar2(20),        
      subs_activation_date_key date,
      group_type char(2),
                  
      /* -1 week */ 
      paid_ind_1m number(1),
      out_traf_ind_1m number(1),        
      arpu_1m number,
      voice_out_traf_1m number,
      voice_in_traf_1m number,
                  
      /* -2 week */
      paid_ind_2m number(1),
      out_traf_ind_2m number(1),        
      arpu_2m number,
      voice_out_traf_2m number,
      voice_in_traf_2m number,
                  
      /* -3 week */
      paid_ind_3m number(1),
      out_traf_ind_3m number(1),         
      arpu_3m number,
      voice_out_traf_3m number,
      voice_in_traf_3m number                  
    
       
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
