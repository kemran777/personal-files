truncate table mkn_ek_nov_2021;     
begin 
      for i in (select market_key from dwh.dim_market where market_key not in ('-99','ALL') order by 1) loop
        insert /*+append*/ into mkn_ek_nov_2021
        select          
        distinct       
               C.time_key, 
               C.rtc_segment_key, 
               C.market_key,
               C.price_plan_key,
               C.ban_key,
               C.subs_key,
               C.arpu_amt_r,
               C.activity_type_mask,
               C.subs_activation_date_key,     
               a.group_type
                           
        from rep_str.es_FCT_RTC_MONTHLY@dwhstr_ik C
        inner join
        ((select market_key,ban_key,subs_key, 'TG' as group_type from mkn_ek_tg B where b.market_key=i.market_key /*and SOC_EFFECTIVE_DATE>=to_date('270721','ddmmyy') and SOC_EFFECTIVE_DATE<to_date('020821','ddmmyy')*/)
         union 
        (Select market_key,ban_key,subs_key, 'CG' as group_type from mkn_ek_cg a
        where market_key=i.market_key)) a  on C.ban_key = a.ban_key and C.subs_key = a.subs_key  
        left join bay_vse_info b1 on c.price_plan_key=b1.price_plan_key

        where C.time_key = date '2021-11-01'          
          and C.market_key = i.market_key
          and C.rtc_active_ind > 0
          and C.dw_status_key in ('A','S') 
          and C.rtc_segment_key in ('N01','N02','N03','N04','N05','N06','N07','N08','N09','N10','N11','N12','EXS') --B2C
          and b1.pp_archetype not in ('B2B LScr','B2C LScr','Tablet','TabletFREEM','TabletSDB','SEBRECIP')
          
        ;             
          commit;
          insert into MKN_LOG values(sysdate,'mkn_ek_nov_2021', i.market_key);
         commit;
      end loop;
end;

--control
Select group_type, count(subs_key), count(distinct subs_key) from mkn_ek_nov_2021
group by group_type





create table mkn_ek_nov_2021_v2
    (

       time_key date, 
       rtc_segment_key char(3), 
       market_key char(3),
       price_plan_key varchar2(20),
       ban_key number(9),
       subs_key char(10),
       arpu_amt_r number,
       activity_type_mask varchar2(50),
       subs_activation_date_key date,
               
       
       group_type char(2)

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
