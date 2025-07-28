--market_key in('ANR',  'BGK',  'BIR',  'BRN',  'BUR',  'CHL',  'CHT',  'DTI',  'GRZ',  'IRK',  'IVN',  'KCH',  'KIR',  'KRL',  'KSK',  'KSM',  'MGD',  'NAL',  'NNG',  'RZN',  'SML',  'STV',  'TUL',  'TUM',  'VLA',  'VLD',  'VLK',  'YAM',  'YRL',  'EXT',  'HMS',  'IGK',  'KLG',  'PRM',  'SAM',  'TVR',  'STK',  'YAK')
-- Филиалы с коммутатором Huawey, на котором сработало уменьшение длительности дозвона в марте-апреле. Убираем эти филиалы также для КЦГ
-- Центр, Москва и Восток также убрал, так как там уменьшение длительности звонка уменьшенено со 120 минут до 60 с декабря 21г


truncate table mkn_ek_cg;
insert into  mkn_ek_cg
select t1.*
from(select t1.time_key,t1.market_key,t1.subs_key,t1.ban_key from MKN_PA_EK t1
inner  join (select * from  bay_vse_info where BUNDLE_TYPE_FOR_MIX in ('High Bundle','Low Bundle','Medium (incl. Unlim)'))bay
on t1.price_plan_key=bay.price_plan_key

left join ( select * from mkn_soc_ek_wo_zapec) t2
on t1.subs_key=t2.subs_key and t1.ban_key=t2.ban_key

where ek_ind=0 and SALES_IND_1M=0 and segment_key like 'B2C' and time_key=date'2021-11-01' and t2.subs_key is null and t2.ban_key is null
and t1.market_key='VIP'/* t1.market_key not in ('ABK',
'ANR',
'BAR',
'BGK',
'BIR',
'BUR',
'CHB',
'CHL',
'CHT',
'DTI',
'EKT',
'GAL',
'HBR',
'HMS',
'IGK',
'IKO',
'IRK',
'KIR',
'KMR',
'KRG',
'KRS',
'KSK',
'KZL',
'KZN',
'MGD',
'MGN',
'NNG',
'NOR',
'NSK',
'NTG',
'OMS',
'ORB',
'PNZ',
'PPK',
'PRM',
'SAM',
'SHL',
'SKH',
'SRN',
'SRT',
'STK',
'TMS',
'TOL',
'TUM',
'UFA',
'ULN',
'USH',
'VIP',
'VLA',
'YAK',
'YAM',
'GRZ',
'RZN',
'TUL',
'BRN',
'VLD',
'IVN',
'KLG',
'KSM',
'SML',
'TVR',
'YRL',
'NAL',
'VLK',
'KRL',
'KCH',
'STV',
'EXT')*/
order by dbms_random.random) t1
 where rownum <=500000
 ;
 commit;
 
 
 
 -----------Создание таблицы---------------------
drop table mkn_ek_tg purge
create table  mkn_ek_cg
           (        
      
      
      time_key date,
      market_key varchar2(3),
      subs_key varchar2(10),
      ban_key number
     /* segment_key varchar2(20),
      sales_ind_1m number,
      price_plan_key varchar2(256),
      account_type_key number,
      first_time_aab varchar2(100),
      ek_ind number*/
                
                
                
      
    
      
      
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


      
      
      
  
