







set hive.mapred.reduce.tasks.speculative.execution=false;
set hive.exec.parallel=false; 
set mapreduce.map.speculative=false;
set mapreduce.reduce.speculative=false;



set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.map.output.compress=true;
set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec;
set mapreduce.output.fileoutputformat.compress.type = BLOCK;
--set hive.auto.convert.join=true;
--set hive.optimize.skewjoin=true;

--set mapreduce.job.queuename=highpriority;
--set mapreduce.job.queuename=vaprod;
set mapreduce.job.queuename=varegular;
--set mapreduce.job.queuename=valarge;

set hive.mapjoin.localtask.max.memory.usage = 0.999;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.map.aggr.hash.force.flush.memory.threshold=0.8;
set hive.map.aggr.hash.min.reduction=1;
set hive.auto.convert.join=false;


Use vadev;



drop table if exists jg_mrch_db_zip_lvl_Hotels_final11;
create table jg_mrch_db_zip_lvl_Hotels_final11 STORED AS SEQUENCEFILE as select tmp.* from(
select 
corporation
,client
,industry
,rollup_level
,level
,month_updated
,population
,product_type
,platform
,time_period
,C_5_50_Result
,mrch_pstl_cd
,mrch_zip_radious
,msa_name
,state
,region
,spend_market_share
,txn_market_share
,active_card_share
,spend_market_share_ly
,txn_market_share_ly
,active_card_share_ly
,ratio_of_spend_per_card
,ratio_of_txn_per_card


,crd_cnt_merch
,us_tran_amt_merch
,txn_cnt_merch
,us_tran_amt_mcc
,txn_cnt_mcc
,crd_cnt_mcc
,us_tran_amt_pre_yr
,txn_cnt_pre_yr
,crd_cnt_pre_yr
,us_tran_amt_mcc_pre_yr
,txn_cnt_mcc_pre_yr
,crd_cnt_mcc_pre_yr


from jg_mrch_db_zip_lvl_Hotels
where merch_cnt > 5

union all

select 
corporation
,client
,industry
,rollup_level
,level
,month_updated
,population
,product_type
,platform
,time_period
,C_5_50_Result
,mrch_pstl_cd
,mrch_zip_radious
,msa_name
,state
,region
,spend_market_share
,txn_market_share
,active_card_share
,spend_market_share_ly
,txn_market_share_ly
,active_card_share_ly
,ratio_of_spend_per_card
,ratio_of_txn_per_card


,crd_cnt_merch
,us_tran_amt_merch
,txn_cnt_merch
,us_tran_amt_mcc
,txn_cnt_mcc
,crd_cnt_mcc
,us_tran_amt_pre_yr
,txn_cnt_pre_yr
,crd_cnt_pre_yr
,us_tran_amt_mcc_pre_yr
,txn_cnt_mcc_pre_yr
,crd_cnt_mcc_pre_yr

from jg_mrch_db_MSA_lvl_Hotels 
where merch_cnt > 5
)tmp;



-------------------------------------------------------------;
---- total pv % calculations;
-------------------------------------------------------------;

--dba lvl;
--------------;	
drop table if exists jg_zip_Mrch_Hotels_dba_pv_tot;
create table jg_zip_Mrch_Hotels_dba_pv_tot STORED AS SEQUENCEFILE as select
	mrch_dba_nm as client,
	Corporation,
	industry,
	Time_Period,
	sum(us_tran_amt_merch) as us_tran_amt_merch_tot
from jg_zip_Mrch_Hotels_dba_combined
where mrch_zip_radious in ('0 Miles')
group by 
	mrch_dba_nm,
	Corporation,
	industry,
	Time_Period;
	
--corp lvl;
--------------;	
drop table if exists jg_zip_Mrch_Hotels_corp_pv_tot;
create table jg_zip_Mrch_Hotels_corp_pv_tot STORED AS SEQUENCEFILE as select
	"All" as client,
	Corporation,
	industry,
	Time_Period,
	sum(us_tran_amt_merch) as us_tran_amt_merch_tot
from jg_zip_Mrch_Hotels_corp_combined
where mrch_zip_radious in ('0 Miles')
group by 
	Corporation,
	industry,
	Time_Period;
	
-- Merchant Pv tot;
--------------------;
drop table if exists jg_zip_Mrch_Hotels_Merchant_pv_tot;	
create table jg_zip_Mrch_Hotels_Merchant_pv_tot STORED AS SEQUENCEFILE as select * from(
select * from jg_zip_Mrch_Hotels_dba_pv_tot
union all
select * from jg_zip_Mrch_Hotels_corp_pv_tot)tmp1;
	

--mcc lvl;
--------------	
drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_pv_tot;
create table jg_zip_lvl_Mrch_Hotels_mcc_pv_tot STORED AS SEQUENCEFILE as select
	
	industry,
	Time_Period,
	sum(us_tran_amt_mcc) as us_tran_amt_mcc_tot
from jg_zip_lvl_Mrch_Hotels_mcc_combined
where mrch_zip_radious in ('0 Miles')
group by 
	industry,
	Time_Period;

--combining tot pv;
-------------------
drop table if exists jg_zip_lvl_Mrch_Hotels_tot_Pv_data;	
create table jg_zip_lvl_Mrch_Hotels_tot_Pv_data STORED AS SEQUENCEFILE as select 
a.*,
b.us_tran_amt_mcc_tot
from jg_zip_Mrch_Hotels_Merchant_pv_tot a
left outer join jg_zip_lvl_Mrch_Hotels_mcc_pv_tot b
on a.Time_Period=b.Time_Period and
	a.industry=b.industry;

--previous year;
----------------;
drop table if exists jg_zip_lvl_Mrch_Hotels_tot_Pv_data_pre_yr;
create table jg_zip_lvl_Mrch_Hotels_tot_Pv_data_pre_yr  STORED AS SEQUENCEFILE as select
* from jg_zip_lvl_Mrch_Hotels_tot_Pv_data
where Time_Period ='Year ended Jun15';


--merging last year data ;
--------------------------;
drop table if exists jg_zip_lvl_Mrch_Hotels_tot_Pv_data_final;
create table jg_zip_lvl_Mrch_Hotels_tot_Pv_data_final  STORED AS SEQUENCEFILE as select
a.*
,b.us_tran_amt_merch_tot as us_tran_amt_merch_tot_ly
,b.us_tran_amt_mcc_tot as us_tran_amt_mcc_tot_ly
from jg_zip_lvl_Mrch_Hotels_tot_Pv_data a
left outer join jg_zip_lvl_Mrch_Hotels_tot_Pv_data_pre_yr b
on a.client=b.client
and a.Corporation=b.Corporation
and a.industry = b.industry
where a.Time_Period ='Year ended Jun16';

--Merging with data;
---------------------;

drop table if exists jg_mrch_db_zip_lvl_Hotels_final22;
create table jg_mrch_db_zip_lvl_Hotels_final22 STORED AS SEQUENCEFILE as select a.*
,b.us_tran_amt_merch_tot
,b.us_tran_amt_mcc_tot
,b.us_tran_amt_merch_tot_ly
,b.us_tran_amt_mcc_tot_ly
,(a.us_tran_amt_merch/b.us_tran_amt_merch_tot) as per_mrch_pv
,(a.us_tran_amt_mcc/b.us_tran_amt_mcc_tot) as per_ind_pv
,(a.us_tran_amt_pre_yr/us_tran_amt_merch_tot_ly) as per_mrch_pv_ly
,(a.us_tran_amt_mcc_pre_yr/us_tran_amt_mcc_tot_ly) as per_ind_pv_ly
from jg_mrch_db_zip_lvl_Hotels_final11 a
left outer join jg_zip_lvl_Mrch_Hotels_tot_Pv_data_final b
on a.client=b.client
and a.Corporation=b.Corporation
and a.industry=b.industry;

--Store Lvel data preperation;
------------------------------------;
drop table if exists jg_mrch_db_zip_lvl_Hotels_final_store_lvl;
create table jg_mrch_db_zip_lvl_Hotels_final_store_lvl STORED AS SEQUENCEFILE as select 

 corporation
,client
,industry
,rollup_level
,level
,month_updated
,population
,product_type
,platform
,time_period
,C_5_50_Result
,mrch_pstl_cd
--,zip_city
,state
,region
--,lat
--,lng

,crd_cnt_merch
,us_tran_amt_merch
,txn_cnt_merch
,us_tran_amt_pre_yr
,txn_cnt_pre_yr
,crd_cnt_pre_yr
from jg_mrch_db_zip_lvl_Hotels_final22 
where mrch_zip_radious = "0 Miles";



drop table if exists jg_mrch_db_zip_lvl_Hotels_final33 purge;
create table jg_mrch_db_zip_lvl_Hotels_final33 STORED AS SEQUENCEFILE as select 
a.*
,b.crd_cnt_merch as store_crd_cnt_merch
,b.us_tran_amt_merch as store_us_tran_amt_merch
,b.txn_cnt_merch as store_txn_cnt_merch
,b.us_tran_amt_pre_yr as store_us_tran_amt_pre_yr
,b.txn_cnt_pre_yr as store_txn_cnt_pre_yr
,b.crd_cnt_pre_yr as store_crd_cnt_pre_yr

from jg_mrch_db_zip_lvl_Hotels_final22 a left outer join jg_mrch_db_zip_lvl_Hotels_final_store_lvl b
on trim(a.corporation) = trim(b.corporation)
and trim(a.client) = trim(b.client)
and trim(a.rollup_level) = trim(b.rollup_level)
and trim(a.level) = trim(b.level)
and trim(a.month_updated) = trim(b.month_updated)
and trim(a.population) = trim(b.population)
and trim(a.product_type) = trim(b.product_type)
and trim(a.platform) = trim(b.platform)
and trim(a.time_period) = trim(b.time_period)
and trim(a.C_5_50_Result) = trim(b.C_5_50_Result)
and trim(a.mrch_pstl_cd) = trim(b.mrch_pstl_cd)
and trim(a.state) = trim(b.state)
and trim(a.region) = trim(b.region)
and trim(a.industry) = trim(b.industry)
;



drop table if exists jg_mrch_db_zip_lvl_Hotels_final44;
create table jg_mrch_db_zip_lvl_Hotels_final44 STORED AS SEQUENCEFILE as select *,

(store_us_tran_amt_merch/store_txn_cnt_merch) as store_merch_Ticket_Size,
(store_us_tran_amt_merch/store_crd_cnt_merch) as store_merch_Spend_per_card,
(store_txn_cnt_merch/store_crd_cnt_merch) as store_merch_Txn_per_card,

(store_crd_cnt_merch/crd_cnt_mcc) as store_Active_Card_Share,
(store_us_tran_amt_merch/us_tran_amt_mcc) as store_Spend_Market_Share,
(store_txn_cnt_merch/txn_cnt_mcc) as store_Txn_Market_Share,

case when store_crd_cnt_pre_yr is null then 0 else (store_crd_cnt_pre_yr/crd_cnt_mcc_pre_yr) end as store_Active_Card_Share_ly,
case when store_crd_cnt_pre_yr is null then 0 else (store_us_tran_amt_pre_yr/us_tran_amt_mcc_pre_yr) end  as store_Spend_Market_Share_ly,
case when store_crd_cnt_pre_yr is null then 0 else (store_txn_cnt_pre_yr/txn_cnt_mcc_pre_yr) end as store_Txn_Market_Share_ly,

(us_tran_amt_mcc/crd_cnt_mcc) / (store_us_tran_amt_merch/store_crd_cnt_merch) as store_Ratio_of_Spend_per_Card_1,
(txn_cnt_mcc/crd_cnt_mcc) / (store_txn_cnt_merch/store_crd_cnt_merch) as store_Ratio_of_Txn_per_Card_1,

(store_us_tran_amt_merch/store_crd_cnt_merch) / (us_tran_amt_mcc/crd_cnt_mcc) as store_Ratio_of_Spend_per_Card,
(store_txn_cnt_merch/store_crd_cnt_merch) / (txn_cnt_mcc/crd_cnt_mcc) as store_Ratio_of_Txn_per_Card,

(store_us_tran_amt_merch/us_tran_amt_merch_tot) as store_per_mrch_pv,
(store_us_tran_amt_pre_yr/us_tran_amt_merch_tot_ly) as store_per_mrch_pv_ly,


((us_tran_amt_mcc/us_tran_amt_mcc_pre_yr)-1) as Growth_ind_Pv

from jg_mrch_db_zip_lvl_Hotels_final33

where C_5_50_Result not in ('FAIL')
and (crd_cnt_merch > 10 or crd_cnt_merch <=0 or crd_cnt_merch is null)
and (crd_cnt_mcc > 10 or crd_cnt_mcc <=0 or crd_cnt_mcc is null)
--and ((trim(Mrch_zip_radious) in ('0 Miles','2 Miles','5 Miles','')) or Mrch_zip_radious is null)
;




--------------------------------------------------------------;
----geting the list of zips in the readious;
-----------------------------------------------------------;

drop table if exists jg_zip_radious_data_list_Hotels;
create table jg_zip_radious_data_list_Hotels STORED AS SEQUENCEFILE as select 
	zip5_1,
	Mrch_radious,
	collect_set(zip5_2) as zip_list
from jg_zip_radious_data_Hotels
group by 
	zip5_1,
	Mrch_radious;

--selecting the required variables in order;
--------------------------------------------;

drop table if exists jg_mrch_db_Mrkt_shr_zip_lvl_final_Hotels;
create table jg_mrch_db_Mrkt_shr_zip_lvl_final_Hotels STORED AS SEQUENCEFILE as select 
industry
,a.corporation
,case when a.client = "All" then CONCAT(a.client," ",a.corporation) else a.client end as cleint
,a.rollup_level
,a.level
,a.month_updated
,a.population
,a.product_type
,a.platform
,a.time_period
,a.C_5_50_Result
,a.mrch_pstl_cd as zip
,a.mrch_zip_radious as mrch_zip_radius
,case when trim(a.mrch_pstl_cd) in ('00000') then "ONLINE MSA"
	when (a.msa_name is not null) and trim(a.msa_name) not in ('') then a.msa_name
	when (a.msa_name is null or trim(a.msa_name) in ('')) and d.zip is null then "NON MSA"
	when d.zip is null then a.msa_name
	when d.zip is not null and (d.msa_name is null or trim(d.msa_name) in ('')) then "NON MSA"
	else trim(d.msa_name) end as msa
,d.zip_city
,a.state
,a.region
,d.lat as lat_data
,d.lng as long_data
--,a.us_tran_amt_merch as Merchant_PV
,a.spend_market_share
,a.txn_market_share
,a.active_card_share
,a.spend_market_share_ly
,a.txn_market_share_ly
,a.active_card_share_ly
,a.ratio_of_spend_per_card
,a.ratio_of_txn_per_card

,a.Growth_ind_Pv

,a.per_mrch_pv
,a.per_ind_pv
,a.per_mrch_pv_ly
,a.per_ind_pv_ly

,a.store_per_mrch_pv
,a.store_per_mrch_pv_ly

,a.store_Active_Card_Share
,a.store_Spend_Market_Share
,a.store_Txn_Market_Share
,a.store_Active_Card_Share_ly
,a.store_Spend_Market_Share_ly
,a.store_Txn_Market_Share_ly
,a.store_Ratio_of_Spend_per_Card
,a.store_Ratio_of_Txn_per_Card
,c.zip_list

from jg_mrch_db_zip_lvl_Hotels_final44 a 
left outer join default.jg_zip_to_msa_melisa d 
	on trim(a.mrch_pstl_cd)=trim(d.zip)
left outer join jg_zip_radious_data_list_Hotels c
on a.mrch_pstl_cd=c.zip5_1
and a.mrch_zip_radious=c.Mrch_radious 
;








