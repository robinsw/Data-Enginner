


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
set mapreduce.reduce.java.opts=-Xmx8153960755;
set hive.mapred.mode=nonstrict;


Set mapred.task.timeout=18000000;
set mapreduce.reduce.memory.mb=10240;

--set mapreduce.job.queuename=highpriority;
--set mapreduce.job.queuename=vaprod;
set mapreduce.job.queuename=varegular;
--set mapreduce.job.queuename=valarge;

--set hive.exec.compress.output=true;
--set hive.exec.compress.intermediate=true;
--set mapreduce.output.fileoutputformat.compress=true;
--set mapreduce.map.output.compress=true;
--set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec;
--set mapreduce.output.fileoutputformat.compress.type = BLOCK;
--set hive.vectorized.execution.enabled=false;
--set hive.mapred.mode=nonstrict;
--set mapreduce.reduce.speculative =fales;
--
--set hive.execution.engine=tez;
--set tez.queue.name=varegular;



Use vadev;


	
----summry3 - Merchant level All Product and paltforms;
--------------------------------------------------------------------------------;

drop table if exists jg_zip_Mrch_Hotels_corp_combined;
create table jg_zip_Mrch_Hotels_corp_combined STORED AS SEQUENCEFILE as select


	Corporation,
	industry,
	"All Products" as Product_Type,
	"All Platforms" as Platform,
	'a. Merchant' as Population,
	"All Platforms and Pdts" as Rollup_Level,
	"" as Segments,
	"" as Segment_Criteria,
	Month_Updated,
	Time_Period,
	state,
	region,
	Mrch_zip_radious,
	mrch_pstl_cd,
	count(distinct acct_num_ods) as crd_cnt_merch,
	sum(us_tran_amt) as us_tran_amt_merch,
	sum(txn_cnt) as txn_cnt_merch
from jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry_short
--where Mrch_zip_radious in ('0 Miles','2 Miles','5 Miles')
group by 

	Corporation,
	industry,
	Month_Updated,
	Time_Period,
	state,
	region,
	Mrch_zip_radious,
	mrch_pstl_cd;
---==========================================================================================;
drop table if exists jg_mrch_db_zip_lvl_Hotels_corp_0_miles;
create table jg_mrch_db_zip_lvl_Hotels_corp_0_miles STORED AS SEQUENCEFILE as select distinct
	 corporation
	,industry
	,rollup_level
	,month_updated
	,population
	,product_type
	,Segments
	,Segment_Criteria
	,platform
	,time_period
	,mrch_pstl_cd
	,state
	,region
from jg_zip_Mrch_Hotels_corp_combined
where mrch_zip_radious in ('0 Miles');

drop table if exists jg_mrch_db_zip_lvl_Hotels_0_miles_corp;
create table jg_mrch_db_zip_lvl_Hotels_0_miles_corp STORED AS SEQUENCEFILE as select 
	a.*
	,b.Mrch_zip_radious
	,b.crd_cnt_merch
	,b.us_tran_amt_merch
	,b.txn_cnt_merch

from jg_mrch_db_zip_lvl_Hotels_corp_0_miles a 
left outer join jg_zip_Mrch_Hotels_corp_combined b
on  a.corporation = b.corporation
and a.rollup_level = b.rollup_level
and a.month_updated = b.month_updated
and a.population = b.population
and a.product_type = b.product_type
and a.platform = b.platform
and a.time_period = b.time_period
and a.mrch_pstl_cd = b.mrch_pstl_cd
and a.state = b.state
and a.region= b.region
and a.industry=b.industry
;
----===========================================================;
--- corporation level 0 miles;
----===========================================================;

drop table if exists jg_mrch_db_zip_lvl_Hotels_mcc_corp_0_miles;
create table jg_mrch_db_zip_lvl_Hotels_mcc_corp_0_miles STORED AS SEQUENCEFILE as select distinct
	industry
	,rollup_level
	,month_updated
	,population
	,product_type
	,Segments
	,Segment_Criteria
	,platform
	,time_period
	,mrch_pstl_cd
	,state
	,region

	
from jg_zip_lvl_Mrch_Hotels_mcc_combined
where mrch_zip_radious in ('0 Miles');

drop table if exists jg_mrch_db_zip_lvl_Hotels_0_miles_corp_mcc;
create table jg_mrch_db_zip_lvl_Hotels_0_miles_corp_mcc as select 
	a.*
	,b.Mrch_zip_radious
	,b.crd_cnt
	,b.us_tran_amt_mcc
	,b.txn_cnt_mcc

from jg_mrch_db_zip_lvl_Hotels_mcc_corp_0_miles a 
left outer join jg_zip_lvl_Mrch_Hotels_mcc_combined b
on  a.rollup_level = b.rollup_level
and a.month_updated = b.month_updated
and a.population = b.population
and a.product_type = b.product_type
and a.platform = b.platform
and a.time_period = b.time_period
and a.mrch_pstl_cd = b.mrch_pstl_cd
and a.state = b.state
and a.region = b.region
and a.industry=b.industry
;


--==============================================================================;
	


-- previous year values for growth calc;
-------------------------------------------------------------;
drop table if exists jg_zip_Mrch_Hotels_corp_preyear;
create table jg_zip_Mrch_Hotels_corp_preyear STORED AS SEQUENCEFILE as select * 
from jg_mrch_db_zip_lvl_Hotels_0_miles_corp
where Time_Period ='Year ended Jun15';

-- previous year values for growth calc mcc;
-------------------------------------------------------------;
drop table if exists jg_zip_Mrch_Hotels_mcc_preyear_corp;
create table jg_zip_Mrch_Hotels_mcc_preyear_corp STORED AS SEQUENCEFILE as select * 
from jg_mrch_db_zip_lvl_Hotels_0_miles_corp_mcc
where 
--Mrch_zip_radious in ('0 Miles','2 Miles','5 Miles') and 
Time_Period ='Year ended Jun15';


drop table if exists jg_zip_corp_list;
create table jg_zip_corp_list STORED AS SEQUENCEFILE as select distinct Corporation,industry from jg_MOR_details_Hotels;

drop table if exists jg_zip_Mrch_Hotels_mcc_corp_combined_crt_prdt;
create table jg_zip_Mrch_Hotels_mcc_corp_combined_crt_prdt STORED AS SEQUENCEFILE as
select a.*,b.Corporation from jg_mrch_db_zip_lvl_Hotels_0_miles_corp_mcc a join jg_zip_corp_list b
on trim(a.industry)=trim(b.industry)
where a.Rollup_Level = "All Platforms and Pdts";



-- merging merchant and previous year data to all records;
-------------------------------------------------------------;
drop table if exists jg_zip_Mrch_Hotels_mcc_corp_combined_w_merch_and_preyr_var;
create table jg_zip_Mrch_Hotels_mcc_corp_combined_w_merch_and_preyr_var STORED AS SEQUENCEFILE as select 
	a.industry,
	a.Product_Type,
	a.Platform,
	'a. Merchant' as Population,
	a.Rollup_Level,
	a.Segments,
	a.Segment_Criteria,
	a.Month_Updated,
	a.Time_Period,
	a.Mrch_zip_radious,
	a.state,
	a.region,
	a.mrch_pstl_cd,
	a.crd_cnt as crd_cnt_mcc,
	a.us_tran_amt_mcc,
	a.txn_cnt_mcc,
	a.Corporation,
	
	b.crd_cnt_merch,
	b.us_tran_amt_merch,
	b.txn_cnt_merch,

	c.us_tran_amt_merch as us_tran_amt_pre_yr,
	c.txn_cnt_merch as txn_cnt_pre_yr,
	c.crd_cnt_merch as crd_cnt_pre_yr,

	d.us_tran_amt_mcc as us_tran_amt_mcc_pre_yr,
	d.txn_cnt_mcc as txn_cnt_mcc_pre_yr,
	d.crd_cnt as crd_cnt_mcc_pre_yr	
from jg_zip_Mrch_Hotels_mcc_corp_combined_crt_prdt a 
left outer join jg_mrch_db_zip_lvl_Hotels_0_miles_corp b
	
--from jg_zip_Mrch_Hotels_corp_combined a 
--left outer join jg_zip_lvl_Mrch_Hotels_mcc_combined b
on trim(a.Product_Type) = trim(b.Product_Type)
	and trim(a.Platform) = trim(b.Platform)
	and trim(a.Month_Updated) = trim(b.Month_Updated)
	and trim(a.Time_Period) = trim(b.Time_Period)
	and trim(a.Corporation) = trim(b.Corporation)
	and trim(a.Rollup_Level) = trim(b.Rollup_Level)
	and trim(a.Segments) = trim(b.Segments)
	and trim(a.Segment_Criteria) = trim(b.Segment_Criteria)
--	and trim(a.tkt_quintile) = trim(b.tkt_quintile)
	and trim(a.state) = trim(b.state)
	and trim(a.region) = trim(b.region)
	and trim(a.Mrch_zip_radious) = trim(b.Mrch_zip_radious)
	and trim(a.mrch_pstl_cd) = trim(b.mrch_pstl_cd)
	and trim(a.industry) = trim(b.industry)
	

left outer join jg_zip_Mrch_Hotels_corp_preyear c
on trim(a.Product_Type) = trim(c.Product_Type)
	and trim(a.Platform) = trim(c.Platform)
	and trim(a.Month_Updated) = trim(c.Month_Updated)
	and trim(a.Corporation) = trim(c.Corporation)
	and trim(a.Rollup_Level) = trim(c.Rollup_Level)
	and trim(a.Segments) = trim(c.Segments)
	and trim(a.Segment_Criteria) = trim(c.Segment_Criteria)
--	and trim(a.tkt_quintile) = trim(c.tkt_quintile)
--	and trim(a.Population) = trim(c.Population)
	and trim(a.state) = trim(c.state)
	and trim(a.region) = trim(c.region)
	and trim(a.Mrch_zip_radious) = trim(c.Mrch_zip_radious)
	and trim(a.mrch_pstl_cd) = trim(c.mrch_pstl_cd)
	and trim(a.industry) = trim(c.industry)
left outer join jg_zip_Mrch_Hotels_mcc_preyear_corp d
on trim(a.Product_Type) = trim(d.Product_Type)
	and trim(a.Platform) = trim(d.Platform)
	and trim(a.Month_Updated) = trim(d.Month_Updated)
--	and trim(a.Time_Period) = trim(d.Time_Period)
--	and trim(a.Corporation) = trim(d.Corporation)
	and trim(a.Rollup_Level) = trim(d.Rollup_Level)
	and trim(a.Segments) = trim(d.Segments)
	and trim(a.Segment_Criteria) = trim(d.Segment_Criteria)
--	and trim(a.tkt_quintile) = trim(d.tkt_quintile)
	and trim(a.state) = trim(d.state)
	and trim(a.region) = trim(d.region)
	and trim(a.Mrch_zip_radious) = trim(d.Mrch_zip_radious)
	and trim(a.mrch_pstl_cd) = trim(d.mrch_pstl_cd)
	and trim(a.industry) = trim(d.industry)
where trim(a.Time_Period) ='Year ended Jun16'
	;
	

-- calculations;
-------------------------------------------------------------;
drop table if exists jg_zip_Mrch_Hotels_mcc_corp_combined_w_merch_and_preyr_var_w_calc;
create table jg_zip_Mrch_Hotels_mcc_corp_combined_w_merch_and_preyr_var_w_calc STORED AS SEQUENCEFILE as select
"All" as Client,*,

(us_tran_amt_mcc/txn_cnt_mcc) as Ticket_Size,
(us_tran_amt_mcc/crd_cnt_mcc) as Spend_per_card,
(txn_cnt_mcc/crd_cnt_mcc) as Txn_per_card,

(us_tran_amt_merch/txn_cnt_merch) as merch_Ticket_Size,
(us_tran_amt_merch/crd_cnt_merch) as merch_Spend_per_card,
(txn_cnt_merch/crd_cnt_merch) as merch_Txn_per_card,

(crd_cnt_merch/crd_cnt_mcc) as Active_Card_Share,
(us_tran_amt_merch/us_tran_amt_mcc) as Spend_Market_Share,
(txn_cnt_merch/txn_cnt_mcc) as Txn_Market_Share,

case when crd_cnt_pre_yr is null then 0 else (crd_cnt_pre_yr/crd_cnt_mcc_pre_yr) end as Active_Card_Share_ly,
case when crd_cnt_pre_yr is null then 0 else (us_tran_amt_pre_yr/us_tran_amt_mcc_pre_yr) end  as Spend_Market_Share_ly,
case when crd_cnt_pre_yr is null then 0 else (txn_cnt_pre_yr/txn_cnt_mcc_pre_yr) end as Txn_Market_Share_ly,

(us_tran_amt_mcc/crd_cnt_mcc) / (us_tran_amt_merch/crd_cnt_merch) as Ratio_of_Spend_per_Card_1,
(txn_cnt_mcc/crd_cnt_mcc) / (txn_cnt_merch/crd_cnt_merch) as Ratio_of_Txn_per_Card_1,

(us_tran_amt_merch/crd_cnt_merch) / (us_tran_amt_mcc/crd_cnt_mcc) as Ratio_of_Spend_per_Card,
(txn_cnt_merch/crd_cnt_merch) / (txn_cnt_mcc/crd_cnt_mcc) as Ratio_of_Txn_per_Card

from jg_zip_Mrch_Hotels_mcc_corp_combined_w_merch_and_preyr_var;
	
	




