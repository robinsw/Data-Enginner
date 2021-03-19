

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


drop table if exists jg_zip_lvl_data_Hotels;
create table jg_zip_lvl_data_Hotels STORED AS SEQUENCEFILE as select tmp2.* from(

select
client
,corporation
,industry
,product_type
,platform
,population
,rollup_level
,segments
,segment_criteria
,month_updated
,time_period
,state
,region
,"" as msa_name
,mrch_zip_radious
,mrch_pstl_cd
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
,ticket_size
,spend_per_card
,txn_per_card
,merch_ticket_size
,merch_spend_per_card
,merch_txn_per_card
,active_card_share
,spend_market_share
,txn_market_share
,active_card_share_ly
,spend_market_share_ly
,txn_market_share_ly
,ratio_of_spend_per_card
,ratio_of_txn_per_card
from jg_zip_Mrch_Hotels_mcc_DBA_combined_w_merch_and_preyr_var_w_calc


union all

select 
client
,corporation
,industry
,product_type
,platform
,population
,rollup_level
,segments
,segment_criteria
,month_updated
,time_period
,state
,region
,"" as msa_name
,mrch_zip_radious
,mrch_pstl_cd
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
,ticket_size
,spend_per_card
,txn_per_card
,merch_ticket_size
,merch_spend_per_card
,merch_txn_per_card
,active_card_share
,spend_market_share
,txn_market_share
,active_card_share_ly
,spend_market_share_ly
,txn_market_share_ly
,ratio_of_spend_per_card
,ratio_of_txn_per_card
from jg_zip_Mrch_Hotels_mcc_corp_combined_w_merch_and_preyr_var_w_calc)tmp2;

drop table if exists jg_mrch_db_zip_lvl_Hotels_1;
create table jg_mrch_db_zip_lvl_Hotels_1 STORED AS SEQUENCEFILE as select a.*,"ZIP" as level,
case 
--	when (a.crd_cnt_merch < 10 or a.crd_cnt_mcc < 10 )then "FAIL"
	when b.tot_PV_peer_50 = "PASS" and b.tot_pv_peer_5 = "PASS" then "PASS"
	when b.tot_pv_peer_5 = "FAIL"  then "FAIL"
	when (b.tot_PV_peer_50 = "FAIL" and 
			((trim(b.top_corp) = trim(a.corporation)) 
				or (substr(trim(b.top_corp),1,5) = "OTHER") 
			)
		) then "PASS"
		else "FAIL" end as C_5_50_Result,
		b.top_corp,
		b.tot_PV_peer_50,
		b.tot_pv_peer_5,
		b.perc_50,
		b.merch_cnt		
from jg_zip_lvl_data_Hotels a left outer join jg_zip_lvl_Mrch_Hotels_mcc_combined_5_50 b
on a.Product_Type = b.Product_Type and
a.Platform = b.Platform and
--a.Population = b.Population and
a.Rollup_Level = b.Rollup_Level and
a.Segments = b.Segments and
a.Segment_Criteria = b.Segment_Criteria and
a.Month_Updated = b.Month_Updated and
a.Time_Period = b.Time_Period and
a.Mrch_zip_radious=b.Mrch_zip_radious and 
a.state=b.state and
a.region=b.region and
a.mrch_pstl_cd=b.mrch_pstl_cd and
a.industry=b.industry
;


drop table if exists jg_cop_list;
create table jg_cop_list STORED AS SEQUENCEFILE as select distinct corporation,industry from jg_MOR_details_Hotels;

drop table if exists jg_mrch_db_zip_lvl_Hotels;
create table jg_mrch_db_zip_lvl_Hotels STORED AS SEQUENCEFILE as select a.* from
jg_mrch_db_zip_lvl_Hotels_1 a inner join jg_cop_list b
on a.corporation=b.corporation
	and a.industry=b.industry;











