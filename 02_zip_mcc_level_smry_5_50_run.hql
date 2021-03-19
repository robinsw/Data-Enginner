




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


Set mapred.task.timeout=18000000;
set mapreduce.reduce.memory.mb=10240;

--set mapreduce.job.queuename=highpriority;
--set mapreduce.job.queuename=vaprod;
set mapreduce.job.queuename=varegular;
--set mapreduce.job.queuename=valarge;





Use vadev;




--summry3 - MCC level All Product and paltforms;
------------------------------------------------------------------------------;

drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_smry_all_prdt_and_pltfm_5_50_1;
create table jg_zip_lvl_Mrch_Hotels_mcc_smry_all_prdt_and_pltfm_5_50_1 STORED AS SEQUENCEFILE as select
	industry,
	"All Products" as Product_Type,
	"All Platforms" as Platform,
	'b. Industry' as Population,
	"All Platforms and Pdts" as Rollup_Level,
	"" as Segments,
	"" as Segment_Criteria,
	Month_Updated,
	Time_Period,
	Mrch_zip_radious,
	state,
	region,
	mrch_pstl_cd,
	corporation_zip_lvl,
	case when rank() over (partition by 
		industry,
		Month_Updated,
		Time_Period,
		Mrch_zip_radious,
		state,
		region,
		mrch_pstl_cd
		order by sum(us_tran_amt) desc) =1 then corporation_zip_lvl else "" end as top_corp,
	case when rank() over (partition by 
		industry,
		Month_Updated,
		Time_Period,
		Mrch_zip_radious,
		state,
		region,
		mrch_pstl_cd
		order by sum(us_tran_amt) desc) =2 then corporation_zip_lvl else "" end as top_corp2,
	case when rank() over (partition by 
		industry,
		Month_Updated,
		Time_Period,
		Mrch_zip_radious,
		state,
		region,
		mrch_pstl_cd
		order by sum(us_tran_amt) desc) =3 then corporation_zip_lvl else "" end as top_corp3,
	case when rank() over (partition by 
		industry,
		Month_Updated,
		Time_Period,
		Mrch_zip_radious,
		state,
		region,
		mrch_pstl_cd
		order by sum(us_tran_amt) desc) =4 then corporation_zip_lvl else "" end as top_corp4,
	case when rank() over (partition by 
		industry,
		Month_Updated,
		Time_Period,
		Mrch_zip_radious,
		state,
		region,
		mrch_pstl_cd
		order by sum(us_tran_amt) desc) =5 then corporation_zip_lvl else "" end as top_corp5,
--	count(distinct acct_num_ods) as crd_cnt,
	sum(us_tran_amt) as us_tran_amt_mcc,
	sum(txn_cnt) as txn_cnt_mcc
from jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry_short
group by 
	industry,
	Month_Updated,
	Time_Period,
	Mrch_zip_radious,
	state,
	region,
	mrch_pstl_cd,
	corporation_zip_lvl;
	
	
drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_combined_5_50;
create table jg_zip_lvl_Mrch_Hotels_mcc_combined_5_50 STORED AS SEQUENCEFILE as select
	industry,
	Product_Type,
	Platform,
	Population,
	Rollup_Level,
	Segments,
	Segment_Criteria,
	Month_Updated,
	Time_Period,
	Mrch_zip_radious,
	state,
	region,
	mrch_pstl_cd,
	max(top_corp) as top_corp,
	max(top_corp2) as top_corp2,
	max(top_corp3) as top_corp3,
	max(top_corp4) as top_corp4,
	max(top_corp5) as top_corp5,
	max(us_tran_amt_mcc)/sum(us_tran_amt_mcc) as perc_50,
	sum(if(us_tran_amt_mcc > 0,1,0)) as merch_cnt,
	
--    case when (max(us_tran_amt_mcc)/sum(us_tran_amt_mcc)) > 0.5 then "FAIL" else "PASS" end as tot_PV_peer_50,
--    case when sum(if(us_tran_amt_mcc > 0,1,0)) < 6 then "FAIL" else "PASS" end as tot_pv_peer_5,

	case when max(us_tran_amt_mcc)/sum(us_tran_amt_mcc) > 0.5 and substr(trim(max(top_corp)),1,5) not in  ("OTHER") then "FAIL" else "PASS" end as tot_PV_peer_50,
	case when sum(if(us_tran_amt_mcc > 0,1,0)) < 6 then "FAIL" else "PASS" end as tot_pv_peer_5,
	
	sum(us_tran_amt_mcc) as us_tran_amt_mcc,
	sum(txn_cnt_mcc) as txn_cnt_mcc
from jg_zip_lvl_Mrch_Hotels_mcc_smry_all_prdt_and_pltfm_5_50_1
group by 
	industry,
	Product_Type,
	Platform,
	Population,
	Rollup_Level,
	Segments,
	Segment_Criteria,
	Month_Updated,
	Time_Period,
	Mrch_zip_radious,
	state,
	region,
	mrch_pstl_cd;	





