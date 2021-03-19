



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




--set mapreduce.job.queuename=highpriority;
--set mapreduce.job.queuename=vaprod;
set mapreduce.job.queuename=varegular;
--set mapreduce.job.queuename=valarge;


set mapreduce.reduce.java.opts=-Xmx8153960755;
Set mapred.task.timeout=18000000;
set mapreduce.reduce.memory.mb=10240;



Use vadev;

drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry22 purge;
create table jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry22 STORED AS SEQUENCEFILE as select 
	
	acct_num_ods,
	month_updated,
	time_period,
	mrch_dba_id,	
	Case when CHANNEL in ('CNP') then "00000" else mrch_pstl_cd end as mrch_pstl_cd,
	mrch_dba_nm,
	corporation,
	industry,
	lat,
	lng,
	state,
	region,
	corporation_zip_lvl,
	sum(us_tran_amt) as us_tran_amt,
	sum(txn_cnt) as txn_cnt
from jg_Mrch_Hotels_mcc_acct_lvl_smry 
where Issuer_region ="US" and Merchant_region = "US"
group by acct_num_ods,
	month_updated,
	time_period,
	mrch_dba_id,
	Case when CHANNEL in ('CNP') then "00000" else mrch_pstl_cd end,
	mrch_dba_nm,
	corporation,
	industry,
	lat,
	lng,
	state,
	region,
	corporation_zip_lvl
;


--Input data from Athena. Zip to Zip Distance;
-----------------------------------------------;

drop table if exists jg_zip_radious_data_Hotels purge;
create table jg_zip_radious_data_Hotels STORED AS SEQUENCEFILE as select distinct zip5_1,zip5_2,Mrch_radious from(
select zip5_1,zip5_2,"0 Miles" as Mrch_radious from default.aqu_us_zip_distance where (trim(zip5_1)=trim(zip5_2))
union all
select zip5_1,zip5_2,"10 Miles" as Mrch_radious from default.aqu_us_zip_distance where distance_mile <=10
union all
select zip5_1,zip5_2,"20 Miles" as Mrch_radious from default.aqu_us_zip_distance where distance_mile <=20
)temp1;

drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry_short purge;
CREATE TABLE jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry_short STORED AS SEQUENCEFILE as select 
	a.industry,
	a.acct_num_ods,
	a.month_updated,
	a.time_period,
	a.mrch_dba_id,
	a.mrch_pstl_cd as mrch_pstl_cd_osv,
	a.mrch_dba_nm,
	a.corporation,
	a.lat,
	a.lng,
	a.state,
	a.region,
	a.corporation_zip_lvl,
	a.us_tran_amt,
	a.txn_cnt,
	c.zip5_1,
	c.zip5_2,
case when c.zip5_2 is not null then c.zip5_1 else a.mrch_pstl_cd end as mrch_pstl_cd,
case when c.zip5_2 is not null then c.Mrch_radious else "0 Miles" end as Mrch_zip_radious
from jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry22 a 
left outer join jg_zip_radious_data_Hotels c
on trim(a.mrch_pstl_cd)=trim(c.zip5_2)
;	

	



--summry3 - MCC level All Product and paltforms;
------------------------------------------------------------------------------;

drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_combined;
create table jg_zip_lvl_Mrch_Hotels_mcc_combined STORED AS SEQUENCEFILE as select
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
	count(distinct acct_num_ods) as crd_cnt,
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
	mrch_pstl_cd;











