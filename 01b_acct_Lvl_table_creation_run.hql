

--set hive.mapred.reduce.tasks.speculative.execution=false;
--set hive.exec.parallel=false; 
--set mapreduce.map.speculative=false;
--set mapreduce.reduce.speculative=false;
--
--
--
--set hive.exec.compress.output=true;
--set hive.exec.compress.intermediate=true;
--set mapreduce.output.fileoutputformat.compress=true;
--set mapreduce.map.output.compress=true;
--set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec;
--set mapreduce.output.fileoutputformat.compress.type = BLOCK;

set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.exec.reducers.bytes.per.reducer=270532608;
set hive.hadoop.supports.splittable.combineinputformat=true;
set mapreduce.input.fileinputformat.split.maxsize=2048000000;
set mapreduce.input.fileinputformat.split.minsize=1024000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.map.speculative=true;
set mapreduce.job.speculative.speculativecap=0.1;
set hive.map.aggr=true;
set hive.fetch.task.conversion = more;
set hive.fetch.task.aggr=true;
set hive.fetch.task.conversion.threshold=1024000000;
set mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;
set mapred.output.compression.type = BLOCK;


set mapreduce.job.queuename=varegular;



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
--set tez.queue.name=highpriority;




Use vadev;

----- STR file dedupe logic;
--------------------------------;
--drop table if exists jg_hotel_class_mapping_STR purge;
--create table jg_hotel_class_mapping_STR as select
--	match_visa_zipcode,
--	match_visa_dscrprtr,
--	case when upper(trim(class)) in ('MIDSCALE CLASS','UPPER MIDSCALE CLASS') then "UPPER MIDSCALE & MIDSCALE CLASS"
--	when upper(trim(class)) in ('UPPER UPSCALE CLASS','UPSCALE CLASS') then "UPPER UPSCALE & UPSCALE CLASS"
--	else upper(trim(class)) end as class,
----	class,
--	max(affiliation) as affiliation 
--from (select
--	e.match_visa_zipcode,
--	e.match_visa_dscrprtr,
--	e.class,
--	e.affiliation,
--	e.match_quality
--	from (select 
--		max(class) as class,
--		match_visa_dscrprtr,
--		match_visa_zipcode,
--		match_quality		
--	from (select a.* from default.cm_xx_str_matching_lookup_us a 
--			inner join 
--				(select match_visa_dscrprtr,match_visa_zipcode,min(match_quality) as match_quality from default.cm_xx_str_matching_lookup_us where match_quality > 0 group by match_visa_dscrprtr,match_visa_zipcode) b
--			on a.match_visa_dscrprtr=b.match_visa_dscrprtr
--			and a.match_visa_zipcode=b.match_visa_zipcode
--			and a.match_quality=b.match_quality
--		) c group by match_visa_dscrprtr,match_visa_zipcode,match_quality)d  inner join default.cm_xx_str_matching_lookup_us e
--on d.match_visa_dscrprtr=e.match_visa_dscrprtr
--and d.match_visa_zipcode=e.match_visa_zipcode
--and d.class=e.class
--and d.match_quality=e.match_quality)f group by match_visa_dscrprtr,match_visa_zipcode,class ;
--
-----variable creation in transaction level file;
--------------------------------------------------------;
--
--drop table if exists  jg_Mrch_Hotels_osv_txn_data_full purge;
--create table jg_Mrch_Hotels_osv_txn_data_full STORED AS SEQUENCEFILE as select 
--	tmp.acct_num_ods,
--	tmp.Product_Type,
--	tmp.Platform,
--	tmp.Month_Updated,
--	tmp.Rep_Month,
--	tmp.mrch_dba_id,
--	tmp.mrch_st_cd_enr,
--	tmp.mrch_pstl_cd,
--	tmp.mrch_nm_dscrptr,
--	tmp.eci_moto_cd ,
--	tmp.merch_desc_3,
--	tmp.us_tran_amt, 
--	tmp.dt,
--	tmp.POSENTRY_MODE_CD,
--	str_class_final,
--	str_affliation,
--	str_class, 	
--	hotel_Class,
--	mrg_flg_2,			
--	
--	
--  case when mrch_catg_cd in (3501,3502,3503,3504,3505,3506,3507,3508,3509,3510,3511,3512,3513,3514,3515,3516,3517,3518,3519,3520,3521,3522,3523,3524,3525,3526,3527,3528,3529, 3530,3531,3532,3533,3534,3535,3536,3537,3538,3539,3540,3541,3542,3543,3544,3545,3546,3548,3549,3550,3551,3552,3553,3555,3556,3557,3558,3559,3560,3561,3562,3563,3564,3565,3566,3567,3569, 3570,3571,3572,3573,3575,3576,3577,3579,3580,3581,3582,3583,3584,3585,3586,3587,3588,3589,3590,3591,3592,3595,3596,3597,3598,3599,3600,3602,3604,3606,3607,3608,3609,3610,3611,3612,3613, 3614,3615,3616,3617,3618,3619,3620,3621,3623,3625,3626,3627,3628,3629,3630,3631,3632,3633,3634,3635,3636,3637,3638,3639,3640,3641,3642,3643,3644,3645,3646,3647,3649,3650,3651,3652,3653, 3654,3655,3656,3657,3658,3659,3660,3661,3662,3663,3664,3665,3667,3668,3669,3670,3671,3672,3673,3674,3675,3676,3677,3678,3679,3680,3681,3683,3684,3685,3686,3687,3688,3689,3690,3692,3693, 3694,3695,3696,3697,3698,3699,3700,3701,3702,3703,3704,3706,3707,3708,3709,3710,3711,3712,3713,3714,3715,3716,3717,3718,3719,3720,3721,3722,3723,3724,3725,3726,3727,3728,3729,3730,3731, 3732,3733,3735,3736,3737,3738,3739,3740,3741,3742,3743,3744,3745,3746,3747,3748,3749,3750,3751,3752,3753,3754,3755,3757,3760,3761,3762,3763,3764,3765,3766,3767,3768,3769,3770,3771,3772, 3773,3774,3775,3776,3777,3778,3779,3780,3781,3782,3783,3784,3785,3786,3787,3788,3789,3790,3791,3792,3793,3794,3795,3796,3797,3798,3799,3800,3801,3802,3804,3807,3808,3811,3812,3813,3814, 3815,3816,3818,3819,3820,3822,3823,3824,3825,3826,3827,3828,3829,3830,3831,3832,3833,7011) then 'Hotels' when mrch_catg_cd in (4411) then 'Cruise Ships' when mrch_catg_cd in (4814,4812) then 'Telecommunication' when mrch_catg_cd in (4899) then 'Cable' when mrch_catg_cd in (5542) then 'AFDs Industry' when mrch_catg_cd in (5541,5499) then 'Service Stations Industry' when mrch_catg_cd in (3405, 3357, 3389, 3366, 3393, 7512, 3387, 3390, 3395, 3441, 3359, 3355, 3370, 3412, 3351, 3427, 3354, 3364, 3380, 3409, 3435, 3368, 3398, 3400, 3385, 3433, 3425, 3362, 3388, 3431, 3429, 3360, 3386, 3438, 3436, 7513) then 'Auto Rentals' when mrch_catg_cd in (5310,5331) then 'Discount Stores' when mrch_catg_cd in (6300,5960) and mrch_dba_id in (3673,1295,12394,138,27450,8,4028,134,2106,9299,26432,3424,14248,10105,47,14194,31014,1156,162,3826,29267,14186,26443,24525,1997,22708,29060,37126,34522,26511,27599,14268,28708,34225,14124,27842,26430, 14977,78,23199,26474,28546,29067,34724,29167,26251,34376,35995,26406,30994,33408,12828,24557,32304,37768,26603,29171,29145,38409,14230,37078,34352,34739,34968,28728,31933,27094,31210,31018,34523,37115, 39828,35232,31652,31703,26461,25070,31003,34731,37084,30993,34533,3939,31005,37208,23281,34964,39938,34723,25830,40093,30604,39775,31328,31017,39742,28330,37920,35223,34762,39764,24549,24523,26602,39871, 26536,40082,31850,26448,39831,40067,39899,31943,37601,39890,40288,40277,40291,3116,29545,31906,29546,40231,24633,40279,24558,40316,29533,24718,30996,40295,40280,40297,29634,40272,40289,40293,40281,34537, 40300,24735,3636,34538,949,32712,14171,40319,957,1427,34342,39392,24887,34388,1492,14820,34531,31006,4115,15190,28065,35093,35094) then 'Property and Casualty Insurance' when mrch_catg_cd in (5200,5251,5231,5261,5211) then 'Home Improvement' when ((mrch_catg_cd in (5311,5611,5621,5631,5651,5691,5699) and mrch_dba_id in (10987,2151,3891,1656,1933,1756,3500,950,1392,524,13578,14314,358,161,3834,32526,9315,14271,3437,1128,683,2121,31343,3683,1866,4020,4240,13865,349,505,26078,999,65,24990,1953,10699,1863,919,14832, 15203,1790,3065,34274,13679,15363,1489,34235,198,1539,19,3198,738,35748,2179,487,3289,17412,4104,21632,28802,3784,1826,14837,30371,13586,1381,1847,28713,27984,18248,1692,24905,27172,14004,14102, 30115,29090,14177,15487,14336,13037,13705,520,17695,14825,32116,31829,15128,32136,29010,1461,36221,34996,4231,27373,13861,613,32574,2155,14892,13948,26599,4048,2190,24548,635,27743,3210,32945, 14134,30363,15032,34212,14718,31211,29540,35752,28720,36023,225,633,17046,30599,16198,14838,1678,1277,36464,432,14398,15359,16731,16890,14158,36588,24573,456,2172,13746,26868,33914,31468,998, 3659,24632,209,1685,35103,2152,16112,38216,16002,3072,15356,26513,36568,26596,13983,30392,584,2053,14099,18404,13880,15972,23478,16091,14280,2537,2478,2703,2704,2722,3040)) or mrch_dba_id in (2151)) then 'Department Stores' else 'Other' end as industry ,
--  
--  case when dow in ('Sat','Sum') then "Weekend" else "Weekday" end as Time_Of_Week,
--	
--	case when tmp.dt < 20150701 then 'Year ended Jun15' else 'Year ended Jun16' end as Time_Period,	
--	
--	case when acct_num_ctry_cd = 840  then 'US' else 'Non US' end as Issuer_region,
--	case when MRCH_CTRY_CD_ENR = 840 then 'US' else 'Non US' end as Merchant_region,
--		
--	case 
--      when trim(POSENTRY_MODE_CD) in ('02','03','05','07','90','91','95') then 'F2F'
--      when trim(eci_moto_cd) in ('01','02','03','04','05','06','07','08','09') then 'CNP'
--              else 'F2F' end as CHANNEL ,
--
--case when tmp.dt_tm is not null and (trim(POSENTRY_MODE_CD) in ('02','03','05','07','90','91','95') or trim(eci_moto_cd) not in ('01','02','03','04','05','06','07','08','09')) 
--					and (hour(tmp.dt_tm) >= 3) and (hour(tmp.dt_tm) < 11) then "Breakfast"
--	when tmp.dt_tm is not null and (trim(POSENTRY_MODE_CD) in ('02','03','05','07','90','91','95') or trim(eci_moto_cd) not in ('01','02','03','04','05','06','07','08','09'))
--					and (hour(tmp.dt_tm) >= 11) and (hour(tmp.dt_tm) < 16) then "Lunch"
--	when tmp.dt_tm is not null and (trim(POSENTRY_MODE_CD) in ('02','03','05','07','90','91','95') or trim(eci_moto_cd) not in ('01','02','03','04','05','06','07','08','09'))
--					and (( (hour(tmp.dt_tm) >= 16) and (hour(tmp.dt_tm) < 25)) or (hour(tmp.dt_tm) < 3))  then "Dinner"
--	else "Unknown" end as Time_of_day
--
--from
--(select /*+ MAPJOIN(f) */ a.*,f.Time_Zone,
--case when trim(f.Time_Zone) in ('Eastern')
--     then (from_utc_timestamp(concat(substr(tran_gmt_dt,1,4),'-',(substr(tran_gmt_dt,5,2)),'-',(substr(tran_gmt_dt,7,2)),
--     ' ',(substr(tran_gmt_tm,1,2)),':',(substr(tran_gmt_tm,3,2)),':',(substr(tran_gmt_tm,5,2))),'EST'))
--
--    when trim(f.Time_Zone) in ('Central')
--      then (from_utc_timestamp(concat(substr(tran_gmt_dt,1,4),'-',(substr(tran_gmt_dt,5,2)),'-',(substr(tran_gmt_dt,7,2)),
--     ' ',(substr(tran_gmt_tm,1,2)),':',(substr(tran_gmt_tm,3,2)),':',(substr(tran_gmt_tm,5,2))),'CST'))
--
--     when trim(f.Time_Zone) in ('Mountain')
--      then (from_utc_timestamp(concat(substr(tran_gmt_dt,1,4),'-',(substr(tran_gmt_dt,5,2)),'-',(substr(tran_gmt_dt,7,2)),
--     ' ',(substr(tran_gmt_tm,1,2)),':',(substr(tran_gmt_tm,3,2)),':',(substr(tran_gmt_tm,5,2))),'MST'))
--
--     when trim(f.Time_Zone) in ('Pacific')
--       then (from_utc_timestamp(concat(substr(tran_gmt_dt,1,4),'-',(substr(tran_gmt_dt,5,2)),'-',(substr(tran_gmt_dt,7,2)),
--     ' ',(substr(tran_gmt_tm,1,2)),':',(substr(tran_gmt_tm,3,2)),':',(substr(tran_gmt_tm,5,2))),'PST'))
--
--      end as dt_tm,
--
-- from_unixtime(unix_timestamp(concat(substr(prch_dt,1,4),'-',(substr(prch_dt,5,2)),'-',(substr(prch_dt,7,2))),'yy-MM-dd'),'EEE') as DOW,        
--
--
--
--	case when length(trim(a.mrch_nm_dscrptr)) < 3 then a.mrch_nm_dscrptr
--		when (((length(trim(a.mrch_nm_dscrptr)) - 0 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),1,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),1,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 1 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),2,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),2,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 2 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),3,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),3,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 3 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),4,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),4,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 4 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),5,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),5,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 5 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),6,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),6,3)				
--		when (((length(trim(a.mrch_nm_dscrptr)) - 6 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),7,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),7,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 7 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),8,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),8,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 8 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),9,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),9,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 9 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),10,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),10,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 10 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),11,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),11,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 11 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),12,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),12,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 12 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),13,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),13,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 13 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),14,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),14,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 14 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),15,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),15,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 15 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),16,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),16,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 16 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),17,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),17,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 17 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),18,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),18,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 18 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),19,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),19,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 19 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),20,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),20,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 20 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),21,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),21,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 21 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),22,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),22,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 22 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),23,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),23,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 23 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),24,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),24,3)
--		when (((length(trim(a.mrch_nm_dscrptr)) - 24 ) >=3 ) and   (substr(trim(a.mrch_nm_dscrptr),25,1) not in ('0','1','2','3','4','5','6','7','8','9','-',' ','#','!','@','$','%','^','&','*','_','=','+','/','?',',','<','.','>','[',']','{','}','|','(',')')))
--				then substr(trim(a.mrch_nm_dscrptr),25,3)
--		else substr(trim(a.mrch_nm_dscrptr),25,3) end as merch_desc_3,
--
------below part is for hotel classification
--		
--	c.class as str_class_final,
--	c.affiliation as str_affliation,
--	c.class as str_class, 	
--		
--case when c.class is not null then upper(c.class)
--	when trim(d.mrch_nm_dscrptr) is not null then upper(d.hotel_class)
--	else "None" end as hotel_Class,
--case when c.class is not null then "STR" 
--	when trim(d.mrch_nm_dscrptr) is not null then "Model"
--	else "N" end as mrg_flg_2,			
--	
--		
--	substr(trim(dt),1,6) as Rep_Month
--
--from jg_Mrch_Hotels_osv_data_full a left outer join default.jg_zip_lvl_time_zone f
--on substr(trim(a.mrch_pstl_cd),1,5) = substr(trim(f.zip),1,5)
--left outer join jg_hotel_class_mapping_STR c   ------Merging the STR file for Hotel classification
-- on trim(c.match_visa_dscrprtr)=trim(a.mrch_nm_dscrptr)
--		and trim(a.mrch_pstl_cd)=trim(c.match_visa_zipcode)
--left outer join jg_model_descptr_with_class d   ------Merging the Model file for Hotel classification
--	on trim(a.mrch_nm_dscrptr)=trim(d.mrch_nm_dscrptr)
--)tmp ;
--
--
--
------------------------------------------------------------------------------;
----card level summary;
------------------------------------------------------------------------------;
--drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry1 purge;
--CREATE TABLE jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry1 STORED AS SEQUENCEFILE as select * from(
--select
--acct_num_ods as acct_num_osv,
--	Product_Type,
--	Platform,
--	Month_Updated,
--	Rep_Month,
--	industry,
--	Time_Period,
--	mrch_dba_id,
--	mrch_st_cd_enr,
--	trim(mrch_pstl_cd) as mrch_pstl_cd,
--	mrch_nm_dscrptr,
--	CHANNEL ,
--	merch_desc_3,
--	Time_of_day,
--	Time_Of_Week,
--	Issuer_region,
--	Merchant_region,
--	str_class_final,
--	str_affliation,
--	str_class, 	
--	hotel_Class,
--	mrg_flg_2,	
--	sum(us_tran_amt) as us_tran_amt,
--	count(*) as txn_cnt
--from jg_Mrch_Hotels_osv_txn_data_full
--group by 
--	acct_num_ods,
--	Product_Type,
--	Platform,
--	Month_Updated,
--	Rep_Month,
--	industry,
--	Time_Period,
--	mrch_dba_id,
--	mrch_st_cd_enr,
--	trim(mrch_pstl_cd),
--	mrch_nm_dscrptr,
--	CHANNEL,
--	merch_desc_3,
--	Time_of_day,
--	Time_Of_Week,
--	Issuer_region,
--	Merchant_region,
--	str_class_final,
--	str_affliation,
--	str_class, 	
--	hotel_Class,
--	mrg_flg_2	
--
--	
------ Code to combine telecome and cable together;
-------------------------------------------------;
--
--	
--union all
--
--
--select
--acct_num_ods as acct_num_osv,
--	Product_Type,
--	Platform,
--	Month_Updated,
--	Rep_Month,
--	"Telecom and Cable" as industry,
--	Time_Period,
--	mrch_dba_id,
--	mrch_st_cd_enr,
--	trim(mrch_pstl_cd) as mrch_pstl_cd,
--	mrch_nm_dscrptr,
--	CHANNEL ,
--	merch_desc_3,
--	Time_of_day,
--	Time_Of_Week,
--	Issuer_region,
--	Merchant_region,
--	str_class_final,
--	str_affliation,
--	str_class, 	
--	hotel_Class,
--	mrg_flg_2,	
--	sum(us_tran_amt) as us_tran_amt,
--	count(*) as txn_cnt
--from jg_Mrch_Hotels_osv_txn_data_full
--where industry in ('Telecommunication','Cable')
--group by 
--	acct_num_ods,
--	Product_Type,
--	Platform,
--	Month_Updated,
--	Rep_Month,
--	Time_Period,
--	mrch_dba_id,
--	mrch_st_cd_enr,
--	trim(mrch_pstl_cd),
--	mrch_nm_dscrptr,
--	CHANNEL,
--	merch_desc_3,
--	Time_of_day,
--	Time_Of_Week,
--	Issuer_region,
--	Merchant_region,	
--	str_class_final,
--	str_affliation,
--	str_class, 	
--	hotel_Class,
--	mrg_flg_2	
--)tmp;
--
--
--
--
--
--
--------------------------------------------------------------------------------;
------card linkage and corp mapping;
--------------------------------------------------------------------------------;	
--	
--drop table if exists jg_zip_msa_Mrch_Hotels_mcc_acct_lvl_smry2 purge;
--CREATE TABLE jg_zip_msa_Mrch_Hotels_mcc_acct_lvl_smry2 STORED AS SEQUENCEFILE as select tmp1.*,
--
--
--
--
--
--case 
--when upper(trim(mrch_dba_nm1)) like '%ADAMS MARK%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AMERICAN INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AMERICANA HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ARIA RESORT AND CASINO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ATLANTIS CASINO RESORT%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BALLY\'S HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BEAU RIVAGE HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BELLAGIO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BELVEDERE HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BILTMORE HOTEL & SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BREAKERS HOTEL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BUFFALO BILL\'S HOTEL & CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CALIFORNIA HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CARLTON HOTELS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CEDAR LODGE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CHATEAU ELAN WINERY AND RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CIRCUS CIRCUS HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CITY LODGE HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CLUBCORP/CLUBRESORTS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%C\'MON INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COAST HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COMPASS COVE RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CONCORDE HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CONTINENTAL INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CROSSLAND ECONOMY STUDIOS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DAYS HOTEL%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ECONOMY INNS OF AMERICA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EDGEWATER%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EXCALIBUR HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FONTAINEBLEAU RESORTS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FRIENDSHIP INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GATEWAY INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GOLD STRIKE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GOLDEN NUGGET%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GRAND HOTEL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GREAT WOLF RESORTS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GREENBRIAR RESORTS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HARRAH\'S HOTEL AND CASINO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HAWK\'S CAY RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HORSESHOE CASINO & HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOSPITALITY INNS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOTEL DEL CORONADO%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOTEL PENNSYLVANIA%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HUSA HOTELS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%IMPERIAL LONDON HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%INDEPENDENT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--
--when upper(trim(mrch_dba_nm1)) like '%KALAHARI RESORT & CONVENTION CENTER%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LEGO%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LONG BAY RESORT%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LUXOR HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MAIN ST STATION HTL/CAS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MANDALAY BAY RESORT%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MGM GRAND HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MONTE CARLO HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NEMACOLIN WOODLANDS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NEW YORK-NEW YORK HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ALA MOANA HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AT&T%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BACARA RESORT & SPA HOTEL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BRIDGE STREET%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BROADMOOR HOTEL%'  then 'LUXURY CLASS' 
--when upper(trim(mrch_dba_nm1)) like '%CAESAR\'S HOTEL AND CASINO%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CALLAWAY GARDENS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CANYON RANCH%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CARIBE ROYALE SUITES ORLANDO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COLORADO BELLE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DESTIN RESORTS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FLAMINGO HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FOXWOOD GRAND PEQUOT TOWER%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GEORGE WASHINGTON HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HILTON INTERNATIONAL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%JOHN ASCUAGA\'S NUGGET%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%KAPALUA VILLAS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MAMMOTH MOUNTAIN SKI%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MENGER HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%METROPOLE HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MIDWAY MOTOR LODGE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MIRAGE HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MIRAVAL RESORT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OCEAN REEF RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER ELDORADO HOTEL AND CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER FREMONT HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER GRAND CASINO HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER JOURNEY\'S END MOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER OXFORD SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER PEPPERMILL HOTEL CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER RIVERSIDE RESORT HOTEL&CA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER ROYAL LAHAINA RESORTS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER SEA PINES RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER STRATOSPHERE HOTEL AND CA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER TRIBECA GRAND HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER VDARA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OXFORD SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PARK SHORE WAIKIKI HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PENINSULA HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PEPPERMILL HOTEL CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PGA NATIONAL RESORT & SPA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PLAZA HOTEL CASINO & CASINO%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PURDUE MEMORIAL UNION%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RAILROAD PASS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RED ROCK CASINO RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RELAX INNS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RIO SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RIVERSIDE RESORT HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ROSEN HOTELS AND RESORTS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ROYAL HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SANDMAN HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SANDS RESORT%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SCOTTSDALE PLAZA RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SEA MIST RESORT%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SEAPORT HOTEL & WORLD TRADE CENTER%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SILVER LEGACY HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SMUGGLERS NOTCH RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%STOWE MOUNTAIN RESORT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%STRATOSPHERE HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%STRATTON%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SUNSET STATION HOTEL & CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE BENSON HOTEL%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE BEVERLY HILLS HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE COLONNADE HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE COSMOPOLITAN OF LAS VEGAS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE HOMESTEAD%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE HOTEL CAPTAIN COOK%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE HOTEL HERSHEY%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE INN AT SPANISH BAY%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE JUPITER BEACH RESORT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE LODGE AT PEBBLE BEACH%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE NATIONAL HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NEW YORK%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE PARAMOUNT HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE PFISTER HOTEL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TOWN & COUNTRY RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TRAVELERS INNS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TREASURE ISLAND HOTEL & CASINO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TROPICANA RESORT & CASINO%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TRUMP NATIONAL DORAL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TURTLE BAY RESORT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VAGABOND HOTELS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VENETIAN RESORT HOTEL CASINO%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VIRGIN RIVER HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WASHINGTON DUKE INN%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WESTGATE%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WHISKEY PETE\'S HOTEL & CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WYNN LAS VEGAS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%YOSEMITE VIEW LODGE%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SAM\'S TOWN HOTEL AND CASINO%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER KAUAI COCONUT BEACH RESOR%' then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OTHER MASTERS ECONOMY INNS%' then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PEABODY HOTELS%' then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SHONEY\'S INN%' then 'UPPER UPSCALE & UPSCALE CLASS' 
--when upper(trim(mrch_dba_nm1)) like '%THE WARWICK%' then 'UPPER UPSCALE & UPSCALE CLASS'
--
--
--when upper(trim(mrch_dba_nm1)) like '%A VICTORY%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NOVOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SOFITEL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ACE HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AFFINIA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AFFORDABLE SUITES OF AMERICA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AKA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%3 PALMS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AMERICA`S BEST INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AMERICA`S BEST SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BUDGETEL%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COUNTRY HEARTH INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%JAMESON INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AMERICINN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AQUA HOTELS & RESORTS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LITE HOTELS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MONOGRAM HOTEL COLLECTION%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AYRES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BELMOND HOTELS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BEST WESTERN'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BEST WESTERN PLUS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BEST WESTERN PREMIER%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BOARDERS INN & SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GUESTHOUSE INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SETTLE INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BOULDERS INN & SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BUDGET HOST%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BUDGET SUITES OF AMERICA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CABOT LODGE%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CANAD INN%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CAPELLA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COUNTRY INN & SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PARK INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PARK PLAZA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RADISSON HOTEL'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%REGENT INTERNATIONAL HOTELS'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RADISSON BLU%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CENTERSTONE HOTELS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CHASE SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ASCEND COLLECTION%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CAMBRIA SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CLARION%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COMFORT INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COMFORT SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ECONO LODGE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ECONOLODGE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MAINSTAY%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--
--when upper(trim(mrch_dba_nm1)) like '%QUALITY INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RODEWAY INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SLEEP INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SUBURBAN EXTENDED STAY%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CITIZENM HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CLUB MED%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CLUB QUARTERS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CLUBHOUSE%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COAST HOTELS & RESORTS USA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COBBLESTONE%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CRESTWOOD SUITES%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CRYSTAL INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DISNEY%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DOLCE HOTELS & RESORTS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DORCHESTER COLLECTION%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DOWNTOWNER INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DRURY INN'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DRURY INN & SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DRURY LODGE%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DRURY PLAZA HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DRURY SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PEAR TREE INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DUSITD2%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CROSSLAND SUITES%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EXTENDED STAY AMERICA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EXTENDED STAY DELUXE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%E-Z 8%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FAIRBRIDGE INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LOYALTY INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FAIRMONT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SWISSOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FAMILY INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FIRMDALE%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FOUR SEASONS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MOTEL 6%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%STUDIO 6%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GOOD NITE INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GRAND AMERICA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CROSSINGS BY GRANDSTAY%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GRANDSTAY RESIDENTIAL SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GREAT WESTERN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GREAT WOLF LODGE%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GOLDEN TULIP%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CAMINO REAL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MELIA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HARD ROCK%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CONRAD%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CURIO COLLECTION%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DOUBLETREE'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DOUBLETREE HOTEL'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DOUBLETREE CLUB%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EMBASSY SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EMBASSY HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HAMPTON INN'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HAMPTON INN & SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HILTON'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HILTON GARDEN INN%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HILTON GRAND VACATIONS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOME2 SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOMEWOOD SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WALDORF ASTORIA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOMEGATE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOME-TOWNE SUITES%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ANDAZ%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GRAND HYATT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like 'HYATT'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HYATT HOUSE%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HYATT PLACE%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PARK HYATT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%INNSUITES HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CANDLEWOOD SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%CROWNE PLAZA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EVEN HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOLIDAY INN'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOLIDAY INN EXPRESS%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOLIDAY INN SELECT%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOTEL INDIGO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%INTERCONTINENTAL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%STAYBRIDGE SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%INTOWN SUITES%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ISLE OF CAPRI%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOTEL NIKKO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%JOIE DE VIVRE%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%JOLLY%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%KEY WEST INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%KIMPTON%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LAKEVIEW%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LANGHAM%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LARKSPUR LANDING%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LEES INN OF AMERICA%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LEGACY VACATION CLUB%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LOEWS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LA QUINTA INNS & SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MANDARIN ORIENTAL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AC HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AUTOGRAPH COLLECTION%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%COURTYARD BY MARRIOTT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%EDITION%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FAIRFIELD%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%GAYLORD%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%JW MARRIOTT%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MARRIOTT'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MARRIOTT CONFERENCE CENTER%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RENAISSANCE HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RESIDENCE INN%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RITZ-CARLTON%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SPRINGHILL SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TOWNEPLACE SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MASTER HOSTS INNS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MASTERS INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MILLENNIUM%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MIYAKO%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MONTAGE%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NATIONAL 9%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NEW OTANI%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ROSEWOOD%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NYLO HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%O`CALLAGHAN%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OAK TREE INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MOKARA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OMNI%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OHANA%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%OUTRIGGER%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PALACE INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PAN PACIFIC%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PASSPORT INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PESTANA%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PHOENIX INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PRINCE HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE PRINCE%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RED CARNATION%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RED CARPET INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RED LION%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ASTON HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ROAD STAR INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ROCKRESORTS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RODE INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ROOM MATE%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SAVANNAH SUITES%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SCOTTISH INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SELECT INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SHILO INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SILVER CLOUD%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SIXTY HOTELS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SONESTA ES SUITES%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SONESTA HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ST. GILES HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%STARHOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ALOFT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ELEMENT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%FOUR POINTS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LE MERIDIEN%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LUXURY COLLECTION%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SHERATON HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SHERATON%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ST REGIS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%ST. REGIS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%W HOTEL%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WESTIN%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SUN SUITES%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DOYLE COLLECTION%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TAJ%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THE PENINSULA%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%THOMPSON HOTELS%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TRUMP HOTEL COLLECTION%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VAGABOND INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VALUE PLACE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%AMERICAS BEST VALUE INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%LEXINGTON%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VICEROY%'  then 'LUXURY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%VISTA%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WARWICK HOTEL%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WESTMARK%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RED ROOF INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SHELL VACATIONS CLUB%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%PLANET HOLLYWOOD%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WYNDHAM VACATION RESORT%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%BAYMONT%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DAYS INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%DREAM HOTELS%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HAWTHORN SUITES BY WYNDHAM%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOWARD JOHNSON'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%HOWARD JOHNSON EXPRESS%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%KNIGHTS INN%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%MICROTEL INN & SUITES BY WYNDHAM%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%NIGHT HOTEL%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RAMADA INN'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%RAMADA PLAZA%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SUPER 8%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TRAVELODGE%'  then 'ECONOMY CLASS'
--when upper(trim(mrch_dba_nm1)) like '%TRYP BY WYNDHAM%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WINGATE BY WYNDHAM%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WINGATE INN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WYNDHAM'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WYNDHAM HOTELS'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%WYNDHAM GARDEN%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%SUMMERFIELD SUITES%'  then 'UPPER UPSCALE & UPSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like '%XANTERRA%'  then 'UPPER MIDSCALE & MIDSCALE CLASS'
--when upper(trim(mrch_dba_nm1)) like 'HOTEL IBIS' then 'ECONOMY CLASS' 
--
--	 else hotel_Class end as hotel_industry
--
--
--
--from (select
--	a.*,
--	b.mrch_dba_nm,
--	b.corporation,
--	case when (d.zip is null or d.msa_name is null or trim(d.msa_name) in ('')) then "NON MSA" else trim(d.msa_name) end as msa_name,
--	d.msa_id,
--	d.lat,
--	d.lng,
--	case when d.zip is null then trim(a.mrch_st_cd_enr) else trim(d.state) end as state,
--
--	case when d.zip is null and a.mrch_st_cd_enr in ('IA','IL','IN','KS','MI','MN','MO','MT','ND','NE','OH','SD','WI') then "Midwest"
--		when d.zip is null and a.mrch_st_cd_enr in ('CT','MA','ME','NH','NJ','NY','PA','RI','VT') then "Northeast"
--		when d.zip is null and a.mrch_st_cd_enr in ('AL','AR','DC','DE','FL','GA','KY','LA','MD','MS','NC','OK','SC','TN','TX','VA','WV') then "South"
--		when d.zip is null and a.mrch_st_cd_enr in ('AZ','CA','CO','ID','NM','NV','OR','UT','WA','WY') then "West"
--		when d.zip is null and a.mrch_st_cd_enr in ('AK','HI') then "West AK-HI"	
--		when d.zip is null then "Other" else trim(d.region) end as region,
--		
--	case when substr(trim(b.corporation),1,5) in ('OTHER') and e.mrch_nm_dscrptr is not null then trim(e.gmr_enterprise_name)
--		when substr(trim(b.corporation),1,5) in ('OTHER') and e.mrch_nm_dscrptr is null and a.merch_desc_3 is not null then a.merch_desc_3
--			else trim(b.corporation) end as corporation_zip_lvl,
--			
--	case when c.acct_num is null then trim(a.acct_num_osv) else trim(c.grp_id) end as acct_num_ods,
--	
----------fore hotel classification purpose;
--
--case  when trim(mrch_pstl_cd)='05254' and trim(a.mrch_nm_dscrptr)='THE EQUINOX RESORT AND SP' then 'THE LUXURY COLLECTION' 
--     when upper(trim(b.corporation)) like '%HYATT HOTELS%' and upper(trim(str_affliation)) like '%INDEPENDENT%' then 'Hyatt'
--
--	when upper(trim(b.mrch_dba_nm)) like '%FAIRFIELD%' then 'FAIRFIELD SUITES'
--     when upper(trim(b.mrch_dba_nm))  like '%GAYLORD%' then 'GAYLORD'
--     ---Stephanie changes JW Marriott
--     when upper(trim(b.corporation)) like '%MARRIOTT CORP%' and upper(trim(str_affliation))='JW MARRIOTT'  then 'JW MARRIOTT'
--     when upper(trim(b.corporation)) like '%MARRIOTT CORP%' and upper(trim(b.mrch_dba_nm)) like 'JW MARRIOTT' and  (upper(trim(str_affliation)) not in ('JW MARRIOTT') or upper(trim(str_affliation)) is null)
--                                          and mrch_pstl_cd not in ('55425', '78701','90401', '33131') then 'OTHER MARRIOTT'
--     --Autograph
--     when upper(trim(b.mrch_dba_nm)) like '%AUTOGRAPH%' and (upper(trim(str_affliation)) in ('INDEPENDENT') or upper(trim(str_affliation)) is null) and trim(mrch_pstl_cd) in ('94102','32301','27514','37203','33139','10036','52801','10016','48126','32819','33139','74119','60610','34747','32084','32801','10017','77002','87571','31401',
--     '80202','30303','31822','89109','55401','28803','31401','70112','81630','78256','64106','91932','67202','92262','33606','73103','44115','99201','33180','64112','92352','85016','35223','95814','95060','33037','33040','96743','70801','70130','04101','21401','02210','49770','29401','84060','99201','20036','60611','84068','60654') then 'Autograph Collection'
--     when upper(trim(str_affliation))='AUTOGRAPH COLLECTION' then 'Autograph Collection'
--     when upper(trim(b.mrch_dba_nm)) like '%AUTOGRAPH%' and (upper(trim(str_affliation)) in ('INDEPENDENT') or upper(trim(str_affliation)) is null) and trim(mrch_pstl_cd) not in 
--     ('94102','32301','27514','37203','33139','10036','52801','10016','48126','32819','33139','74119','60610','34747','32084','32801','10017','77002','87571','31401','80202','30303','31822','89109','55401','28803','31401','70112','81630','78256','64106','91932','67202','92262','33606','73103','44115','99201','33180','64112','92352','85016','35223','95814','95060','33037','33040','96743','70801','04101','21401','02210','49770','29401','84060','99201','20036','60611','84068','60654') 
--     then 'OTHER MARRIOTT'
--     when upper(trim(b.mrch_dba_nm)) like '%AUTOGRAPH%' and (upper(trim(str_affliation)) in ('INDEPENDENT') or upper(trim(str_affliation)) is null) and trim(mrch_pstl_cd) not in 
--     ('70130') 
--     then 'MARRIOTT'
--     --Starwood Luxury Brand
--     when upper(trim(b.corporation)) like '%STARWOOD%' and (upper(trim(str_affliation)) not in ('LUXURY COLLECTION') or upper(trim(str_affliation)) is null ) then upper(trim(mrch_dba_nm))
--     when upper(trim(str_affliation)) like '%LUXURY COLLECTION%' then 'THE LUXURY COLLECTION'
--     when (upper(trim(a.mrch_nm_dscrptr)) in ('SLS HOTEL BEVERLY HILLS', 'SLS BEVERLY HILLS', 'EQUINOX GOLF RESORT AND S','THE EQUINOX RESORT AND SP','ST ANTHONY HOTEL') or upper(trim(a.mrch_nm_dscrptr)) like 'THE NINE%'  or upper(trim(a.mrch_nm_dscrptr)) like 'LA POSADA%'  or upper(trim(a.mrch_nm_dscrptr)) like 'THE GWEN%') and trim(mrch_pstl_cd) in ('90048','05254','97204','87501','60611','78205') then 'THE LUXURY COLLECTION'
--     --when upper(trim(b.corporation))='OTHER' and upper(trim(a.mrch_nm_dscrptr))='THE EQUINOX RESORT AND SP' and upper(trim(str_affliation)) like '%LUXURY COLLECTION%' then 'THE LUXURY COLLECTION'
--     --Hyatt
--     when (upper(trim(str_affliation)) like '%HYATT%') or (upper(trim(b.corporation)) like '%HYATT%' and str_affliation is not null ) then str_affliation
--     when upper(trim(str_affliation)) like '%HYATT%' then str_affliation
--     when upper(trim(b.corporation)) like 'HYATT HOTELS%' and str_affliation is null and trim(mrch_pstl_cd) in ('92101', '10005') then 'Andaz'
--     when upper(trim(b.corporation)) like 'HYATT HOTELS%' and upper(trim(a.mrch_nm_dscrptr)) like 'ANDAZ NAPA HOTEL%' then 'Andaz'
--     when upper(trim(b.corporation)) like '%HYATT%' and str_affliation is null and ( trim(mrch_pstl_cd) not in ('92101', '10005') or upper(trim(a.mrch_nm_dscrptr)) not like 'ANDAZ NAPA HOTEL%' ) then upper(trim(mrch_dba_nm)) 
--     --HILTON
--     when upper(trim(b.corporation)) like 'OTHER' and upper(trim(mrch_dba_nm))='OTHER HILTON CONRAD' then 'HILTON CONRAD'
--     when upper(trim(mrch_dba_nm)) like 'WALDORF ASTORIA HOTELS & RESORTS%' and trim(mrch_pstl_cd) in ('33434','30144','32250','14303','20601','33139','63005','78041','33139') then 'OTHER HILTON'
--     ---Stephanie changes ends
--	 --when upper(trim(b.corporation)) like '%STARWOOD%' then upper(trim(mrch_dba_nm))
--	 --when upper(trim(b.corporation)) like '%HYATT%' and str_affliation is not null then str_affliation
--	 --when upper(trim(b.corporation)) like '%HYATT%' and str_affliation is null then upper(trim(mrch_dba_nm))
--	 when upper(trim(b.corporation)) like '% INTERCONTINENTAL HOTELS GROUP PLC%' and str_affliation is null then upper(trim(mrch_dba_nm))
--	 when upper(trim(b.corporation)) like '% INTERCONTINENTAL HOTELS GROUP PLC%' and str_affliation is not null then str_affliation
--	 when upper(trim(b.corporation)) like '%MARRIOTT CORP' and mrch_pstl_cd='96707' and upper(trim(str_affliation))='JW MARRIOTT' then 'JW MARRIOTT'	 
--	 when upper(trim(b.corporation)) like '%DRURY INN' and str_affliation is not null and upper(trim(str_affliation)) not in ('INDEPENDENT') then upper(trim(str_affliation))
--	 when upper(trim(b.corporation)) like '%DRURY INN' and str_affliation is not null and upper(trim(str_affliation)) in ('INDEPENDENT') then upper(trim(mrch_dba_nm))
--	 when upper(trim(b.corporation)) like '%DRURY INN' and str_affliation is null then upper(trim(mrch_dba_nm))
--     
--	 when upper(trim(b.corporation)) like 'WYNDHAM WORLDWIDE' and upper(trim(mrch_dba_nm))='WYNDHAM HOTELS' and str_affliation is null then upper(trim(mrch_dba_nm))
--	 when upper(trim(b.corporation)) like 'WYNDHAM WORLDWIDE' and upper(trim(mrch_dba_nm))='WYNDHAM HOTELS' and upper(trim(str_affliation))='WYNDHAM' then 'WYNDHAM HOTELS'
--	 when upper(trim(b.corporation)) like 'WYNDHAM WORLDWIDE' and upper(trim(mrch_dba_nm))='WYNDHAM HOTELS' and upper(trim(str_affliation))='INDEPENDENT' then upper(trim(mrch_dba_nm))
--   when upper(trim(b.corporation)) like 'WYNDHAM WORLDWIDE' and upper(trim(mrch_dba_nm))='WYNDHAM HOTELS' and upper(trim(str_affliation))='SUPER 8' then 'SUPER 8 MOTEL'
--	 
--    	 
--	 when upper(trim(b.corporation)) like 'WYNDHAM WORLDWIDE' and upper(trim(mrch_dba_nm))='WYNDHAM HOTELS' and
--	          (str_affliation is not null) and (upper(trim(str_affliation))  not in ('WYNDHAM','INDEPENDENT')) then str_affliation
--	 when upper(trim(b.corporation)) like 'HAWTHORNE SUITES' then 'HAWTHORN SUITES BY WYNDHAM'
--	 
--	 when upper(trim(mrch_dba_nm)) in ('MICROTEL', 'OTHER MICROTEL INNS & SUITES') then 'MICROTEL INN & SUITES BY WYNDHAM'
--	 when upper(trim(mrch_dba_nm)) in ('HOMESTEAD STUDIO SUITES') then 'EXTENDED STAY AMERICA'
--	 when upper(trim(mrch_dba_nm)) in ('LA QUINTA MOTOR INN') then 'LA QUINTA INNS & SUITES' 
--     when upper(trim(mrch_dba_nm)) in ('OTHER COUNTRY INN BY CARLSON') then 'COUNTRY INN & SUITES'
--     when upper(trim(mrch_dba_nm)) in ('INTER-CONTINENTAL') then 'INTERCONTINENTAL'	 
--	 when upper(trim(b.corporation)) like 'OMNI' and upper(trim(str_affliation)) is not null  then str_affliation
--	 when upper(trim(b.corporation)) like 'OMNI' and upper(trim(str_affliation)) is null  then upper(trim(mrch_dba_nm))
--	 when  upper(trim(str_affliation))='XANTERRA'  then 'XANTERRA'
--     when upper(trim(b.corporation)) like '%ACCOR HOTELS%' and upper(trim(mrch_dba_nm))='HOTEL IBIS' and str_affliation is null then upper(trim(mrch_dba_nm)) 
--	 else b.mrch_dba_nm end as mrch_dba_nm1	
--	
--from jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry1 a left outer join default.JG_DBA_TO_CORP_MAPPING b
--	on a.mrch_dba_id=b.mrch_dba_id
--left outer join default.acn_card_link_201605 c
--	on trim(a.acct_num_osv)=trim(c.acct_num)
--left outer join default.jg_zip_to_msa_melisa d
--	on a.mrch_pstl_cd=d.zip
--left outer join default.lkup_mrch_w_gmr_dedup e 
--	on trim(a.mrch_nm_dscrptr)=trim(e.mrch_nm_dscrptr)
--left outer join jg_optout_accts_list f
--on trim(a.acct_num_osv)=trim(f.acct_num)
--where f.acct_num is null) tmp1
--;
	


drop table if exists jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry3 purge;
create table jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry3 STORED AS SEQUENCEFILE as select 
	acct_num_ods,
	Product_Type,
	Platform,
	month_updated,
	Rep_Month,
	industry,
	time_period,
	CHANNEL,
	mrch_dba_id,
	mrch_pstl_cd,
	mrch_dba_nm,
	
------ below change is done based on AEs mail---
	
	case when mrch_dba_id in (3885) then "CHARTER COMMUNICATIONS"
		when mrch_dba_id in (37335) then "DISH NETWORK"
		when mrch_dba_id in (37099) then "DISH NETWORK"
		when mrch_dba_id in (963) then "DISH NETWORK"
		when mrch_dba_id in (1047) then "DISH NETWORK"
		when mrch_dba_id in (4092) then "SPRINT NEXTEL CORPORATION"
		when mrch_dba_id in (13311) then "VERIZON"
		when mrch_dba_id in (1467) then "HERTZ CORPORATION"
		when upper(trim(corporation)) in ('STARWOOD') then "MARRIOTT CORP"
		else trim(corporation) end as corporation,
	msa_name,
	lat,
	lng,
	state,
	region,
	case when mrch_dba_id in (3885) then "CHARTER COMMUNICATIONS"
		when mrch_dba_id in (37335) then "DISH NETWORK"
		when mrch_dba_id in (37099) then "DISH NETWORK"
		when mrch_dba_id in (963) then "DISH NETWORK"
		when mrch_dba_id in (1047) then "DISH NETWORK"
		when mrch_dba_id in (4092) then "SPRINT NEXTEL CORPORATION"
		when mrch_dba_id in (13311) then "VERIZON"
		when mrch_dba_id in (1467) then "HERTZ CORPORATION"
		when upper(trim(corporation_zip_lvl)) in ('STARWOOD') then "MARRIOTT CORP"
		else trim(corporation_zip_lvl)		end as corporation_zip_lvl,
	Time_of_day,
	Time_Of_Week,
	Issuer_region,
	Merchant_region,
	sum(us_tran_amt) as us_tran_amt,
	sum(txn_cnt) as txn_cnt
from (select acct_num_ods,
				Product_Type,
				Platform,
				month_updated,
				Rep_Month,
				case when industry in ('Hotels') then concat("Hotels - ",upper(hotel_industry)) else industry end as industry,
				time_period,
				CHANNEL,
				mrch_dba_id,
				mrch_pstl_cd,
				mrch_dba_nm,
				corporation,
				msa_name,
				lat,
				lng,
				state,
				region,
				corporation_zip_lvl,
				Time_of_day,
				Time_Of_Week,
				Issuer_region,
				Merchant_region,
				us_tran_amt,
				txn_cnt,
				mrch_dba_nm1 
			from jg_zip_msa_Mrch_Hotels_mcc_acct_lvl_smry2
---------------- below part id for all Hotels as one idustry-----------------------	

				union all
				
				select acct_num_ods,
				Product_Type,
				Platform,
				month_updated,
				Rep_Month,
				industry,
				time_period,
				CHANNEL,
				mrch_dba_id,
				mrch_pstl_cd,
				mrch_dba_nm,
				corporation,
				msa_name,
				lat,
				lng,
				state,
				region,
				corporation_zip_lvl,
				Time_of_day,
				Time_Of_Week,
				Issuer_region,
				Merchant_region,
				us_tran_amt,
				txn_cnt,
				mrch_dba_nm1 
			from jg_zip_msa_Mrch_Hotels_mcc_acct_lvl_smry2
				where industry in ('Hotels')
	) tmp
	
 where ((substr(trim(industry),1,6) not in ('Hotels')) or (substr(trim(industry),1,6) in ('Hotels') and  trim(mrch_dba_nm1) not in ('ARAMARK','BLUEGREEN VACATIONS','GOOGLE WALLET NFC','GOOGLE WALLET PSP',
		'GOOGLE CHECKOUT','HILTON RESTAURANT','INTERVAL TRAVEL','INTRACOASTAL REALTY','INTUIT PAYMENT FACILITATION','INTUIT','LINK 2 GOV','KAISER REALTY','MARINE CORP COMMUNITY SERVICES',
		'MARRIOTT VACATION CLUB','NAVY EXCHANGE','NORTH AMERICAN BANCARD','OTHER TIMESHARES','OTHER TRAILER PARKS/CAMPGROUNDS','OUTDOOR ADVENT RIVER SPECIAL','OUTER BEACHES REALTY','PAYPAL','PRICELINE.COM',
		'PROPAY','RCI TRAVEL','SINGULAR PAYMENTS','SQUARE INC','TIMESHARE ADVENTURES','UNIQUE VACATIONS','WORLDMARK','WYNDHAM RESORT DEVELOPMENT','WYNDHAM VACATION',
		'YAPSTONE','YOSEMITE RESERVATIONS','AIRBNB GLOBAL NETWORK')))
group by acct_num_ods,
	Product_Type,
	Platform,
	month_updated,
	Rep_Month,
	industry,
	time_period,
	CHANNEL,
	mrch_dba_id,
	mrch_pstl_cd,
	mrch_dba_nm,
	case when mrch_dba_id in (3885) then "CHARTER COMMUNICATIONS"
		when mrch_dba_id in (37335) then "DISH NETWORK"
		when mrch_dba_id in (37099) then "DISH NETWORK"
		when mrch_dba_id in (963) then "DISH NETWORK"
		when mrch_dba_id in (1047) then "DISH NETWORK"
		when mrch_dba_id in (4092) then "SPRINT NEXTEL CORPORATION"
		when mrch_dba_id in (13311) then "VERIZON"
		when mrch_dba_id in (1467) then "HERTZ CORPORATION"
		when upper(trim(corporation)) in ('STARWOOD') then "MARRIOTT CORP"
		else trim(corporation) end ,
	msa_name,
	lat,
	lng,
	state,
	region,
	case when mrch_dba_id in (3885) then "CHARTER COMMUNICATIONS"
		when mrch_dba_id in (37335) then "DISH NETWORK"
		when mrch_dba_id in (37099) then "DISH NETWORK"
		when mrch_dba_id in (963) then "DISH NETWORK"
		when mrch_dba_id in (1047) then "DISH NETWORK"
		when mrch_dba_id in (4092) then "SPRINT NEXTEL CORPORATION"
		when mrch_dba_id in (13311) then "VERIZON"
		when mrch_dba_id in (1467) then "HERTZ CORPORATION"
		when upper(trim(corporation_zip_lvl)) in ('STARWOOD') then "MARRIOTT CORP"
		else trim(corporation_zip_lvl) end,
	Time_of_day,
	Time_Of_Week,
	Issuer_region,
	Merchant_region

;


--------------------------------------------------------------------------------;
------transaction frequency L/M/H flagging;
--------------------------------------------------------------------------------;		
drop table if exists jg_tab_quintile11_Hotels purge;
create table jg_tab_quintile11_Hotels STORED AS SEQUENCEFILE as select
        acct_num_ods
		,industry
		,Time_Period
        ,sum(us_tran_amt) as us_tran_amt
        ,sum(txn_cnt) as txn_cnt
        ,sum(us_tran_amt)/sum(txn_cnt) as tkt_size
from jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry3
group by acct_num_ods
		,industry
		,Time_Period;
			
			
-------------------------------------------------------;
------code used to find the H/M/L bountry for Tran Freq;		
-------------------------------------------------------;
--drop table if exists jg_tab_quintile111_Hotels;
--create table jg_tab_quintile111_Hotels STORED AS SEQUENCEFILE as select         
--		Time_Period
--		,industry
--		,case when txn_cnt < 20 then floor(txn_cnt)
--				when txn_cnt < 100 then floor(txn_cnt/5)*5
--				when txn_cnt < 200 then floor(txn_cnt/10)*10
--				when txn_cnt < 500 then floor(txn_cnt/25)*25
--				when txn_cnt < 1000 then floor(txn_cnt/100)*100
--				when txn_cnt < 10000 then floor(txn_cnt/1000)*1000				
--				when txn_cnt < 100000 then floor(txn_cnt/10000)*10000			
--				when txn_cnt < 1000000 then floor(txn_cnt/100000)*100000
--				else 1000000 end as Segments2,
--				sum(us_tran_amt) as us_tran_amt,
--				sum(txn_cnt) as txn_cnt,
--		count(distinct acct_num_ods) as crd_cnt
--from jg_tab_quintile11_Hotels 
--group by 	Time_Period,
--			industry
--		,case when txn_cnt < 20 then floor(txn_cnt)
--				when txn_cnt < 100 then floor(txn_cnt/5)*5
--				when txn_cnt < 200 then floor(txn_cnt/10)*10
--				when txn_cnt < 500 then floor(txn_cnt/25)*25
--				when txn_cnt < 1000 then floor(txn_cnt/100)*100
--				when txn_cnt < 10000 then floor(txn_cnt/1000)*1000				
--				when txn_cnt < 100000 then floor(txn_cnt/10000)*10000			
--				when txn_cnt < 1000000 then floor(txn_cnt/100000)*100000
--				else 1000000 end
--;		
--
--drop table if exists jg_tab_quintile112_Hotels;
--create table jg_tab_quintile112_Hotels STORED AS SEQUENCEFILE as select         
--		Time_Period
--		,industry
--		,case when tkt_size < 10 then floor(tkt_size)
--				when tkt_size < 100 then floor(tkt_size/5)*5
--				when tkt_size < 200 then floor(tkt_size/10)*10
--				when tkt_size < 500 then floor(tkt_size/25)*25
--				when tkt_size < 1000 then floor(tkt_size/100)*100
--				when tkt_size < 10000 then floor(tkt_size/1000)*1000				
--				when tkt_size < 100000 then floor(tkt_size/10000)*10000			
--				when tkt_size < 1000000 then floor(tkt_size/100000)*100000
--				else 1000000 end as Segments,
--				sum(us_tran_amt) as us_tran_amt,
--				sum(txn_cnt) as txn_cnt,
--		count(distinct acct_num_ods) as crd_cnt
--from jg_tab_quintile11_Hotels 
--group by 	Time_Period,
--			industry,
--			case when tkt_size < 10 then floor(tkt_size)
--				when tkt_size < 100 then floor(tkt_size/5)*5
--				when tkt_size < 200 then floor(tkt_size/10)*10
--				when tkt_size < 500 then floor(tkt_size/25)*25
--				when tkt_size < 1000 then floor(tkt_size/100)*100
--				when tkt_size < 10000 then floor(tkt_size/1000)*1000				
--				when tkt_size < 100000 then floor(tkt_size/10000)*10000			
--				when tkt_size < 1000000 then floor(tkt_size/100000)*100000
--				else 1000000 end
--;	







drop table if exists  jg_Mrch_Hotels_mcc_acct_lvl_smry purge;
create table jg_Mrch_Hotels_mcc_acct_lvl_smry STORED AS SEQUENCEFILE as select a.*,

	case when a.industry in ('Cruise Ships') then Issuer_region 
	
		when a.industry in ('AFDs a.industry') and b.txn_cnt < 18 then "c.LOW"
		when a.industry in ('AFDs a.industry') and b.txn_cnt < 48 then "b.MEDIUM"
		when a.industry in ('AFDs a.industry') and b.txn_cnt >=  48 then "a.HIGH" 
		
		when a.industry in ('Service Stations a.industry') and b.txn_cnt < 13 then "c.LOW"
		when a.industry in ('Service Stations a.industry') and b.txn_cnt < 48 then "b.MEDIUM"
		when a.industry in ('Service Stations a.industry') and b.txn_cnt >= 48 then "a.HIGH" 
		
		when a.industry in ('Telecom and Cable') and b.tkt_size < 50 then "c.LOW"
		when a.industry in ('Telecom and Cable') and b.tkt_size < 100 then "b.MEDIUM"
		when a.industry in ('Telecom and Cable') and b.tkt_size >= 100 then "a.HIGH"  	
		
		when a.industry in ('Telecommunication') and b.tkt_size < 100 then "c.LOW"
		when a.industry in ('Telecommunication') and b.tkt_size < 200 then "b.MEDIUM"
		when a.industry in ('Telecommunication') and b.tkt_size >= 200 then "a.HIGH" 

		when a.industry in ('Cable') and b.tkt_size < 100 then "c.LOW"
		when a.industry in ('Cable') and b.tkt_size < 160 then "b.MEDIUM"
		when a.industry in ('Cable') and b.tkt_size >= 160 then "a.HIGH" 	
		
		when a.industry in ('Home Improvement') and b.tkt_size < 25 then "c.LOW"
		when a.industry in ('Home Improvement') and b.tkt_size < 70 then "b.MEDIUM"
		when a.industry in ('Home Improvement') and b.tkt_size >= 70 then "a.HIGH" 
		
		when a.industry in ('Department Stores') and b.txn_cnt < 4 then "c.LOW"
		when a.industry in ('Department Stores') and b.txn_cnt < 15 then "b.MEDIUM"
		when a.industry in ('Department Stores') and b.txn_cnt >= 15 then "a.HIGH" 	
		
		when a.industry in ('Discount Stores') and b.tkt_size < 15 then "c.LOW"
		when a.industry in ('Discount Stores') and b.tkt_size < 40 then "b.MEDIUM"
		when a.industry in ('Discount Stores') and b.tkt_size >= 40 then "a.HIGH" 
		
		when a.industry in ('Property and Casualty Insurance') and b.tkt_size < 100 then "c.LOW"
		when a.industry in ('Property and Casualty Insurance') and b.tkt_size < 200 then "b.MEDIUM"
		when a.industry in ('Property and Casualty Insurance') and b.tkt_size >= 200 then "a.HIGH" 	
 
	
		when a.industry in ('Auto Rentals') and b.tkt_size < 100 then "c.LOW"
		when a.industry in ('Auto Rentals') and b.tkt_size < 225 then "b.MEDIUM"
		when a.industry in ('Auto Rentals') and b.tkt_size >= 225 then "a.HIGH"   
		
		when a.industry in ('Retail') and b.tkt_size < 30 then "c.LOW"
		when a.industry in ('Retail') and b.tkt_size < 65 then "b.MEDIUM"
		when a.industry in ('Retail') and b.tkt_size >= 65 then "a.HIGH" 

		when substr(trim(a.industry),1,6) in ('Hotels') and b.txn_cnt < 3 then "c.LOW"
		when substr(trim(a.industry),1,6) in ('Hotels') and b.txn_cnt < 7 then "b.MEDIUM"
		when substr(trim(a.industry),1,6) in ('Hotels') and b.txn_cnt >= 7 then "a.HIGH" 

			
		
	else "Other" end as Segments,


	case when a.industry in ('Cruise Ships') then Merchant_region
		when substr(trim(a.industry),1,6) in ('Hotels') then Time_Of_Week

		when a.industry in ('Discount Stores') and b.txn_cnt < 6 then "c.LOW"
		when a.industry in ('Discount Stores') and b.txn_cnt < 16 then "b.MEDIUM"
		when a.industry in ('Discount Stores') and b.txn_cnt >= 16 then "a.HIGH" 
	
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('01','04','07','10') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('01','04','07','10') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('01','04','07','10') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('01','04','07','10') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('02') and substr(Rep_Month,5,2) in ('01') then "Q1.Jan"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('02') and substr(Rep_Month,5,2) in ('02','03') then "Q1.Feb-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('02') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('02') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('02') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('03') and substr(Rep_Month,5,2) in ('03') then "Q1.Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('03') and substr(Rep_Month,5,2) in ('02','01') then "Q1.Jan-Feb"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('03') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('03') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('03') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('05') and substr(Rep_Month,5,2) in ('04') then "Q2.Apr"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('05') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('05') and substr(Rep_Month,5,2) in ('05','06') then "Q2.May-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('05') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('05') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('06') and substr(Rep_Month,5,2) in ('06') then "Q2.Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('06') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('06') and substr(Rep_Month,5,2) in ('05','04') then "Q2.Apr-May"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('06') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('06') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('08') and substr(Rep_Month,5,2) in ('07') then "Q3.Jul"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('08') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('08') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('08') and substr(Rep_Month,5,2) in ('08','09') then "Q3.Aug-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('08') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('09') and substr(Rep_Month,5,2) in ('09') then "Q3.Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('09') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('09') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('09') and substr(Rep_Month,5,2) in ('08','07') then "Q3.Jul-Aug"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('09') and substr(Rep_Month,5,2) in ('10','11','12') then "Q4.Oct-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('11') and substr(Rep_Month,5,2) in ('10') then "Q4.Oct"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('11') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('11') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('11') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('11') and substr(Rep_Month,5,2) in ('11','12') then "Q4.Nov-Dec"
		                    
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('12') and substr(Rep_Month,5,2) in ('12') then "Q4.Dec"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('12') and substr(Rep_Month,5,2) in ('01','02','03') then "Q1.Jan-Mar"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('12') and substr(Rep_Month,5,2) in ('04','05','06') then "Q2.Apr-Jun"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('12') and substr(Rep_Month,5,2) in ('07','08','09') then "Q3.Jul-Sep"
		when a.industry in ('Auto Rentals') and substr('20140701',5,2) in ('12') and substr(Rep_Month,5,2) in ('11','10') then "Q4.Oct-Nov"

	else a.CHANNEL end as segments2

from jg_zip_lvl_Mrch_Hotels_mcc_acct_lvl_smry3 a left outer join jg_tab_quintile11_Hotels b
on a.acct_num_ods=b.acct_num_ods
and a.Time_Period=b.Time_Period
and a.industry=b.industry;



----------------------------------------------------------------------------------;
--------table creation for using in Unix script for dba level loopping;
----------------------------------------------------------------------------------;	
------select the active DBA and corporation list;	
drop table if exists jg_dba_corp_test_Hotels purge;	
create table jg_dba_corp_test_Hotels STORED AS SEQUENCEFILE as select distinct mrch_dba_id,mrch_dba_nm,corporation,industry from jg_Mrch_Hotels_mcc_acct_lvl_smry;

----Get the Merchant of Reference MoR DBA and corporation list;	
drop table if exists jg_Hotels_MOR_details1 purge;
create table jg_Hotels_MOR_details1 (corporation string, dba_name string, dba_id int, industry string) ROW FORMAT DELIMITED fields terminated by '\t' lines terminated by '\n';
load data local inpath '/sas/prod/gpfs/sasdata/jugeorge/Projects_Codes/Merchant_DB/Market_Share/lookup_data/MOR_List_Hotels.txt' into table jg_Hotels_MOR_details1;


----export the below table. Unix script maro will pass the values	from this table for each DBA and Corporation;
drop table if exists jg_MOR_details_Hotels purge;
create table jg_MOR_details_Hotels STORED AS SEQUENCEFILE as select a.* from jg_dba_corp_test_Hotels a inner join jg_Hotels_MOR_details1 b
on a.mrch_dba_id=b.dba_id
 and a.corporation=b.corporation
 and a.industry=b.industry
--where a.industry in ('Auto Rentals')
;


