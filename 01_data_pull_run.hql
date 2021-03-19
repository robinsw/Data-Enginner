

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


--set mapreduce.job.queuename=highpriority;
--set mapreduce.job.queuename=vaprod;
set mapreduce.job.queuename=varegular;
--set mapreduce.job.queuename=valarge;


------------------------------------------------------------------------------;
------------------------------------------------------------------------------;
----data pull from OSVMAIN;
------------------------------------------------------------------------------;
------------------------------------------------------------------------------;
Use vadev;

drop table if exists jg_Mrch_Hotels_osv_data_full purge;
create table jg_Mrch_Hotels_osv_data_full STORED AS SEQUENCEFILE as select trim(acct_num_ods) as acct_num_ods,
acct_num_bus_id,
acct_num_ctry_cd,
acct_prod_id,
cpd_dt,
crd_acptr_id,
cs_tran_seq_id,
us_cshbk_amt,
eci_moto_cd,
mrch_catg_cd,
mrch_city_enr,
mrch_ctry_cd_enr,
mrch_dba_id,
mrch_nm_dscrptr,
mrch_geo_id,
mrch_nrmlzd_id,
mrch_pstl_cd,
mrch_st_cd_enr,
ntwrk_id,
pos_env_cd_enr,
posentry_mode_cd,
prch_dt,
prod_id_num_enr,
prod_typ_cd_enr,
rwds_pgm_id_enr,
tran_cd,
tran_gmt_dt,
tran_gmt_tm,
tran_id,
usage_cd_enr,
freq_zip_cd,
alp_acct_prod_id,
gbl_prod_id,
prod_acct_fund_srce_cd_enr,
prod_acct_fund_srce_cd_vcis,
dt,
cast(us_tran_amt as double) as us_tran_amt,
'June 2016' as Month_Updated,
case when dt < 20150701 then 'Year ended Jun15' else 'Year ended Jun16' end as Time_Period,


case when (trim(Prod_acct_fund_srce_cd_vcis) in ('C') or trim(prod_typ_cd_enr) in ('C')) then "Credit Cards"
	when (trim(Prod_acct_fund_srce_cd_vcis) in ('D') or trim(prod_typ_cd_enr) in ('D')) then "Debit Cards"
	else "Other" end as Product_Type,
	
case when (trim(ALP_Acct_Prod_ID) in ('C','A','B','D','F','I') or prod_id_num_enr in(2,3,32,34,39,40,46)) then "Consumer"
	when (trim(ALP_Acct_Prod_ID) in ('G','G1','G3') or prod_id_num_enr in(9,41,48)) then "Small Business"
	when (trim(ALP_Acct_Prod_ID) in ('K','S','S1') or prod_id_num_enr in(22,23)) then "Commercial"
	else "Other" end as Platform,
	
case when trim(posentry_mode_cd) in ('02', '03', '05', '07', '90', '91', '95') then "F2F"
     when trim(eci_moto_cd) in ('01', '02', '03','04','05','06','07','08','09') then "CNP"
     else "F2F" end as Channel

from default.osvmain

where ( dt >= 20140701 and dt < 20160701)
			and acct_num_ctry_cd=840
			and MRCH_CTRY_CD_ENR=840
			and trim(USAGE_CD_ENR) ='1'
			and substr(trim(acct_num_ods),1,1) = '4'
			and length(trim(acct_num_ods)) = 16
			and ((trim(Prod_acct_fund_srce_cd_vcis) in ('C') and ALP_Acct_Prod_ID in('C','A','B','D','G','G1','G3','I','K','S','S1')) or
					(trim(Prod_acct_fund_srce_cd_vcis) in ('D') and ALP_Acct_Prod_ID in('F','G')) or
					(trim(prod_typ_cd_enr) in ('C') and prod_id_num_enr in(34,39,40,46,9,41,48,22,23)) or
					(trim(prod_typ_cd_enr) in ('D') and prod_id_num_enr in(2,3,32,9))
					)
			and trim(TRAN_CD)="05"
			and NTWRK_ID in (1,2,3,4)
	
			and mrch_catg_cd in (3501,3502,3503,3504,3505,3506,3507,3508,3509,3510,3511,3512,3513,3514,3515,3516,3517,3518,3519,3520,3521,3522,3523,3524,3525,3526,3527,3528,3529, 3530,3531,3532,3533,3534,3535,3536,3537,3538,3539,3540,3541,3542,3543,3544,3545,3546,3548,3549,3550,3551,3552,3553,3555,3556,3557,3558,3559,3560,3561,3562,3563,3564,3565,3566,3567,3569, 3570,3571,3572,3573,3575,3576,3577,3579,3580,3581,3582,3583,3584,3585,3586,3587,3588,3589,3590,3591,3592,3595,3596,3597,3598,3599,3600,3602,3604,3606,3607,3608,3609,3610,3611,3612,3613, 3614,3615,3616,3617,3618,3619,3620,3621,3623,3625,3626,3627,3628,3629,3630,3631,3632,3633,3634,3635,3636,3637,3638,3639,3640,3641,3642,3643,3644,3645,3646,3647,3649,3650,3651,3652,3653, 3654,3655,3656,3657,3658,3659,3660,3661,3662,3663,3664,3665,3667,3668,3669,3670,3671,3672,3673,3674,3675,3676,3677,3678,3679,3680,3681,3683,3684,3685,3686,3687,3688,3689,3690,3692,3693, 3694,3695,3696,3697,3698,3699,3700,3701,3702,3703,3704,3706,3707,3708,3709,3710,3711,3712,3713,3714,3715,3716,3717,3718,3719,3720,3721,3722,3723,3724,3725,3726,3727,3728,3729,3730,3731, 3732,3733,3735,3736,3737,3738,3739,3740,3741,3742,3743,3744,3745,3746,3747,3748,3749,3750,3751,3752,3753,3754,3755,3757,3760,3761,3762,3763,3764,3765,3766,3767,3768,3769,3770,3771,3772, 3773,3774,3775,3776,3777,3778,3779,3780,3781,3782,3783,3784,3785,3786,3787,3788,3789,3790,3791,3792,3793,3794,3795,3796,3797,3798,3799,3800,3801,3802,3804,3807,3808,3811,3812,3813,3814, 3815,3816,3818,3819,3820,3822,3823,3824,3825,3826,3827,3828,3829,3830,3831,3832,3833,7011);



