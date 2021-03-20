#  payment_workbook.R
#
#  Program to create final payment workbook for VIIP
#
# -------------------------------------------------------------------------
 
# list all the packages needed --------------------------------------------
packages <- c("tidyverse", "sparklyr", "reshape")
 
# this will check all existing packages for the packages listed above
new_packages <- packages[!(packages %in%installed.packages()[,"Package"])]
 
# if the packages are not installed, this will install them
if (length(new_packages)) {
  install.packages(new_packages, lib = "/home/cdsw/R")
}
 
# this will load all the listed packages
invisible(lapply(packages, library,character.only = T))
 
library(readxl)
PPG_Xwalk <-read_excel("/home/cdsw/R/PPG_xwalk_20191115.xlsx")
 
db_tbl <- read_excel("/home/cdsw/R/VIIP2019_DB_Tables_payment_test10_16DEC2019_qa.xlsx", sheet = "tbl_scorecard_domain_w_payment")
 
# -- Open a Spark session
config <- spark_config()
config$spark.executor.cores <- 4
config$spark.executor.memory <- "8G"
config$spark.kryoserializer.buffer.max <- "1G"
sc <- spark_connect(master = "yarn-client",
                    config = config, 
                    app_name = "matt_payment_workbook")
 
#spark_disconnect(sc)

# aggregate payment to PPG by domain;
sc_db <- copy_to(sc, db_tbl, overwrite = TRUE)
 
pay_MY18_2 <- sc_db %>%
  dplyr::select("VIIP_PPG", "domain", "payment_amount") %>%
                ,!is.na(payment_amount), VIIP_PPG %!in% c('LAAV', 'HMG')) %>%
  dplyr::distinct() %>%
  dplyr::collect()
 
head(pay_MY18_2)
 
pay_MY18_3 <- pay_MY18_2 %>%
  melt(id =(c("VIIP_PPG", "domain"))) %>%
  cast(VIIP_PPG + variable ~ domain)

head(pay_MY18_3)
head(pay_MY18_2)
class( sc_db)
 
pay_MY18_3 <-(na.omit(pay_MY18_2))%>%
melt(id=(c("VIIP_PPG", "domain", "payment_amount")))%>%
dplyr::mutate('pgv', 'encounter timeliness', 'pat_sat', 'utilization') 
 
# cleanup and transpose;
library(reshape)
mcal_mbrs_mo2 <- (na.omit(mcal_mbrs_mo)) %>%
  melt(id=(c("bp", "ppg_combined"))) %>%
  cast(ppg_combined + variable ~ bp) %>%
  dplyr::group_by(ppg_combined) %>%
  dplyr:: mutate(tot_mbrs= sum(BCSC, CFST, MCLA, na.rm = TRUE)) %>%
  dplyr::mutate(pct_cfst = CFST/tot_mbrs) %>%
  dplyr::mutate(pct_bcsc = BCSC/tot_mbrs) %>%
  dplyr::mutate(pct_mcla = MCLA/tot_mbrs)
 
tail(pay_MY18_2)
test <- xtabs('encounter timeliness',"VIIP_PPG", pay_MY18_2)
 
md <- melt(pay_MY18_2, id=(c("VIIP_PPG", "domain", "payment_amount")))
* aggregate payment to PPG by domain;
 
library(reshape)
md <- melt(mydata, id=(c("id", "time")))
 
proc sql;
create table pay_MY18_2 as
select distinct
VIIP_PPG
,domain
,payment_amount
from pay_MY18
where domain in ('pgv', 'encounter timeliness', 'pat_sat', 'utilization');
quit;
 
data pay_MY18_2; set pay_MY18_2;
if payment_amount = . then delete;
run;
 
*removing  LAAV & IPAs that have $0 payment or are in bad standing;
data pay_MY18_2; set pay_MY18_2;
if VIIP_PPG in ('LAAV', 'HMG') then delete;
run;
 
 
* transpose to one line per ppg;
 
proc transpose  data=pay_MY18_2
out= pay_MY18_2tr;
by VIIP_PPG;
id domain;
run;
 
