#  raw_PGV
# 
#  Program to load all PGV files and add ppg and license_no
#
# -------------------------------------------------------------------------

# list all the packages needed --------------------------------------------
 
packages <- c("tidyverse", "sparklyr", "readxl", "rlang", "httr")
 
# this will check all existing packages for the packages listed above
new_packages <- packages[!(packages %in%installed.packages()[,"Package"])]
 
# if the packages are not installed, this will install them
if (length(new_packages)) {
  install.packages(new_packages, lib = "/home/cdsw/R")
}
 
# this will load all the listed packages
invisible(lapply(packages, library, character.only = T))
 
# -- Open a Spark session
config <- spark_config()
config$spark.executor.cores <- 4
config$spark.executor.memory <- "8G"
config$spark.kryoserializer.buffer.max <- "1G"
config$spark.dyamicAllication.enabled <- "true"
config$spark.kryoserializer.buffer.max <- "1024MB"
config$spark.unsafe.sorter.spill.read.ahead.enabled <- 'false'
config$spark.maxRemoteBlockSizeFetchToMem <- '200m'
config$spark.sql.broadcastTimeout <- '36000'
config$spark.hive.convertMetastoreOrc <- "false"
 
sc <- spark_connect(master = "yarn-client",
                    config = config,
                    app_name = "raw_PGV")
 
raw_PGV <- spark_read_csv(sc, "/home/cdsw/PGV_Load/pp_18_enc_v_t.csv")
                      
#head(raw_PGV)
 
raw_PGV2 <- raw_PGV %>%
  mutate(Adj_score = as.double(Adj_score)) %>%
  mutate(N = as.integer(N)) %>%
  mutate(Reliab = as.double(Reliab))
 
head(raw_PGV2)
 
raw_PGV_list <- raw_PGV %>%
  select(Base_Measure_ID) %>%
  distinct() %>%
  sdf_collect()
 
head(raw_PGV_list)
 
edu <- raw_PGV %>%
  filter(Base_Measure_ID == "CTM_EDU")
 
head(edu)
 
for (i in 1:nrow(raw_PGV_list)) {
  
  filename <- data.frame(raw_PGV_list[i,1])
 
  filename2 <- tolower(as.character(filename))
  
  print(filename2)
  
  PGV_select <- raw_PGV %>%
    filter(Base_Measure_ID == toupper(filename2))
  
    print(head(PGV_select))
  
    print(class(filename))
    
    meas_list <- paste0("hdfs dfs -rm -r /user/hive/warehouse/PGV_my2021.db/ahu_member_detail")
    meas_list
    system(meas_list, intern = TRUE)
    
    drop_tbl <- paste0("drop table if exists PGV_my2018.ahu_member_detail purge")
    #create_tbl <- paste0("create table viip_my2019.pcp_", measure3, "_", tolower(submeasure))
    
    DBI::dbGetQuery(sc, drop_tbl)
  
    spark_write_table(
      raw_PGV,
      name = paste0("viip_my2021.VIIP_EV_MY2021_04NOV2021"),
      mode = 'overwrite'
    ) 
}
 
spark_disconnect(sc)
