# -------------------------------------------------------------------------
#
#  PGV_sql_pull
#
#  Program to ingest files from MS SQL Server
#
# -------------------------------------------------------------------------

# list all the packages needed --------------------------------------------
 
packages <- c("tidyverse", "sparklyr")
 
# this will check all existing packages for the packages listed above
new_packages <- packages[!(packages %in%installed.packages()[,"Package"])]
 
# if the packages are not installed, this will install them
if (length(new_packages)) {
  install.packages(new_packages, lib = "/home/cdsw/R ")
}
 
# this will load all the listed packages
invisible(lapply(packages, library,character.only = T))
 
library(odbc)
con <- dbConnect(odbc(), 
                 Driver = "libtdsodbc.so",
                 Server = "sql18custdev5",
                 Database = "QPM_PGV",
                 UID = Sys.getenv("USR"),
                 PWD = Sys.getenv("PWD"),
                 port = 1433)
 
