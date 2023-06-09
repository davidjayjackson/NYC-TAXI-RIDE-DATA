library(tidyverse)
library(duckdb)
library(DBI)
library(arrow)
library(lubridate)

rm(list=ls())
## Init duckdb 
con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.duckdb", read_only = FALSE)
# 
## Load NYC taxi date for 1013
## Jam - June 
# sample0 <- read_parquet("./data/yellow_tripdata_2014-01.parquet") %>%
#   rename( pickup_datetime = tpep_pickup_datetime) %>%
#   rename( dropoff_datetime = tpep_dropoff_datetime) 
# sample0$start_date <- as.Date(sample0$pickup_datetime)
##

sample1 <- read_parquet("./data/yellow_tripdata_2016-01.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime) 
sample1$start_date <- as.Date(sample1$pickup_datetime)

sample2 <- read_parquet("./data/yellow_tripdata_2016-02.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime) 
sample2$start_date <- as.Date(sample2$pickup_datetime)

sample3 <- read_parquet("./data/yellow_tripdata_2016-03.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample3$start_date <- as.Date(sample3$pickup_datetime)

sample4 <- read_parquet("./data/yellow_tripdata_2016-04.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample4$start_date <- as.Date(sample4$pickup_datetime)

sample5 <- read_parquet("./data/yellow_tripdata_2016-05.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample5$start_date <- as.Date(sample5$pickup_datetime)

sample6 <- read_parquet("./data/yellow_tripdata_2016-06.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample6$start_date <- as.Date(sample6$pickup_datetime)
#
## July - Dec 2013
#
sample7 <- read_parquet("./data/yellow_tripdata_2016-07.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample7$start_date <- as.Date(sample7$pickup_datetime)

sample8 <- read_parquet("./data/yellow_tripdata_2016-08.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample8$start_date <- as.Date(sample8$pickup_datetime)

sample9 <- read_parquet("./data/yellow_tripdata_2016-09.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample9$start_date <- as.Date(sample9$pickup_datetime)

sample10 <- read_parquet("./data/yellow_tripdata_2016-10.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample10$start_date <- as.Date(sample10$pickup_datetime)

sample11 <- read_parquet("./data/yellow_tripdata_2016-11.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample11$start_date <- as.Date(sample11$pickup_datetime)

sample12 <- read_parquet("./data/yellow_tripdata_2016-12.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  rename( dropoff_datetime = tpep_dropoff_datetime)
sample12$start_date <- as.Date(sample12$pickup_datetime)

##
## Create New table for Jan. 2013 data
#dbWriteTable(con, "yellowcab", sample0,overwrite=TRUE)
## Import Taxi zone data
taxi_zone <- read_csv("./taxi_zones/taxi_zone_lookup.csv")
dbWriteTable(con, "zones",taxi_zone,overwrite=TRUE)
##
dbGetQuery(con," SELECT count(*),
           min(start_date),
           max(start_date)  
           FROM yellowcab ;")


## Append Feb - Dec onto yellowcab
dbWriteTable(con, "yellowcab", sample1,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample2,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample3,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample4,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample5,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample6,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample7,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample8,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample9,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample10,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample11,overwrite=FALSE,append = TRUE)
dbWriteTable(con, "yellowcab", sample12,overwrite=FALSE,append = TRUE)
