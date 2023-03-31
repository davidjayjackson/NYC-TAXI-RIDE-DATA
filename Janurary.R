library(duckdb)
library(arrow)
library(tidyverse)
library(lubridate)
library(DBI)

rm(list =ls())

# con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.db")
con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.duckdb", read_only = FALSE)
#
sample <- read_parquet("./Jan2010-2019/yellow_tripdata_2010-01.parquet")
  sample$start_date <- as.Date(sample$pickup_datetime)
dbWriteTable(con, "sample", sample,overwrite=TRUE)
##
jan10 <- read_parquet("./Jan2010-2019/yellow_tripdata_2010-01.parquet")  %>%
  select(pickup_datetime,passenger_count,fare_amount)
jan10$pickup_datetime <- as.Date(jan10$pickup_datetime)


jan11 <- read_parquet("./Jan2010-2019/yellow_tripdata_2011-01.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  select(pickup_datetime,passenger_count,fare_amount)
jan11$pickup_datetime <- as.Date(jan11$pickup_datetime)


jan12 <- read_parquet("./Jan2010-2019/yellow_tripdata_2012-01.parquet") %>% 
  rename( pickup_datetime = tpep_pickup_datetime) %>%
  # select(pickup_datetime,passenger_count,trip_distance,
  #        fare_amount,total_amount)
  select(pickup_datetime,passenger_count,fare_amount)
jan12$pickup_datetime <- as.Date(jan12$pickup_datetime)



#
jan13 <- read_parquet("./Jan2010-2019/yellow_tripdata_2013-01.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>% 
  select(pickup_datetime,passenger_count,fare_amount)
jan13$pickup_datetime <- as.Date(jan13$pickup_datetime)


jan14 <- read_parquet("./Jan2010-2019/yellow_tripdata_2014-01.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>% 
  select(pickup_datetime,passenger_count,fare_amount)
jan14$pickup_datetime <- as.Date(jan14$pickup_datetime)


jan15 <- read_parquet("./Jan2010-2019/yellow_tripdata_2015-01.parquet") %>% 
  rename( pickup_datetime = tpep_pickup_datetime) %>% 
  select(pickup_datetime,passenger_count,fare_amount)
jan15$pickup_datetime <- as.Date(jan15$pickup_datetime)

     
jan16 <- read_parquet("./Jan2010-2019/yellow_tripdata_2016-01.parquet") %>%
  rename( pickup_datetime = tpep_pickup_datetime) %>% 
      select(pickup_datetime,passenger_count,fare_amount)
  jan16$pickup_datetime <- as.Date(jan16$pickup_datetime)
  
jan17 <- read_parquet("./Jan2010-2019/yellow_tripdata_2017-01.parquet") %>%
rename( pickup_datetime = tpep_pickup_datetime) %>% 
    select(pickup_datetime,passenger_count,fare_amount)
  jan17$pickup_datetime <- as.Date(jan17$pickup_datetime)
 
  
  jan18 <- read_parquet("./Jan2010-2019/yellow_tripdata_2018-01.parquet") %>% 
    rename( pickup_datetime = tpep_pickup_datetime) %>% 
      select(pickup_datetime,passenger_count,fare_amount)
  jan18$pickup_datetime<- as.Date(jan18$pickup_datetime)
  
   
  
  jan19 <- read_parquet("./Jan2010-2019/yellow_tripdata_2019-01.parquet") %>% 
    rename( pickup_datetime = tpep_pickup_datetime) %>% 
   # select(pickup_datetime,passenger_count,trip_distance,fare_amount)
    select(pickup_datetime,passenger_count,fare_amount)
  jan19$pickup_datetime <- as.Date(jan19$pickup_datetime)
  
  
  jan20 <- read_parquet("./Jan2010-2019/yellow_tripdata_2020-01.parquet") %>% 
    rename( pickup_datetime = tpep_pickup_datetime) %>% 
       select(pickup_datetime,passenger_count,fare_amount)
  jan20$pickup_datetime <- as.Date(jan20$pickup_datetime)
  
  
  jan21<- read_parquet("./Jan2010-2019/yellow_tripdata_2021-01.parquet") %>% 
    rename( pickup_datetime = tpep_pickup_datetime) %>% 
    select(pickup_datetime,passenger_count,fare_amount)
  jan21$pickup_datetime <- as.Date(jan21$pickup_datetime)
  

  jan22 <- read_parquet("./Jan2010-2019/yellow_tripdata_2022-01.parquet") %>% 
    rename( pickup_datetime = tpep_pickup_datetime) %>% 
    select(pickup_datetime,passenger_count,fare_amount)
       # select(pickup_datetime,passenger_count,trip_distance,fare_amount)
  jan22$pickup_datetime <- as.Date(jan22$pickup_datetime)

  jan23 <- read_parquet("./Jan2010-2019/yellow_tripdata_2023-01.parquet") %>% 
    rename( pickup_datetime = tpep_pickup_datetime) %>% 
    select(pickup_datetime,passenger_count,fare_amount)
   # select(pickup_datetime,passenger_count,trip_distance,fare_amount)
  jan23$pickup_datetime <- as.Date(jan23$pickup_datetime)

dbWriteTable(con, "jan2010", jan10,overwrite=TRUE)
dbWriteTable(con, "jan2011", jan11,overwrite=TRUE)
dbWriteTable(con, "jan2012", jan12,overwrite=TRUE)
dbWriteTable(con, "jan2013", jan13,overwrite=TRUE)
dbWriteTable(con, "jan2014", jan14,overwrite=TRUE)
dbWriteTable(con, "jan2015", jan15,overwrite=TRUE)
dbWriteTable(con, "jan2016", jan16,overwrite=TRUE)
dbWriteTable(con, "jan2017", jan17,overwrite=TRUE)
dbWriteTable(con, "jan2018", jan18,overwrite=TRUE)
dbWriteTable(con, "jan2019", jan19,overwrite=TRUE)
dbWriteTable(con, "jan2020", jan20,overwrite=TRUE)
dbWriteTable(con, "jan2021", jan21,overwrite=TRUE)
dbWriteTable(con, "jan2022", jan22,overwrite=TRUE)
dbWriteTable(con, "jan2023", jan23,overwrite=TRUE)

db <- bind_rows(jan10,jan11,jan12,jan13,jan14,jan15,jan16,jan17,
                jan18,jan19,jan20,jan21,jan22,jan23)

dbWriteTable(con, "db", db,overwrite=TRUE)

