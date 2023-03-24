library(duckdb)
library(arrow)
library(tidyverse)
library(lubridate)
library(DBI)

rm(list =ls())

# con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.db")
con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.duckdb", read_only = FALSE)
#
taxi01 <- read_parquet("./data/yellow_tripdata_2010-01.parquet") %>%
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)

taxi02 <- read_parquet("./data/yellow_tripdata_2010-02.parquet") %>%
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)

                                                                   
taxi03 <- read_parquet("./data/yellow_tripdata_2010-03.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)

taxi04 <- read_parquet("./data/yellow_tripdata_2010-04.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi05 <- read_parquet("./data/yellow_tripdata_2010-05.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)

taxi06 <- read_parquet("./data/yellow_tripdata_2010-06.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)

# Create data frame
db1  <- bind_rows(taxi01,taxi02,taxi03,taxi04,taxi05,taxi06) 
db1$timestamp <- lubridate::ymd_hms(db1$pickup_datetime)
db1$start_date <- as.Date(db1$timestamp)
db1$start_hour <- lubridate::hour(db1$timestamp)

db1date <- db1 %>% 
  group_by(start_date) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum,mean=  mean,max = max)))

db1hour <- db1 %>% 
  group_by(start_hour) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))
           
            
        
dbWriteTable(con, "db1date", db1date,overwrite=TRUE)
dbWriteTable(con, "db1hour", db1hour,overwrite=TRUE)
dbWriteTable(con, "db1", db1,overwrite=TRUE)
rm(taxi01,taxi02,taxi03,taxi04,taxi05,taxi06)
#
#
taxi07 <- read_parquet("./data/yellow_tripdata_2010-07.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi08 <- read_parquet("./data/yellow_tripdata_2010-08.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi09 <- read_parquet("./data/yellow_tripdata_2010-09.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi10 <- read_parquet("./data/yellow_tripdata_2010-10.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi11 <- read_parquet("./data/yellow_tripdata_2010-11.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi12 <- read_parquet("./data/yellow_tripdata_2010-12.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)


#  Combine the data frames into a single data frame
db2  <- bind_rows(taxi07,taxi08,taxi09,taxi10,taxi11,taxi12)
db2$timestamp <- lubridate::ymd_hms(db2$pickup_datetime)
db2$start_date <- as.Date(db2$timestamp)
db2$start_hour <- lubridate::hour(db2$timestamp)

db2date <- db2 %>% 
  group_by(start_date) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum,mean=  mean,max = max)))

db2hour <- db2 %>% 
  group_by(start_hour) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))
dbWriteTable(con, "db2", db2, overwrite=TRUE)
dbWriteTable(con, "db2date", db2date, overwrite=TRUE)
dbWriteTable(con, "db2hour", db2hour, overwrite=TRUE)
rm(taxi07,taxi08,taxi09,taxi10,taxi11,taxi12)

##
ride_dates <- bind_rows(db1date,db2date)
ride_hours <- bind_rows(db1hour,db2hour)

dbWriteTable(con, "ridedates", ride_dates, overwrite=TRUE)
dbWriteTable(con, "ridehour", ride_hours, overwrite=TRUE)

ride_dates %>%
  ggplot(aes(x=start_date,y=passenger_count_mean)) + geom_line() + geom_smooth()
