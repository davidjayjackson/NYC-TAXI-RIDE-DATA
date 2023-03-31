library(duckdb)
library(arrow)
library(tidyverse)
library(lubridate)
library(DBI)

rm(list =ls())

# con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.db")
con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.duckdb", read_only = FALSE)
#
## Sample Raw Data:
sample <- read_parquet("./data/yellow_tripdata_2010-01.parquet")
sample$pickup_datetime <- ymd_hms(sample$pickup_datetime)
sample$dropoff_datetime <- ymd_hms(sample$dropoff_datetime)
dbWriteTable(con, "sample", sample,overwrite=TRUE)
##
##
taxi01 <- read_parquet("./data/yellow_tripdata_2010-01.parquet")  %>%
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi01$start_date <- as.Date(taxi01$pickup_datetime)
taxi01$start_hour <- lubridate::hour(taxi01$pickup_datetime)
taxi01$month_name <- lubridate::month(taxi01$pickup_datetime, label =TRUE,abbr = FALSE)
taxi01$wday <- lubridate::wday(taxi01$pickup_datetime, label =TRUE,abbr = FALSE)

taxi02 <- read_parquet("./data/yellow_tripdata_2010-02.parquet") %>%
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi02$start_date <- as.Date(taxi02$pickup_datetime)
taxi02$start_hour <- lubridate::hour(taxi02$pickup_datetime)
taxi02$month_name <- lubridate::month(taxi02$pickup_datetime, label =TRUE,abbr = FALSE)
taxi02$wday <- lubridate::wday(taxi02$pickup_datetime, label =TRUE,abbr = FALSE)

                                                                   
taxi03 <- read_parquet("./data/yellow_tripdata_2010-03.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi03$start_date <- as.Date(taxi03$pickup_datetime)
taxi03$start_hour <- lubridate::hour(taxi03$pickup_datetime)
taxi03$month_name <- lubridate::month(taxi03$pickup_datetime, label =TRUE,abbr = FALSE)
taxi03$wday <- lubridate::wday(taxi03$pickup_datetime, label =TRUE,abbr = FALSE)
## Combine 3 data frames
db1 <- bind_rows(taxi01,taxi02,taxi03)
#
taxi04 <- read_parquet("./data/yellow_tripdata_2010-04.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi04$start_date <- as.Date(taxi04$pickup_datetime)
taxi04$start_hour <- lubridate::hour(taxi04$pickup_datetime)
taxi04$month_name <- lubridate::month(taxi04$pickup_datetime, label =TRUE,abbr = FALSE)
taxi04$wday <- lubridate::wday(taxi04$pickup_datetime, label =TRUE,abbr = FALSE)

taxi05 <- read_parquet("./data/yellow_tripdata_2010-05.parquet") %>%
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi05$start_date <- as.Date(taxi05$pickup_datetime)
taxi05$start_hour <- lubridate::hour(taxi05$pickup_datetime)
taxi05$month_name <- lubridate::month(taxi05$pickup_datetime, label =TRUE,abbr = FALSE)
taxi05$wday <- lubridate::wday(taxi05$pickup_datetime, label =TRUE,abbr = FALSE)


taxi06 <- read_parquet("./data/yellow_tripdata_2010-06.parquet") %>% 
  select(pickup_datetime,passenger_count,trip_distance,
         fare_amount,total_amount,pickup_longitude,pickup_latitude)
taxi06$start_date <- as.Date(taxi06$pickup_datetime)
taxi06$start_hour <- lubridate::hour(taxi06$pickup_datetime)
taxi06$month_name <- lubridate::month(taxi06$pickup_datetime, label =TRUE,abbr = FALSE)
taxi06$wday <- lubridate::wday(taxi06$pickup_datetime, label =TRUE,abbr = FALSE)


# Create data frame
db2  <- bind_rows(taxi04,taxi05,taxi06) 
#db3$pickup_longitude <-NA
#db3$pickup_latitude <- NA

db1date <- db1 %>% 
  group_by(start_date) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum,mean=  mean,max = max)))

db1hour <- db1%>% 
  group_by(start_hour) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))
           

db1wday <- db1 %>% 
  group_by(wday) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))

db2date <- db2 %>% 
  group_by(start_date) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum,mean=  mean,max = max)))

db2hour <- db2%>% 
  group_by(start_hour) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))


db2wday <- db2 %>% 
  group_by(wday) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))
        
dbWriteTable(con, "db1date", db1date,overwrite=TRUE)
dbWriteTable(con, "db1hour", db1hour,overwrite=TRUE)
dbWriteTable(con, "db1wday", db1wday,overwrite=TRUE)
dbWriteTable(con, "db2date", db2date,overwrite=TRUE)
dbWriteTable(con, "db2hour", db2hour,overwrite=TRUE)
dbWriteTable(con, "db2wday", db1wday,overwrite=TRUE)
# dbWriteTable(con, "db1", db1,overwrite=TRUE)
# rm(taxi01,taxi02,taxi03,taxi04,taxi05,taxi06)
#
## Second half of 2011

  taxi07 <- read_parquet("./data/yellow_tripdata_2010-07.parquet") %>%
  
    select(pickup_datetime,passenger_count,trip_distance,
           fare_amount,total_amount,pickup_longitude,pickup_latitude)
  taxi07$start_date <- as.Date(taxi07$pickup_datetime)
  taxi07$start_hour <- lubridate::hour(taxi07$pickup_datetime)
  taxi07$month_name <- lubridate::month(taxi07$pickup_datetime, label =TRUE,abbr = FALSE)
  taxi07$wday <- lubridate::wday(taxi07$pickup_datetime, label =TRUE,abbr = FALSE)
  
  taxi08 <- read_parquet("./data/yellow_tripdata_2010-08.parquet") %>%
    select(pickup_datetime,passenger_count,trip_distance,
           fare_amount,total_amount,pickup_longitude,pickup_latitude)
  taxi08$start_date <- as.Date(taxi08$pickup_datetime)
  taxi08$start_hour <- lubridate::hour(taxi08$pickup_datetime)
  taxi08$month_name <- lubridate::month(taxi08$pickup_datetime, label =TRUE,abbr = FALSE)
  taxi08$wday <- lubridate::wday(taxi08$pickup_datetime, label =TRUE,abbr = FALSE)
  
  
  taxi09 <- read_parquet("./data/yellow_tripdata_2010-09.parquet") %>% 
  
    select(pickup_datetime,passenger_count,trip_distance,
           fare_amount,total_amount,pickup_longitude,pickup_latitude)
  taxi09$start_date <- as.Date(taxi09$pickup_datetime)
  taxi09$start_hour <- lubridate::hour(taxi09$pickup_datetime)
  taxi09$month_name <- lubridate::month(taxi09$pickup_datetime, label =TRUE,abbr = FALSE)
  taxi09$wday <- lubridate::wday(taxi09$pickup_datetime, label =TRUE,abbr = FALSE)
  ###
  ### combine Month 7 - 9
   
  
  taxi10 <- read_parquet("./data/yellow_tripdata_2010-10.parquet") %>% 
  
    select(pickup_datetime,passenger_count,trip_distance,
           fare_amount,total_amount,pickup_longitude,pickup_latitude)
  taxi10$start_date <- as.Date(taxi10$pickup_datetime)
  taxi10$start_hour <- lubridate::hour(taxi10$pickup_datetime)
  taxi10$month_name <- lubridate::month(taxi10$pickup_datetime, label =TRUE,abbr = FALSE)
  taxi10$wday <- lubridate::wday(taxi10$pickup_datetime, label =TRUE,abbr = FALSE)
  
  taxi11 <- read_parquet("./data/yellow_tripdata_2010-11.parquet") %>% 
   
    select(pickup_datetime,passenger_count,trip_distance,
           fare_amount,total_amount,pickup_longitude,pickup_latitude)
  taxi11$start_date <- as.Date(taxi11$pickup_datetime)
  taxi11$start_hour <- lubridate::hour(taxi11$pickup_datetime)
  taxi11$month_name <- lubridate::month(taxi11$pickup_datetime, label =TRUE,abbr = FALSE)
  taxi11$wday <- lubridate::wday(taxi11$pickup_datetime, label =TRUE,abbr = FALSE)
  
  
  taxi12 <- read_parquet("./data/yellow_tripdata_2010-12.parquet") %>% 
    
    select(pickup_datetime,passenger_count,trip_distance,
           fare_amount,total_amount,pickup_longitude,pickup_latitude)
  taxi12$start_date <- as.Date(taxi12$pickup_datetime)
  taxi12$start_hour <- lubridate::hour(taxi12$pickup_datetime)
  taxi12$month_name <- lubridate::month(taxi12$pickup_datetime, label =TRUE,abbr = FALSE)
  taxi12$wday <- lubridate::wday(taxi12$pickup_datetime, label =TRUE,abbr = FALSE)

  
# Create data frame
db3  <- bind_rows(taxi07,taxi08,taxi09)
db4  <- bind_rows(taxi10,taxi11,taxi12) 
# db4$pickup_longitude <-NA
# db4$pickup_latitude <- NA

db3date <- db3 %>% 
  group_by(start_date) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum,mean=  mean,max = max)))

db3hour <- db3 %>% 
  group_by(start_hour) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))


db3wday <- db3 %>% 
  group_by(wday) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))

db4date <- db4 %>% 
  group_by(start_date) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum,mean=  mean,max = max)))

db4hour <- db4 %>% 
  group_by(start_hour) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))


db4wday <- db4 %>% 
  group_by(wday) %>% 
  summarise(across(passenger_count:total_amount,c(sum = sum, mean= mean, max = max)))



dbWriteTable(con, "db3date", db2date,overwrite=TRUE)
dbWriteTable(con, "db3hour", db2hour,overwrite=TRUE)
dbWriteTable(con, "db3wday", db2wday,overwrite=TRUE)
dbWriteTable(con, "db4date", db2date,overwrite=TRUE)
dbWriteTable(con, "db4hour", db2hour,overwrite=TRUE)
dbWriteTable(con, "db4wday", db2wday,overwrite=TRUE)

rm(taxi07,taxi08,taxi09,taxi10,taxi11,taxi12)

##
ride_dates <- bind_rows(db3date,db4date)
ride_hours <- bind_rows(db3hour,db4date)

dbWriteTable(con, "ridedates", ride_dates, overwrite=TRUE)
dbWriteTable(con, "ridehour", ride_hours, overwrite=TRUE)

ride_dates %>%
  ggplot(aes(x=start_date,y=passenger_count_sum)) + geom_line() + geom_smooth()
