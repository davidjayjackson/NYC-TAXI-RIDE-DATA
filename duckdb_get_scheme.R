library(duckdb)
library(DBI)
library(tidyverse)

con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.duckdb", read_only = FALSE)


# Execute the SQL query and fetch the results
dbGetQuery(con,"SELECT name FROM sqlite_master WHERE type='table';")

dbGetQuery(con,"PRAGMA table_info(sample);")
dbGetQuery(con,"SELECT pickup_datetime,count(*) as p_count  FROM db GROUP BY pickup_datetime
          ORDER  BY p_count desc;") %>% ggplot(aes(x=pickup_datetime,y=p_count)) + geom_col()


dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2010;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2011;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2012;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2013;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2014;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2015;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2016;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2017;")
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2018;")
## Problem with data range Max date = 2088-01-24
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2019;")
# data range problems: 2002-01-01 to 2088-01-24
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2020;")
# date range: 2008-12-31 to 2021-02-22
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2021;")
# date range: 2008-12-31 to 2022-05-18
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2022;")
# date range 2008-12-31 to 2023-02-01
dbGetQuery(con,"SELECT min(pickup_datetime), max(pickup_datetime)  FROM jan2023;")

dbGetQuery(con,"SELECT  pickup_datetime,count(*) as ride_counts 
           FROM jan2023 GROUP BY pickup_datetime 
           ORDER BY pickup_datetime 
           WHERE pickup_datetime >="2022-12-31";")
