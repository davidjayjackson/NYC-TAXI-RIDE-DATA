library(duckdb)
library(DBI)
library(tidyverse)
library(scales)

con <- dbConnect(duckdb::duckdb(), dbdir = "./nyctaxis.duckdb", read_only = FALSE)
dbGetQuery(con,"SELECT pickup_datetime,count(*) as p_count  FROM db_cleaner 
           GROUP BY pickup_datetime
           ORDER  BY p_count desc;") %>% 
ggplot(aes(x=pickup_datetime,y=p_count)) + geom_line()

dbGetQuery(con,"
           SELECT pickup_datetime, COUNT(*) AS p_count  
            FROM db_cleaner 
            WHERE pickup_datetime LIKE '%-01-%'
            GROUP BY pickup_datetime
            ORDER BY p_count DESC;")%>% 
            ggplot(aes(x=pickup_datetime,y=p_count)) + geom_col()


dbGetQuery(con,"
           SELECT
           EXTRACT(year FROM pickup_datetime) AS pickup_year, 
           EXTRACT(month FROM pickup_datetime) AS pickup_month,
           COUNT(*) AS ride_count  
          FROM db_cleaner 
           WHERE pickup_datetime LIKE '%-01-%'
          GROUP BY pickup_year,pickup_month
          ORDER BY pickup_year,pickup_month;") %>%
  ggplot(aes(x=pickup_month,y=ride_count)) + geom_smooth(span=0.25) +
  geom_point(aes(x=pickup_month,y=ride_count)) +
  scale_y_continuous(labels = comma) +
  facet_wrap(~pickup_year)


dbGetQuery(con,"
           SELECT
           EXTRACT(year FROM pickup_datetime) AS pickup_year, 
          COUNT(*) AS ride_count  
          FROM db_cleaner 
           WHERE pickup_datetime LIKE '%-01-%'
          GROUP BY pickup_year;") %>%
  ggplot(aes(x=pickup_year,y=ride_count)) + 
  geom_col()

