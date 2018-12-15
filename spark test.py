# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 12:48:33 2018

@author: YZZ
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()

flightPerfFilePath = "file:///home/yuty/yangzz/departuredelays.csv"
airportsFilePath ="file:///home/yuty/yangzz/airport-codes-na.txt"

airports = spark.read.csv(airportsFilePath,header='true',inferSchema='true',sep='\t')
airports.registerTempTable("airports")

flightPerf = spark.read.csv(flightPerfFilePath,header='true')
flightPerf.registerTempTable("FlightPerformance")

spark.sql("""select a.city,f.origin,sum(f.delay) as Delays
    from FlightPerformance f 
    join airports a 
    on a.IATA = f.origin 
    where a.Country = 'USA' 
    group by a.City,f.origin 
    order by sum(f.delay) asc""").show()

spark.sql("""
          select d1.state,d2.origin,d1.MaxDelay as MD
          from (  select a.State,f.origin,sum(f.delay) as Delays
                  from FlightPerformance f
                  join airports a 
                  on a.IATA = f.origin
                  where a.Country = 'USA'
                  group by f.origin,a.State
                  order by sum(f.delay)) d2
          join (  select State,Min(Delays) as MaxDelay
                  from (select a.State,f.origin,sum(f.delay) as Delays
                  from FlightPerformance f
                  join airports a 
                  on a.IATA = f.origin
                  where a.Country = 'USA'
                  group by f.origin,a.State
                  order by sum(f.delay))
                  group by State
                  order by Min(Delays) asc) d1
          on d1.state = d2.state and d2.Delays = d1.MaxDelay
          order by d1.MaxDelay asc
          """).show()







