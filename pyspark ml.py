# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 19:30:20 2018

@author: YZZ
"""
from pyspark.sql import SparkSession
import pyspark.ml.feature as ft
from pyspark.sql.types import *
import pyspark.sql.types as typ

spark= SparkSession.builder.appName("dataFrame").getOrCreate()

stringCSVRDD = spark.sparkContext.parallelize([
    (123,"Katie",19,"brown"),
    (123,"Kkk",19,"red"),
    (234,"Michael",22,"green"),
    (345,"Simone",23,"blue")
])

schema = StructType([
    StructField("id",StringType(),True),
    StructField("name",StringType(),True),
    StructField("age",LongType(),True),
    StructField("eyeColor",StringType(),True)
])

swimmers = spark.createDataFrame(stringCSVRDD,schema)
swimmers = swimmers.withColumn('id_int',swimmers['id'].cast(typ.IntegerType()))
encoder = ft.OneHotEncoder(inputCol = 'id_int',outputCol = 'idvec')
swimmers.registerTempTable("swimmers")
swimmers.select("idvec").show()


from pyspark.sql import SparkSession
import pyspark.sql.types as typ
import pyspark.ml.feature as ft

labels = [
('INFANT_ALIVE_AT_REPORT', typ.IntegerType()),
('BIRTH_PLACE', typ.IntegerType()),
('MOTHER_AGE_YEARS', typ.IntegerType()),
('FATHER_COMBINED_AGE', typ.IntegerType()),
('CIG_BEFORE', typ.IntegerType()),
('CIG_1_TRI', typ.IntegerType()),
('CIG_2_TRI', typ.IntegerType()),
('CIG_3_TRI', typ.IntegerType()),
('MOTHER_HEIGHT_IN', typ.IntegerType()),
('MOTHER_PRE_WEIGHT', typ.IntegerType()),
('MOTHER_DELIVERY_WEIGHT', typ.IntegerType()),
('MOTHER_WEIGHT_GAIN', typ.IntegerType()),
('DIABETES_PRE', typ.IntegerType()),
('DIABETES_GEST', typ.IntegerType()),
('HYP_TENS_PRE', typ.IntegerType()),
('HYP_TENS_GEST', typ.IntegerType()),
('PREV_BIRTH_PRETERM', typ.IntegerType())
]

births_transformed= "file:///home/yuty/yangzz/births_transformed.csv"
schema = typ.StructType([typ.StructField(e[0], e[1], False) for e in labels])
births = spark.read.csv(births_transformed,header=True,schema=schema)
featuresCreator = ft.VectorAssembler(inputCols=[col[0] for col in labels[1:]] ,outputCol='features').transform(births).select('features').collect()

from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import BisectingKMeans

data = [(Vectors.dense([10, 10]),),(Vectors.dense([3.0, 5.0]),),(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),(Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
df = spark.createDataFrame(data, ["features"])
bkm = BisectingKMeans(k=2, minDivisibleClusterSize=1.0)
model = bkm.fit(df)
centers = model.clusterCenters()
len(centers)
model.computeCost(df)
model.hasSummary
summary = model.summary
summary.k
summary.clusterSizes

transformed = model.transform(df).select("features", "prediction")
rows = transformed.collect()
rows[0].prediction











