import arcgis
import arcgis.geoanalytics

from arcgis.geoanalytics.summarize_data import summarize_center_and_dispersion
from arcgis.geoanalytics.manage_data import run_python_script

gis = arcgis.GIS('https://geospatialdev.telkomsel.co.id/portal', 'adminportaldev', 'GisPRTdev', verify_cert=False)


import arcgis
from arcgis.geoanalytics.manage_data import run_python_script

script_code = '''

import pandas as pd
import os
import arcpy
import warnings
import arcgis

from arcgis.geoanalytics.summarize_data import join_features
from arcgis.geoanalytics.manage_data import copy_to_data_store
from arcgis.geoanalytics.summarize_data import summarize_center_and_dispersion
from arcgis.features import GeoAccessor
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, sum, unix_timestamp, from_unixtime, to_timestamp, lit, count, when, concat, rank, udf, col, mean, stddev
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime as dt
from pyspark.sql.types import DoubleType


spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

aoi_url = r"https://geospatialdev.telkomsel.co.id/arcgis/rest/services/Triangulation/AOI_JabodetabekJabar_Subs/FeatureServer/52"
demand_url = r"https://geospatialdev.telkomsel.co.id/arcgis/rest/services/Demand/Grid1km_DemandPot_Parameter_Model_Update_01082023/FeatureServer/1"
url_fs = r"https://geospatialdev.telkomsel.co.id/geoanalytics/rest/services/DataStoreCatalogs/bigDataFileShares_data/BigDataCatalogServer/covmo_prod_prod"


spark_data = spark.read.format("webgis").load(url_fs)
aoi_spark = spark.read.format("webgis").load(aoi_url)
demand = spark.read.format("webgis").load(demand_url)
demand = demand.select('GRID_ID', 'Norm_DistRoad_InvMinMax', 'Norm_DistBuiltUp_InvMinMax', 'Norm_POI_MinMaxDefault', 'POI_DomCategory', \
                        'wadmpr', 'wadmkk', 'wadmkc', 'wadmkd', 'Shape__Area', 'Shape__Length', '$geometry')\
                .withColumnRenamed('GRID_ID', 'grid_id')



spark_data = spark_data.withColumn("datetime", F.unix_timestamp("date_time", "yyyy-MM-dd HH:mm:ss").cast("double").cast("timestamp"))\
                        .withColumn("date", to_date(col('datetime')))\
                        .withColumn("hour", hour(col("datetime")))\
                        .withColumn("date_final", F.date_format(F.to_timestamp(F.concat("date","hour"),"yyyy-MM-ddHH"), "yyyy-MM-dd HH:mm:ss"))\
                        .withColumn("imsi_date", F.concat_ws("_", "imsi", "date_final"))
                        

w = Window().partitionBy(["imsi", "date_final"]).orderBy(F.col("datetime").asc())
spark_data = spark_data.withColumn("duration", F.lead(F.unix_timestamp("datetime")).over(w))\
                          .withColumn("duration", F.when(F.col("duration").isNotNull(), F.col("duration") - F.unix_timestamp("datetime"))\
                         .otherwise(F.lit(0))).orderBy("imsi","datetime")



print("Spark Data Load: ", spark_data.count())

summarize_triangulation = geoanalytics.summarize_center_and_dispersion(input_layer = spark_data,
                                                summary_type = "MeanCenter",
                                                weight_field='duration',
                                                group_fields='imsi_date')



summarize_triangulation = summarize_triangulation["meanCenterLayer"]


print("Triangulation Number: ", summarize_triangulation.count())


summarize_triangulation.write.format("webgis").save("Subscriber_Location")


intersect_tri = geoanalytics.join_features(target_layer= summarize_triangulation,
                                            join_layer=aoi_spark,
                                            join_operation="JoinOneToMany",
                                            spatial_relationship="Intersects",
                                            )


print("Intersect Result: ", intersect_tri.count())


subs_mobility = intersect_tri.withColumn('imsi', F.split(intersect_tri['imsi_date'], '_').getItem(0)) \
                   .withColumn('datetime',  F.split(intersect_tri['imsi_date'], '_').getItem(1).cast("timestamp"))\
                    .withColumn("date", to_date(col('datetime')))\
                    .withColumn("hour", hour(col("datetime")))\
                    .withColumn("device_count", F.lit(1))\
                    .withColumn("hour_class", F.when((F.col("hour") >= 0) & (F.col('hour') <= 5), "Home_1")\
                        .when((F.col("hour") >= 6) & (F.col('hour') <= 7), "Commute_1")\
                        .when((F.col("hour") >= 8) & (F.col('hour') <= 17), "Working")\
                        .when((F.col("hour") >= 18) & (F.col('hour') <= 20), "Commute_2")\
                        .when((F.col("hour") >= 21) & (F.col('hour') <= 23), "Home_2")
                          )\
                    .withColumn("datetime_class", F.when(F.col("hour_class") == 'Home_1', "Time: 00.00 - 06.00")\
                                    .when(F.col("hour_class") == 'Commute_1', "Time: 06.00 - 08.00")\
                                    .when(F.col("hour_class") == 'Working', "Time: 08.00 - 18.00")\
                                    .when(F.col("hour_class") == 'Commute_2', "Time: 18.00 - 21.00")\
                                    .when(F.col("hour_class") == 'Home_2', "Time: 21.00 - 24.00")
                       )\
                    .withColumn("start_hour_int", F.when(F.col("hour_class") == 'Home_1', 0)\
                                    .when(F.col("hour_class") == 'Commute_1', 6)\
                                    .when(F.col("hour_class") == 'Working', 8)\
                                    .when(F.col("hour_class") == 'Commute_2', 18)\
                                    .when(F.col("hour_class") == 'Home_2', 21)
                       )\
                    .withColumn("end_hour_int", F.when(F.col("hour_class") == 'Home_1', 5)\
                                    .when(F.col("hour_class") == 'Commute_1', 7)\
                                    .when(F.col("hour_class") == 'Working', 17)\
                                    .when(F.col("hour_class") == 'Commute_2', 20)\
                                    .when(F.col("hour_class") == 'Home_2', 23))
                                    

group_cols_2 = ['grid_id', 'date', 'hour_class','datetime_class', 'start_hour_int', 'end_hour_int']
subs_mobility = subs_mobility.groupBy(group_cols_2).agg(sum("device_count").alias("sum_device"))






subs_mobility = subs_mobility.withColumn("start_date_time_utc", (F.date_format(F.to_timestamp(F.concat("date","start_hour_int"),"yyyy-MM-ddHH"), "yyyy-MM-dd HH:mm:ss")) - F.expr("INTERVAL 7 HOURS"))\
                            .withColumn("end_date_time_utc", (F.date_format(F.to_timestamp(F.concat("date","end_hour_int"),"yyyy-MM-ddHH"), "yyyy-MM-dd HH:mm:ss")) - F.expr("INTERVAL 7 HOURS"))\

subs_mobility = subs_mobility.select(["grid_id", "date", "hour_class", "datetime_class", "start_date_time_utc", "end_date_time_utc", "sum_device"])





windowRank  = Window.partitionBy("grid_id", "date").orderBy("sum_device")
subs_mobility = subs_mobility.withColumn("relative_value", rank().over(windowRank))


print("Start Join Subscriber Mobility with demand....", end = " ")

subs_mobility_and_poi = subs_mobility.join(demand, 'grid_id', 'inner')



print("Get max min sum device....", end = " ")

max_value = subs_mobility_and_poi.agg({"sum_device": "max"}).collect()[0]
min_value = subs_mobility_and_poi.agg({"sum_device": "min"}).collect()[0]


print("[Done]")



print("Normalize Sum device....", end = " ")


def custom_normalize(df, columns_to_normalize):
    for col_name in columns_to_normalize:
        min_col = df.agg({col_name: "min"}).collect()[0][0]
        max_col = df.agg({col_name: "max"}).collect()[0][0]
        df = df.withColumn("Norm_sum_device", (col(col_name) - min_col) / (max_col - min_col))
    return df
    
    
columns_to_normalize = ["sum_device"]


subs_mobility_and_poi_df = custom_normalize(subs_mobility_and_poi, columns_to_normalize)



print("[Done]")


print("Normalization Device: ", subs_mobility_and_poi_df.count())


print("Start POI Attractiveness Index Calculation....", end = " ")

subs_mobility_and_poi_df = subs_mobility_and_poi_df.withColumn("POI_attractiveness", F.round((col("Norm_sum_device") * 0.5) + (col("Norm_POI_MinMaxDefault") * 0.45) + \
                            (col("Norm_DistBuiltUp_InvMinMax") * 0.03) + (col("Norm_DistRoad_InvMinMax") * 0.02), 3))

print("[Done]")


print("POI AI: ", subs_mobility_and_poi_df.count())


print("Classify POI Index....", end = " ")

subs_mobility_and_poi_df = subs_mobility_and_poi_df.withColumn("POI_attractiveness_class", F.when((F.col("POI_attractiveness") < 0.25), "Low")\
                        .when((F.col("POI_attractiveness") >= 0.25) & (F.col('POI_attractiveness') < 0.5), "Moderate")\
                        .when((F.col("POI_attractiveness") >= 0.5) & (F.col('POI_attractiveness') < 0.75), "High")\
                        .when((F.col("POI_attractiveness") >= 0.75), "Very High")
                          )
                     
subs_mobility_and_poi_df = subs_mobility_and_poi_df.select('wadmpr', 'wadmkk', 'wadmkc', 'wadmkd', "grid_id", "hour_class", "datetime_class", "datetime_class",\
                        "start_date_time_utc", "end_date_time_utc", "sum_device", "relative_value", "POI_DomCategory", "POI_attractiveness", "POI_attractiveness_class", \
                        'Shape__Area', 'Shape__Length', '$geometry')

subs_mobility_and_poi_df.show()


subs_mobility_and_poi_df.write.format("webgis").save("Subscriber_Mobility_POI_Attractiveness_Index")

print("[Done]")





'''

run_python_script(code = script_code)