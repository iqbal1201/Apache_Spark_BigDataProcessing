from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col
from datetime import datetime as dt
import os
import warnings
from datetime import datetime as dt

import arcpy


arcpy.env.overwriteOutput = True
warnings.filterwarnings('ignore')



spark = SparkSession.builder.master("local").appName('test').getOrCreate()


def get_signal_coverage_data(egdb_path):

    #Planet
    url_planet = r"https://geospatialdev.telkomsel.co.id/arcgis/rest/services/Hosted/Planet_1KM_Final/FeatureServer/0"
    df_Planet = spark.read.format("webgis").load(url_planet)
    df_Planet = df_Planet.select("OBJECTID", "grid_id","wadmpr", "wadmkk", "wadmkc", "wadmkd", "source", "mean_levels_dbm","mean_levels_dbm_category","mean_levels_dbm_label","SHAPE")
    df_Planet = df_Planet.withColumnRenamed("mean_levels_dbm","signal_strength_dbm").withColumnRenamed("mean_levels_dbm_category","signal_strength_category").withColumnRenamed("mean_levels_dbm_label","signal_strength_label")
    df_Planet = df_Planet.withColumn("operator", lit("Telkomsel"))
    df_Planet = df_Planet.select("OBJECTID", "grid_id", "wadmpr", "wadmkk", "wadmkc", "wadmkd", "operator", "source", "signal_strength_dbm","signal_strength_category","signal_strength_label","SHAPE")
    

    # Tutela
    url_tutela = r"https://geospatialdev.telkomsel.co.id/arcgis/rest/services/Hosted/Tutela_1KM_Final/FeatureServer/0"
    df_Tutela =  spark.read.format("webgis").load(url_tutela)
    df_Tutela = df_Tutela.select("OBJECTID", "grid_id", "wadmpr", "wadmkk", "wadmkc", "wadmkd", "operator", "source", "weighted_signal_strength_dbm","weighted_signal_strength_category","weighted_signal_strength_label","SHAPE")
    df_Tutela = df_Tutela.withColumnRenamed("weighted_signal_strength_dbm","signal_strength_dbm").withColumnRenamed("weighted_signal_strength_category","signal_strength_category").withColumnRenamed("weighted_signal_strength_label","signal_strength_label")
    df_Tutela = df_Tutela.select("OBJECTID", "grid_id", "wadmpr", "wadmkk", "wadmkc", "wadmkd", "operator", "source", "signal_strength_dbm","signal_strength_category","signal_strength_label","SHAPE")
    
    
    
    # Union/Concantenate Tutela and Planet (same schema) into one dataframe
    df_tutela_planet = df_Tutela.union(df_Planet)
    
    df_tutela_planet.write.format("webgis").save("GT_1KM_Tutela_Data_Signal_Coverage_Temp") #Result will be used as input for signal df   in blank_spot_status function
    
    



def blank_spot_status():
    """
    input file_demand = layer contains the demand model
    input file_signal = layer contains low signal coverage
    output file = blank_spot_file
    """

    url_demand = r"https://geospatialdev.telkomsel.co.id/arcgis/rest/services/Demand/Grid1km_DemandPot_Parameter_Model_Update_01082023/FeatureServer/1" 
    
    url_tutela_planet = r"https://geospatialdev.telkomsel.co.id/arcgis/rest/services/BlankSpot/GT_1KM_Signal_Coverage_Dev/FeatureServer/57" ## Url Should be the #same with the tutela planet
    
    
    demand_df = spark.read.format("webgis").load(url_demand)
    
    signal_df = spark.read.format("webgis").load( url_tutela_planet)
    
    signal_df = signal_df.withColumnRenamed("grid_id", "grid_id_signal")

    # Getting the required fields from demand layer
    cols = ["grid_id", "local_moransi_categ"]
    file_demand_selected = demand_df.select(cols)

    # create new layer for join the selected demand into signal layer
    blank_spot_file = file_demand_selected.join(signal_df, file_demand_selected.grid_id == signal_df.grid_id_signal, "inner")

    # Create new field to fill the categorisation result

    blank_spot_file = blank_spot_file.withColumn("BlankSpotStatus", \
    when((blank_spot_file.signal_strength_category == "P4") & (blank_spot_file.local_moransi_categ == "High-High"), lit("Blank Spot in High-High")) \
   .when((blank_spot_file.signal_strength_category == "P4") & (blank_spot_file.local_moransi_categ == "High-Low"), lit("Blank Spot in High-Low")) \
   .when((blank_spot_file.signal_strength_category == "P4") & (blank_spot_file.local_moransi_categ == "Low-High"), lit("Blank Spot in Low-High")) \
   .when((blank_spot_file.signal_strength_category == "P4") & (blank_spot_file.local_moransi_categ == "Low-Low"), lit("Blank Spot in Low-Low")) \
   .when((blank_spot_file.signal_strength_category == "P4") & (blank_spot_file.local_moransi_categ == "Insignificant"), lit("Blank Spot in Insignificant")) \
  )
    
    # Save output file into new Spatiotemporal layer
    blank_spot_file.write.format("webgis").save("Blank Spot Category")


    

    end_time = dt.now()
    print("Blank Spot Categories Process Is Complete: {}".format(end_time))
    print("With Duration: {}".format(end_time - start_time))
    print("=====================================================================\n")



if __name__ == '__main__':

    start_time = dt.now()
    print("=====================================================================")
    print("START PROCESSING: {} \n".format(start_time))
    

    egdb_path = r'C:\Users\ypratama\Documents\Project\Telkomsel\CellSiteSuitability\Tsel_Data.gdb'

    print("Get Signal Coverage Data...", end = " ")
    get_signal_coverage_data(egdb_path, spark)
    print("[DONE]")

    print("Compile Signal Coverage Data...", end = " ")
    compile_signal_coverage_data(egdb_path)
    print("[DONE]")
    
  
    print("Blank Spot Categories Processing...", end = " ")
    blank_spot_status()
    print("[DONE]")
 

    end_time = dt.now()
    print("Data Processing is Complete: {}".format(end_time))
    print("With Duration: {}\n".format(end_time-start_time))
    print("[DONE]\n")
