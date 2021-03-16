#importing all the packages
import os
import sys

import numpy as np

sys.path.insert(0, "helperFunctions.py")
sys.path.insert(1, "qualityTests.py")
sys.path.insert(2, "i94MetadataMappings.py")
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, SQLContext, Window
from pyspark.sql.functions import (col, concat, date_format, datediff,
                                   dayofmonth, dayofweek, first, hour, isnan,
                                   last, lit, ltrim)
from pyspark.sql.functions import max as Max
from pyspark.sql.functions import mean
from pyspark.sql.functions import min as Min
from pyspark.sql.functions import month, row_number, rtrim, split
from pyspark.sql.functions import sum as Sum
from pyspark.sql.functions import to_date, udf, upper, weekofyear, when, year
from pyspark.sql.types import FloatType, IntegerType, StringType, TimestampType

from helperFunctions import (clean_date, clean_latitude_longitude,
                             explore_dataframe, split_extract)
from i94MetadataMappings import (cit_and_res_codes, mode_codes, ports_codes,
                                 visa_codes)
from qualityTests import (check_data_type, check_greater_that_zero,
                          check_unique_keys, ensure_no_nulls)

#user defined functions 

get_datetime = udf(lambda date : (timedelta(days=date) + datetime(1960,1,1)) \
                  if date > 0.0 else None, TimestampType())

cast_integer = udf(lambda val: int(val) if val != 0 else np.NaN , IntegerType())

to_split_extract_string = udf(split_extract, StringType())

to_split_extract_float = udf(split_extract,FloatType())

cast_lat_lon = udf(clean_latitude_longitude, FloatType())




def create_spark_session():
    """
    Function to create a spark session with hadoop 2.7.0 aws 
    configuration under the hood
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", \
                "org.apache.hadoop:hadoop-aws:2.7.0, saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport()\
        .getOrCreate()
    
    return spark

def process_immigration_data(spark_context, input_path, output_path):
    """
    Function to read immigration and process immigration data and 
    output immigration fact table and airlines dimension table
    as parquet
    """
    
    #read the data
    immigration_file_path = input_path + "sas_data"
    df_immigration_data = spark_context.read.parquet(immigration_file_path)
    
    #cleaning the arrival and departure dates, and if dates are null assigning 0 date
    df_immigration_data = df_immigration_data\
                            .withColumn("arrdate", get_datetime(df_immigration_data.arrdate))\
                            .withColumn("depdate", when((df_immigration_data.depdate.isNull()) , 0.0)\
                            .otherwise(df_immigration_data.depdate))

    df_immigration_data = df_immigration_data\
                            .withColumn("depdate",get_datetime(df_immigration_data.depdate))

    #converting double columns to integer
    float_immi_columns = {_[0]:_[1] for _ in df_immigration_data.dtypes if _[1] == 'double'}
    columns = [_ for _ in float_immi_columns.keys()]
    df_immigration_data = df_immigration_data.fillna(0, subset=columns)
    for _ in columns:
        df_immigration_data = df_immigration_data.withColumn(_, cast_integer(df_immigration_data[_]))

    #extracting unique flights data and creating a flights table with the unique identifier
    df_flights_data = df_immigration_data.selectExpr("airline as airline_code","fltno as flight_number")\
                               .dropDuplicates()\
                               .na\
                               .drop(subset=["airline_code","flight_number"])
    
    #creating a window to generate a serialised column
    window = Window.orderBy(col("airline_code"), col("flight_number"))
    df_flights_data = df_flights_data.withColumn("flight_id", row_number().over(window) )


    
    #joining the immigration data with flights and only keeping the flight_id
    df_immigration_data = df_flights_data.selectExpr("airline_code as airline",\
                                                     "flight_number as fltno",\
                                                     "flight_id")\
                                          .join(df_immigration_data,\
                                                on=["airline", "fltno"],
                                                how="left")

    df_immigration_data = df_immigration_data.selectExpr("cicid",\
                             "i94cit as city_code",\
                             "i94res as res_code",\
                             "i94port as port",\
                             "arrdate as arr_date",\
                             "i94mode as mode",\
                             "i94addr as addr",\
                             "depdate as dept_date",\
                             "i94bir as age",\
                             "i94visa as visa_code",\
                             "count as counter",\
                             "matflag as matflag",\
                             "biryear as birth_year",\
                             "gender",\
                             "flight_id",\
                             "i94yr as date_year",\
                             "i94mon as date_month",\
                             "visatype as visa_type",\
                             "admnum as admission_number") 

    #filtering out any other years and where dept date is null
    df_immigration_data = df_immigration_data.filter("year(dept_date) = 2016 or dept_date IS NULL")
    
    #filtering out where departure dates are less than arrival dates
    df_immigration_data = df_immigration_data.withColumn("diff", datediff("dept_date","arr_date"))\
                                        .filter("diff >= 0 or dept_date is NULL")\
                                        .drop("diff")
    
    #creating a temporary view temp_date_view of all unique arrival and departure dates
    df_immigration_data.select("arr_date","dept_date")\
            .filter("dept_date IS NOT NULL")\
            .dropDuplicates(subset=["arr_date","dept_date"])\
            .createOrReplaceTempView("temp_date_view")
    
    #Lets perform quality tests for immigration and flights data

    print("Running quality test for immigration_fct table")
    check_unique_keys(df_immigration_data, "immigration_fct", "cicid")
    ensure_no_nulls(df_immigration_data, "cicid")
    check_data_type(df_immigration_data, "dept_date", "TimestampType")
    check_data_type(df_immigration_data, "arr_date", "TimestampType")
    check_greater_that_zero(df_immigration_data)

    print("\nRunning quality test for airlines_dm table")
    check_unique_keys(df_flights_data, "airlines_dm", "flight_id")
    ensure_no_nulls(df_flights_data, "flight_id")
    check_greater_that_zero(df_flights_data)

    #outputting the immigration partitioned by year and month and outputting flights data
    df_immigration_data\
        .write.partitionBy("date_year","date_month")\
            .parquet(output_path + "immigration_fact", 'overwrite')
    
    df_flights_data\
        .write.parquet(output_path + "flights", 'overwrite')



def process_airports_data(spark_context, input_path, output_path):
    """
    Function to read in airports data and process the data using spark
    and output as airports dimension table data as parquet
    """
    
    #Reading the airports data and extractin latitude and longitude from coordinates column
    airports_file_path = input_path + "airport-codes_csv.csv"
    df_airports_data = spark_context.read.format('csv').option("header",True).load(airports_file_path)

    #Dropping the null values for iata_code
    df_airports_data = df_airports_data.dropna(subset=["iata_code"])

    #filtering out 0 iata_code and any airport type that is closed
    df_airports_data = df_airports_data.filter((col("iata_code")!="0") & (col("type") != "closed"))\
                            .dropDuplicates(subset=["iata_code","type"])    

    #Cleaning duplicated IATA codes dont exist in i94PORT codes
    df_airports_data = df_airports_data.filter(col("iata_code")\
                            .isin(["MRE","YMX","SGL","RCH","PRM","LHG"]) == False)
    df_airports_data = df_airports_data.withColumn("region_code", \
                                            to_split_extract_string(df_airports_data\
                                                .iso_region,lit("-"),lit(1)))\
                            .withColumn("latitude", \
                                to_split_extract_float(df_airports_data\
                                    .coordinates,lit(","),lit(0),lit("float")))\
                            .withColumn("longitude", \
                                to_split_extract_float(df_airports_data\
                                    .coordinates,lit(","),lit(1),lit("float")))\
                            .drop("coordinates")

    #updating the definition of the airports data
    df_airports_data = df_airports_data.selectExpr("ident as id",\
                                         "type",\
                                         "name",\
                                         "elevation_ft",\
                                         "continent",\
                                         "iso_country",\
                                         "iso_region",\
                                         "municipality",\
                                         "gps_code",\
                                         "iata_code",\
                                         "region_code",\
                                         "latitude",\
                                         "longitude")
    
    #Running quality tests for airports data
    print("\nRunning quality test for airports_dm table")
    check_unique_keys(df_airports_data, "airports_dm", "id")
    ensure_no_nulls(df_airports_data, "id")
    check_greater_that_zero(df_airports_data)
    
    #outputting the airports data
    df_airports_data\
    .write.parquet(output_path + "airports", 'overwrite')


def process_dates_data(spark_context,output_path):
    """
    Function to read in date data from a temporary view and
    create date dimension table data and output it as parquet
    """
    
    
    query = """
           SELECT distinct date_date FROM
           (
           SELECT arr_date AS date_date FROM  temp_date_view
           UNION ALL
           SELECT dept_date AS date_date FROM temp_date_view)
        """
    df_date_data = spark_context.sql(sqlQuery=query)
    
    #adding more columns to the dates table to enable different analysis
    df_date_data = df_date_data.select("date_date",\
               dayofmonth("date_date").alias("day"),\
               weekofyear("date_date").alias("week"),\
               month("date_date").alias("month"),\
               date_format("date_date", 'MMMM').alias("month_name"),\
               year("date_date").alias("year"),\
               dayofweek("date_date").alias("day_of_week"),\
               date_format("date_Date", 'E').alias("day_of_week_name"))
    
    #Running quality tests for dates dimension table
    print("\nRunning quality test for immi_dates_dm table")
    check_unique_keys(df_date_data, "immi_dates_dm", "date_date")
    ensure_no_nulls(df_date_data, "date_date")
    check_data_type(df_date_data, "date_date", "TimestampType")
    check_greater_that_zero(df_date_data)
    
    #outputting the dates data
    df_date_data\
    .write.parquet(output_path + "dates", 'overwrite')


def process_climate_data(spark_context, input_path, output_path):
    """Function to read in temperature data and regions dimension data
    and output as regions aggregation with monthly averages of temperatures
    as a regions dimension table data in a parquet format
    """
    
    #read in climate and global temperature data
    climate_file_path = input_path + "GlobalLandTemperaturesByCity.csv"
    df_temperature_data = spark_context.read.format("csv").option("header",True).load(climate_file_path)
    
    #Filtering the temperature data to 2000 century
    df_temperature_data = df_temperature_data.withColumn("dt",to_date("dt",'yyyy-MM-dd'))\
                                         .withColumn("month",date_format("dt","MMMM"))\
                                         .filter(df_temperature_data.dt >= "2000-01-01")
                                    
    
    #Casting the latitude and longitude to float and dropping few columns
    df_temperature_data = df_temperature_data.withColumn("Latitude",cast_lat_lon(df_temperature_data.Latitude))\
                            .withColumn("Longitude",cast_lat_lon(df_temperature_data.Longitude))\
                            .withColumn("Country", upper(df_temperature_data.Country))\
                            .drop("AverageTemperatureUncertainty","City","dt")

    #Aggregating the data to country and month, first and then to the country level below
    df_temperature_data = df_temperature_data.groupby("Country", "month")\
                            .agg(mean("AverageTemperature").alias("AverageTemperature"),\
                                 mean("Latitude").alias("Latitude"),\
                                 mean("Longitude").alias("Longitude"))

    df_temperature_lat_lon = df_temperature_data.groupby("Country")\
                            .agg(first("Latitude").alias("Latitude"),\
                                 first("Longitude").alias("Longitude"))

    df_temperature_data = df_temperature_data.groupby("Country")\
                            .pivot("Month")\
                            .agg(mean("AverageTemperature").alias("AverageTemperature"))

    #Doing the join after all the aggregations to have data with lat lon and country aggregations of montly temperatures
    df_temperature_data = df_temperature_data.join(df_temperature_lat_lon, on="Country",how="inner")
    
    
    #Getting the regions mapping so can use with the fact immigration table 
    df_regions_data = spark_context.createDataFrame(list(map(list, cit_and_res_codes.items())),
                                               ["cit_res_code","cit_res_name"])

    df_regions_data = df_regions_data.withColumn("Country", df_regions_data.cit_res_name)

    df_regions_data = df_regions_data.join(df_temperature_data, on="Country", how="left")
    
    
    #Updating the final definition of regions data 
    df_regions_data = df_regions_data.selectExpr("cit_res_code",\
                                                 "cit_res_name",\
                                                 "Latitude as latitude",\
                                                 "Longitude as longitude",\
                                                 "January as temp_january",\
                                                 "February as temp_february",\
                                                 "March as temp_march",\
                                                 "April as temp_april",\
                                                 "May as temp_may",\
                                                 "June as temp_june",\
                                                 "July as temp_july",\
                                                 "August as temp_august",\
                                                 "September as temp_september",\
                                                 "October as temp_october",\
                                                 "November as temp_november",\
                                                 "December as temp_december")

    #Running quality tests for regions dimension table
    print("\nRunning quality test for regions_dm table")
    check_unique_keys(df_regions_data, "regions_dm", "cit_res_code")
    ensure_no_nulls(df_regions_data, "cit_res_code")
    check_greater_that_zero(df_regions_data)
    
    #outputting the regions data
    df_regions_data\
    .write.parquet(output_path + "regions", 'overwrite')


def process_demographics_data(spark_context, input_path, output_path):
    """
    Function to read in demographics data and produce aggregations for 
    state level, race level and outputting as demographics dimension 
    table data in parquet format
    """
    
    demographics_file_path = input_path + "us-cities-demographics.csv"
    df_demographics_data = spark_context.read.format("csv")\
                                .option("sep", ";")\
                                .option("header", True)\
                                .load(demographics_file_path)
    
    #Subsetting the data without race and count, and aggregating after removing duplicates
    df_demographics_state_city_data = df_demographics_data\
                                                    .dropDuplicates(subset=["City","State","State Code"])\
                                                    .groupby("State Code","State")\
                                                    .agg(mean("Median Age").alias("median_age"),\
                                                         Sum("Male Population").alias("male_population"),\
                                                         Sum("Female Population").alias("female_population"),\
                                                         Sum("Total Population").alias("total_population"),\
                                                         Sum("Number of Veterans").alias("veterans_population"),\
                                                         Sum("Foreign-born").alias("foreign_born_population"),\
                                                         mean("Average Household Size").alias("household_size_avg"))
        
    
    #Subsetting out the race data and splitting race to it individual columns, by doing a pivot
    df_race_state_city_data = df_demographics_data.select("State Code", "Race", "Count")\
                                   .groupby("State Code")\
                                   .pivot("Race")\
                                   .agg(Sum("Count"))

    #Once the data is transformed and normalised, can join and bring back together
    df_demographics_data = df_demographics_state_city_data.join(df_race_state_city_data,\
                                                                on="State Code",\
                                                                how="inner")
    
    #Updating the definition of demographics to state level so can join to fact immigration table
    df_demographics_data = df_demographics_data.select(col("State Code").alias("state_code"),\
                                    col("State").alias("state_name"),\
                                    col("median_age"),\
                                    col("male_population"),\
                                    col("female_population"),\
                                    col("total_population"),\
                                    col("veterans_population"),\
                                    col("foreign_born_population"),\
                                    col("household_size_avg"),\
                                    col("American Indian and Alaska Native")\
                                    .alias("american_indian_and_alaskan_native_population"),\
                                    col("Asian").alias("asian_population"),\
                                    col("Black or African-American")\
                                    .alias("black_or_african_american_populaton"),\
                                    col("Hispanic or Latino")\
                                    .alias("hispanic_latino_population"),\
                                    col("White").alias("white_population")\
                                    )
    #Running quality tests for demographics data
    print("\nRunning quality test for demographics_dm table")
    check_unique_keys(df_demographics_data, "demographics_dm", "state_code")
    ensure_no_nulls(df_demographics_data, "state_code")
    check_greater_that_zero(df_demographics_data)

    #outputting the demographics data
    df_demographics_data\
    .write.parquet(output_path + "demographics", 'overwrite')
    

def process_other_dimension_tables(spark_context, output_path):
    """
    Fuinction to process various mappings for immigration fact table codes
    and output as visa, ports and mode of transaport dimension table data
    in parquet format
    """
    
    #Reading the dimensions into spark dataframe from dictionary objects
    df_visa_codes = spark_context.createDataFrame(list(map(list, visa_codes.items())),
                                        ["code","travel_purpose"])
                                        
    df_ports_codes = spark_context.createDataFrame(list(map(list, ports_codes.items())),
                                         ["code", "port_name"])
                                         
    df_mode_codes = spark_context.createDataFrame(list(map(list, mode_codes.items())),
                                        ["code","travel_mode"])
    
    #Running quality checks for remaining dimensions
    print("\nRunning quality test for visas_dm table")
    check_unique_keys(df_visa_codes, "visas_dm", "code")
    ensure_no_nulls(df_visa_codes, "code")
    check_greater_that_zero(df_visa_codes)
    
    print("\nRunning quality test for ports_dm table")
    check_unique_keys(df_ports_codes, "ports_dm", "code")
    ensure_no_nulls(df_ports_codes, "code")
    check_greater_that_zero(df_ports_codes)

    print("\nRunning quality test for modes_dm table")
    check_unique_keys(df_mode_codes, "modes_dm", "code")
    ensure_no_nulls(df_mode_codes,  "code")
    check_greater_that_zero(df_mode_codes)

    #outputting the visas, ports and mode of transport data
    df_visa_codes\
    .write.parquet(output_path + "visas", 'overwrite')
    
    df_ports_codes\
    .write.parquet(output_path + "ports", 'overwrite')
    
    df_mode_codes\
    .write.parquet(output_path + "modes", 'overwrite')
    

def main():
    """
    Main funtion that initiates the spark session and 
    defines location to read data from and write data to
    using spark
    """
    
    spark = create_spark_session()
    sc = SQLContext(spark)
    input_data = "s3a://nakuldefaultest/raw/"

    #Note: The output s3 bucket should be in same region as emr cluster
    output_data = "s3://nakuldefaultest/processed/" 
    process_immigration_data(spark, input_data, output_data)    
    process_airports_data(spark, input_data, output_data)
    process_dates_data(sc, output_data)    
    process_climate_data(spark, input_data, output_data)
    process_demographics_data(spark, input_data, output_data)    
    process_other_dimension_tables(spark, output_data)
    spark.stop()

if __name__ == "__main__":
    main()


