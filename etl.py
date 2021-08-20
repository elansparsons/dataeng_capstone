import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col
from pyspark.sql.functions import date_format
import datetime
import pandas as pd
from sql_queries import *
import psycopg2
import os
import logging

def create_spark_session():
    """
    Creates Spark session for the project.
    
    Returns spark session.
    """
    
    spark = SparkSession \
    .builder \
    .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .appName("Capstone") \
    .getOrCreate()
    
    return spark

def sas_to_parquet(spark, directory='', parquet_name=''):
    """
    Converts a directory of SAS data files into parquet format.
    
    Args:
    spark - current spark session
    directory - directory containing all SAS files to be converted
    parquet_name - name of the parquet file directory to be saved
    """
    
    # check for parquet in current directory, do not re-create if it already exists
    if parquet_name in os.listdir('.'):
        print(f"Parquet files at {parquet_name} already exist.")
        return
    
    # read in all immigration data, filter to columns wanted, append together
    filenames = os.listdir(directory)
    
    immigration_first = spark.read.format('com.github.saurfang.sas.spark').load(directory + filenames[0])
    immigration_df = immigration_first.select(['i94yr', 'i94mon', 'i94res','i94port','i94visa','cicid'])

    for filename in filenames[1:]:
        df = spark.read.format('com.github.saurfang.sas.spark').load(directory + filename)
        filtered_df = df.select(['i94yr', 'i94mon', 'i94res','i94port','i94visa','cicid'])

        immigration_df = immigration_df.union(filtered_df)
        
    #write to parquet
    immigration_df.write.parquet(parquet_name)

def process_immigration(cur, conn, spark, parquet_name=''):
    """
    Args:
    cur - database cursor
    conn - database connection object
    spark - current spark session
    parquet_name - name of the parquet file directory to be read from
    
    Returns none
    """
    # read in immigration parquet
    immigration_df = spark.read.parquet(parquet_name)
    
    # group by columns of interest
    immigration_grouped = immigration_df.groupBy(['i94yr', 'i94mon', 'i94res','i94port','i94visa']).count()

    # reorder columns for a cleaner table
    immigration_grouped = immigration_grouped.select(['i94res', 'i94visa', 'count', 'i94yr', 'i94mon', 'i94port']) \
        .withColumnRenamed('i94res', 'country_id') \
        .withColumnRenamed('i94visa', 'visa_type') \
        .withColumnRenamed('i94yr', 'year') \
        .withColumnRenamed('i94mon', 'month') \
        .withColumnRenamed('i94port', 'port')
    
    immigration_grouped_p = immigration_grouped.toPandas()
    
    # convert columns to proper types
    immigration_grouped_p['country_id'] = immigration_grouped_p['country_id'].astype('int32')
    immigration_grouped_p['visa_type'] = immigration_grouped_p['visa_type'].astype('int32')
    immigration_grouped_p['count'] = immigration_grouped_p['count'].astype('int32')
    immigration_grouped_p['year'] = immigration_grouped_p['year'].astype('int32')
    immigration_grouped_p['month'] = immigration_grouped_p['month'].astype('int32')
    
    for i, row in immigration_grouped_p.iterrows():
        cur.execute(arrivals_insert, list(row))
        
    
def process_countries(cur, conn, spark):
    """
    Args:
    cur - database cursor
    conn - database connection object
    spark - current spark session
    
    Returns: A dataframe of countries and country codes to merge with temperature data,
    and a dataframe of processed ISO codes
    """
    
    # read in iso country codes
    iso_codes = spark.read.option("header",True).option("inferSchema",True).csv('iso_2alpha.csv')
    
    # read in country codes assembled from i94 documentation (I94_SAS_Labels_Descriptions.SAS)
    countries_df = spark.read.option("header",True).option("inferSchema",True).csv('countries.csv')
    countries_df = countries_df.select(['country','code'])
    
    # fix country values so temp, country json, and iso codes match as much as possible
    # need replacement dictionaries
    countries_dict = {
        'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)': 'MEXICO',
        'INVALID: ': '',
        'BURMA': 'MYANMAR'
    }
    countries_cleaning = countries_df.na.replace(countries_dict, subset='country')

    iso_dict = {
    'CURAÇAO': 'CURACAO',
    'IRAN, ISLAMIC REPUBLIC OF': 'IRAN',
    'VIRGIN ISLANDS, BRITISH': 'BRITISH VIRGIN ISLANDS',
    'TANZANIA, UNITED REPUBLIC OF': 'TANZANIA',
    'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF': 'MACEDONIA',
    'TAIWAN, PROVINCE OF CHINA': 'TAIWAN',
    'CONGO, THE DEMOCRATIC REPUBLIC OF THE': 'CONGO',
    'VENEZUELA, BOLIVARIAN REPUBLIC OF': 'VENEZUELA',
    "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF": 'NORTH KOREA',
    'FALKLAND ISLANDS (MALVINAS)': 'FALKLAND ISLANDS',
    'VIET NAM': 'VIETNAM',
    'BRUNEI DARUSSALAM': 'BRUNEI',
    'BOLIVIA, PLURINATIONAL STATE OF': 'BOLIVIA',
    'MICRONESIA, FEDERATED STATES OF': 'MICRONESIA, FED. STATES OF',
    'VIRGIN ISLANDS, U.S.': 'U.S. VIRGIN ISLANDS',
    'MOLDOVA, REPUBLIC OF': 'MOLDOVA',
    'KOREA, REPUBLIC OF': 'SOUTH KOREA',
    'PALESTINE, STATE OF': 'PALESTINE',
    'SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA': 'ST. HELENA',
    'BOSNIA AND HERZEGOVINA': 'BOSNIA-HERZEGOVINA',
    'SAMOA': 'WESTERN SAMOA',
    "CÔTE D'IVOIRE": 'IVORY COAST',
    'CHINA': 'CHINA, PRC',
    'HOLY SEE (VATICAN CITY STATE)': 'HOLY SEE/VATICAN',
    'FAROE ISLANDS': 'FAROE ISLANDS (PART OF DENMARK)',
    'SAINT LUCIA': 'ST. LUCIA',
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC": 'LAOS',
    'MACAO': 'MACAU',
    'TIMOR-LESTE': 'EAST TIMOR',
    'SAINT MARTIN (FRENCH PART)': 'SAINT MARTIN',
    'SINT MAARTEN (DUTCH PART)': 'SAINT MAARTEN',
    'SAINT KITTS AND NEVIS': 'ST. KITTS-NEVIS',
    'BONAIRE, SINT EUSTATIUS AND SABA': 'BONAIRE, ST EUSTATIUS, SABA',
    'COCOS (KEELING) ISLANDS': 'COCOS ISLANDS',
    'SYRIAN ARAB REPUBLIC': 'SYRIA',
    'RUSSIAN FEDERATION': 'RUSSIA',
    'WALLIS AND FUTUNA': 'WALLIS AND FUTUNA ISLANDS',
    'NORTHERN MARIANA ISLANDS': 'MARIANA ISLANDS, NORTHERN',
    'ANTIGUA AND BARBUDA': 'ANTIGUA-BARBUDA',
    'HEARD ISLAND AND MCDONALD ISLANDS': 'HEARD AND MCDONALD IS.',
    'SAINT BARTHÉLEMY': 'SAINT BARTHELEMY',
    'MAYOTTE': 'MAYOTTE (AFRICA - FRENCH)',
    'RÉUNION': 'REUNION',
    'BOUVET ISLAND': 'BOUVET ISLAND (ANTARCTICA/NORWAY TERR.)',
    'SAINT VINCENT AND THE GRENADINES': 'ST. VINCENT-GRENADINES',
    'SAINT PIERRE AND MIQUELON': 'ST. PIERRE AND MIQUELON',
    'UNITED STATES MINOR OUTLYING ISLANDS': 'MINOR OUTLYING ISLANDS - USA',
    'SOUTH SUDAN': 'REPUBLIC OF SOUTH SUDAN',
    'BRITISH INDIAN OCEAN TERRITORY': 'INDIAN OCEAN TERRITORY'
}

    iso_cleaning = iso_codes.select("*", upper(col('Name')))
    iso_cleaning = iso_cleaning.replace(iso_dict, subset='Name') \
        .withColumnRenamed('upper(Name)', 'country') \
        .withColumnRenamed('Code', '2acode')


    # reorder columns for clean table
    countries_cleaning = countries_cleaning.select(['code', 'country'])
    
    # Convert tables to pandas for Postgres insertion
    countries_cleaning_p = countries_cleaning.toPandas()
    
    for i, row in countries_cleaning_p.iterrows():
        cur.execute(countries_insert, list(row))
    
    return countries_cleaning, iso_cleaning
    
    
def process_temp(cur, conn, spark, countries_cleaning, filepath=''):
    """
    Args:
    cur - database cursor
    conn - database connection object
    spark - current spark session
    countries_cleaning - dataframe of country names and codes to standardize across dataset
    filepath - path to data file to read from
    
    Returns none
    """
    
    # read in world temperature data -- measurements listed in C
    temp = spark.read.option("header",True).option("inferSchema",True).csv(filepath)
    
    # fix country values so temp, country json, and iso codes match as much as possible
    # need replacement dictionaries
    temp_dict = {
                'BOSNIA AND HERZEGOVINA':'BOSNIA-HERZEGOVINA',
                'CHINA': 'CHINA, PRC',
                "CÔTE D'IVOIRE": 'IVORY COAST',
                'GUINEA BISSAU': 'GUINEA-BISSAU',
                'CONGO (DEMOCRATIC REPUBLIC OF THE)': 'CONGO',
                'BURMA': 'MYANMAR'
    }
    temp_cleaning = temp.select("*", upper(col('Country'))).drop('Country')
    temp_cleaning = temp_cleaning.na.replace(temp_dict, subset='upper(Country)').withColumnRenamed('upper(Country)', 'country')

    # convert timestamp from type object to datetime for easy year/month extraction
    temp_cleaning = temp_cleaning.withColumn('month', date_format('dt', 'M'))
    temp_cleaning = temp_cleaning.withColumn('year', date_format('dt', 'y'))

    # find avg temp per country per month
    temp_avg = temp_cleaning.select(['AverageTemperature', 'country', 'year', 'month']) \
        .groupBy(['country', 'year', 'month']).mean()
    
    # drop null avg temps, not useful
    temp_avg = temp_avg.na.drop(subset='avg(AverageTemperature)')

    # calculate F for Americans
    temp_avg = temp_avg.withColumn('AverageTemperatureF', (col('avg(AverageTemperature)') * 9/5) + 32) \
        .withColumnRenamed('avg(AverageTemperature)', 'AverageTemperature')

    # get country codes for temperature data table insertion & filtering
    temp_avg_ccode = temp_avg.join(countries_cleaning, 'country')

    # drop extra US code (407 wanted)
    temp_avg_ccode = temp_avg_ccode.filter(temp_avg_ccode.code != 583)

    # limit to the last 10 years of the dataset for relevance (2012 is max full year)
    temp_avg_ccode = temp_avg_ccode.filter(temp_avg_ccode.year >= 2002)

    # filter and reorder columns for cleaner table
    temp_avg_ccode = temp_avg_ccode.select(['code', 'year', 'month', 'AverageTemperature', 'AverageTemperatureF']) \
            .withColumnRenamed('AverageTemperature', 'avg_temp') \
            .withColumnRenamed('AverageTemperature', 'avg_tempF') 
    
    # Convert tables to pandas for Postgres insertion
    temp_avg_ccode_p = temp_avg_ccode.toPandas()
    
    # convert columns to proper types
    temp_avg_ccode_p['year'] = temp_avg_ccode_p['year'].astype('int32')
    temp_avg_ccode_p['month'] = temp_avg_ccode_p['month'].astype('int32')
    
    for i, row in temp_avg_ccode_p.iterrows():
        cur.execute(temp_insert, list(row))
    
    
def process_airports(cur, conn, spark, countries_cleaning, iso_cleaning, filepath=''):
    """
    Args:
    cur - database cursor
    conn - database connection object
    spark - current spark session
    countries_cleaning - dataframe of country names and codes to standardize across dataset
    iso_cleaning - dataframe of ISO country codes
    filepath - path to data file to read from
    
    Returns none
    """
    
    # read in airports data
    codes = spark.read.option("header",True).option("inferSchema",True).csv(filepath)
    
    # slice to columns of interest, to establish where each airport is
    codes_filtered = codes.select(['ident', 'municipality', 'iso_country', 'iso_region']) \
        .withColumnRenamed('iso_country', '2acode')

    # merge with iso_codes and countries df to get country name and standard id across the dataset
    codes_iso = codes_filtered.join(iso_cleaning, on='2acode')

    codes_countries = codes_iso.join(countries_cleaning, on='country')

    # drop extra US code (407 wanted)
    codes_countries = codes_countries.filter(codes_countries.code != 583)

    # filter and reorder columns for clean table
    codes_countries = codes_countries.select(['ident', 'municipality', 'code', 'iso_region']) \
        .withColumnRenamed('ident', 'port') \
        .withColumnRenamed('code', 'country_id') \
        .withColumnRenamed('iso_region', 'region')
    
    codes_countries_p = codes_countries.toPandas()
    
    for i, row in codes_countries_p.iterrows():
        cur.execute(airports_insert, list(row))
        
def quality_check(cur, conn):
    failed_checks = []
    
    for table in ['arrivals', 'airports', 'countries', 'temp']:
        count_check = f"""SELECT COUNT(*) FROM {table};"""
        cur.execute(count_check)
        row_count = cur.fetchone()[0]
        
        if table == 'arrivals' and row_count != 224941:
            failed_checks.append(table)
            print("Count check failed at arrivals table. Expected 224941, got " + row_count)
        if table == 'airports' and row_count != 24566:
            failed_checks.append(table)
            print("Count check failed at airports table. Expected 24566, got " + row_count)
        if table == 'countries' and row_count != 289:
            failed_checks.append(table)
            print("Count check failed at countries table. Expected 289, got " + row_count)
        if table == 'temp' and row_count != 21572:
            failed_checks.append(table)
            print("Count check failed at temp table. Expected 21572, got " + row_count)
    
    if len(failed_checks) == 0:
        print("All count checks passed.")
    else:
        raise ValueError("One or more count checks failed. See tables: " + str(failed_checks))

    
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=capstonedb user=student password=student")
    cur = conn.cursor()
    
    spark = create_spark_session()
    
    print("Spark session created. ETL starting.")

    countries_cleaning, iso_cleaning = process_countries(cur, conn, spark)
    process_temp(cur, conn, spark, countries_cleaning, filepath='../../data2/GlobalLandTemperaturesByCity.csv')
    process_airports(cur, conn, spark, countries_cleaning, iso_cleaning=iso_cleaning, filepath='airport-codes_csv.csv')
    sas_to_parquet(spark, directory='../../data/18-83510-I94-Data-2016/', parquet_name='sas_full_dataset')
    process_immigration(cur, conn, spark, parquet_name='sas_full_dataset')
    
    conn.commit()
    
    print("ETL complete.")
    
    quality_check(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()