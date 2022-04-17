#Importing libraries
from pyspark.sql import SparkSession, SQLContext
import pyspark

import os

import pandas as pd

def write_merged_data_to_db(df):
    #Setting environment variables
    os.environ["JAVA_HOME"] = "/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
    os.environ["SPARK_HOME"] = "spark"

    #Creating Spark session
    spark = SparkSession \
        .builder \
        .appName("PySpark App") \
        .config("spark.jars", "jar_files/postgresql-42.3.2.jar") \
        .getOrCreate()

    #Creating Spark and SQL context
    sparkcontext = pyspark.SparkContext.getOrCreate()
    sqlcontext = SQLContext(sparkcontext)

    #Setting database parameters
    postgres_uri = "jdbc:postgresql://depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com:5432/claragoebel21"
    user = "claragoebel21"
    password = "qwerty123"


    #Defining functions to read from or write to database
    def read_db(db_table):
        try:
            df_spark = spark.read \
            .format("jdbc") \
            .option("url", postgres_uri) \
            .option("dbtable", db_table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
            return df_spark
        except Exception as ex:
            return ex

    def write_to_db(df_spark, db_table):
        try:
            df_spark.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", postgres_uri) \
            .option("dbtable", db_table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        except Exception as ex:
            return ex

    #Writing to database
    df_pandas = pd.DataFrame(data = df["type"].unique(), columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.types")

    #Writing to database
    df_pandas = pd.DataFrame(data = df["source"].unique(), columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.sources")

    #Writing to database
    df_pandas = df[["postcode",
                    "lat",
                    "long",
                    "pop_density",
                    "public_transport_accessibility",
                    "mean_income"]].drop_duplicates()
    df_pandas.columns = ["postcode",
                        "latitude",
                        "longitude",
                        "population_density",
                        "pt_accessibility",
                        "mean_income"]
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.postcodes")

    #Writing to database
    types = ['ages_0_15', 'ages_16_29', 'ages_30_44', 'ages_45_65', 'ages_66_plus']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_age_bins")

    #Writing to database
    types = ['couple_children_dependent', 'couple_children_nondependent', 'lone_parent', 'single', 'other_multi']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_household_types")

    #Writing to database
    types = ['owned_outright', 'owned_mortgage', 'rented_social', 'rented_private']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_dwelling_ownership")

    #Writing to database
    types = ['usual_residents', 'nonusual_residents']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_resident_types")

    #Writing to database
    types = ['house_detached', 'house_semidetached', 'house_terraced', 'flat']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_dwelling_types")

    #Writing to database
    types = ['economic_inactive', 'employee', 'self-employed', 'unemployed', 'student']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_economic_activity")

    #Writing to database
    types = ['unqualified', 'level_1', 'level_2', 'apprenticeship', 'level_3', 'level_4', 'other']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_education_level")

    #Writing to database
    types = ['good_health', 'fair_health', 'bad_health']
    df_pandas = pd.DataFrame(data = types, columns = ["name"])
    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.pcd_health_status")

    #Writing to database
    foreign_key_map = read_db("rental_schema.postcodes").toPandas()
    foreign_key_map = foreign_key_map[["id", "postcode"]]
    foreign_key_map = {postcode: value for value, postcode in zip(foreign_key_map["id"], foreign_key_map["postcode"])}

    df_pandas = df[["address", "postcode"]].drop_duplicates()
    df_pandas["postcode"] = df_pandas["postcode"].map(foreign_key_map)
    df_pandas.columns = ["full_address", "postcode_id"]

    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.address")

    #Defining function to write many-to-many postcode to category share tables to database
    def pcd_share_write_to_db(cat_table, df_columns, cat_id_column, share_table):
        foreign_key_map_pcd = read_db("rental_schema.postcodes").toPandas()
        foreign_key_map_pcd = foreign_key_map_pcd[["id", "postcode"]]
        foreign_key_map_pcd = {postcode: value for value, postcode in zip(foreign_key_map_pcd["id"], foreign_key_map_pcd["postcode"])}

        foreign_key_map_cat = read_db(cat_table).toPandas()
        foreign_key_map_cat = foreign_key_map_cat[["id", "name"]]
        foreign_key_map_cat = {name: value for value, name in zip(foreign_key_map_cat["id"], foreign_key_map_cat["name"])}

        df_pandas = df[df_columns].drop_duplicates()
        df_pandas = pd.melt(df_pandas, id_vars = df_columns[0], value_vars = df_columns[1:])
        df_pandas["postcode"] = df_pandas["postcode"].map(foreign_key_map_pcd)
        df_pandas["variable"] = df_pandas["variable"].map(foreign_key_map_cat)
        df_pandas.columns = ["postcode_id", cat_id_column, "share"]
        
        df_spark = spark.createDataFrame(df_pandas)
        write_to_db(df_spark, share_table)

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_age_bins", 
                        ['postcode', 'ages_0_15', 'ages_16_29', 'ages_30_44', 'ages_45_65', 'ages_66_plus'], 
                        "age_bin_id", 
                        "rental_schema.pcd_age_bin_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_dwelling_ownership", 
                        ['postcode', 'couple_children_dependent', 'couple_children_nondependent', 'lone_parent', 'single', 'other_multi'], 
                        "dwelling_ownership_id", 
                        "rental_schema.pcd_dwelling_ownership_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_household_types", 
                        ['postcode', 'owned_outright', 'owned_mortgage', 'rented_social', 'rented_private'], 
                        "household_type_id", 
                        "rental_schema.pcd_household_type_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_resident_types", 
                        ['postcode', 'usual_residents', 'nonusual_residents'], 
                        "resident_type_id", 
                        "rental_schema.pcd_resident_type_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_dwelling_types", 
                        ['postcode', 'house_detached', 'house_semidetached', 'house_terraced', 'flat'], 
                        "dwelling_type_id", 
                        "rental_schema.pcd_dwelling_type_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_economic_activity", 
                        ['postcode', 'economic_inactive', 'employee', 'self-employed', 'unemployed', 'student'], 
                        "economic_activity_id", 
                        "rental_schema.pcd_economic_activity_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_education_level", 
                        ['postcode', 'unqualified', 'level_1', 'level_2', 'apprenticeship', 'level_3', 'level_4', 'other'], 
                        "education_level_id", 
                        "rental_schema.pcd_education_level_share")

    #Writing to database
    pcd_share_write_to_db("rental_schema.pcd_health_status", 
                        ['postcode', 'good_health', 'fair_health', 'bad_health'], 
                        "health_status_id", 
                        "rental_schema.pcd_health_status_share")

    #Writing to database
    foreign_key_map_source = read_db("rental_schema.sources").toPandas()
    foreign_key_map_source = foreign_key_map_source[["id", "name"]]
    foreign_key_map_source = {name: value for value, name in zip(foreign_key_map_source["id"], foreign_key_map_source["name"])}

    foreign_key_map_address = read_db("rental_schema.address").toPandas()
    foreign_key_map_address = foreign_key_map_address[["id", "full_address"]]
    foreign_key_map_address = {name: value for value, name in zip(foreign_key_map_address["id"], foreign_key_map_address["full_address"])}

    foreign_key_map_type = read_db("rental_schema.types").toPandas()
    foreign_key_map_type = foreign_key_map_type[["id", "name"]]
    foreign_key_map_type = {name: value for value, name in zip(foreign_key_map_type["id"], foreign_key_map_type["name"])}

    df_pandas = df[["bedrooms", "bathrooms", "nearest_station", "price", "size_imputed", "type", "address", "source"]]
    df_pandas["source"] = df_pandas["source"].map(foreign_key_map_source)
    df_pandas["address"] = df_pandas["address"].map(foreign_key_map_address)
    df_pandas["type"] = df_pandas["type"].map(foreign_key_map_type)
    df_pandas.columns = ["bedrooms", "bathrooms", "nearest_station_distance", "price", "size_imputed", "type_id", "address_id", "source_id"]

    df_spark = spark.createDataFrame(df_pandas)
    write_to_db(df_spark, "rental_schema.listings")