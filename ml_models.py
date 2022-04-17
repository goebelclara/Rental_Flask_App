#Importing libraries
from pyspark.sql import SparkSession, SQLContext
import pyspark
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder, StandardScaler
from pyspark.sql.functions import log
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

import math

import os

import seaborn as sns
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

def fit_and_evaluate(new_listing):
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

    #Importing data
    df = pd.read_csv("output_data/data_vf.csv", index_col = 0)
    df_spark = spark.createDataFrame(df)
    df_spark_reduced = spark.createDataFrame(df)

    #Logarithmizing data
    skewed_columns = ["bedrooms", "bathrooms", "price", "nearest_station", "size_imputed", "mean_income"]

    for col in skewed_columns:
        df_spark = df_spark.withColumn(col, log(col))

    #One hot encoding data
    cat_columns = ["type", "postcode"]

    for col in cat_columns:
        df_spark = StringIndexer(inputCol = col, outputCol = col+"_numeric").fit(df_spark).transform(df_spark)
        df_spark = df_spark.drop(col)
        df_spark = OneHotEncoder(inputCol = col+"_numeric", outputCol = col+"_encoded_vector").fit(df_spark).transform(df_spark)
        df_spark = df_spark.drop(col+"_numeric")

    #Assembling features
    features = list(df_spark.columns)
    features.remove("lat")
    features.remove("long")
    features.remove("price")
    features.remove("address")
    features.remove("source")

    assembler = VectorAssembler(inputCols = features, outputCol = 'features')
    df_spark_transformed = assembler.setHandleInvalid("skip").transform(df_spark)

    #Centering data
    scaler = StandardScaler().setInputCol("features").setOutputCol("scaled_features")
    df_spark_transformed_scaled = scaler.fit(df_spark_transformed).transform(df_spark_transformed)

    #Splitting data
    (training_data, test_data) = df_spark_transformed_scaled.randomSplit([0.8,0.2])

    #Fitting lasso regression
    lr = LinearRegression(labelCol = 'price', 
                        featuresCol = 'scaled_features', 
                        regParam = 0.01, 
                        elasticNetParam = 1)
    lr_model = lr.fit(training_data)

    #Selecting only relevant features
    coefs = lr_model.coefficients.flatten()
    coef_features = [x["name"] for x in sorted(training_data.schema["features"].metadata["ml_attr"]["attrs"]["binary"] +
                                            training_data.schema["features"].metadata["ml_attr"]["attrs"]["numeric"], 
                                            key = lambda x: x["idx"])]
    coef_features = pd.Series(coef_features)
    relevant_coef_features = coef_features[coefs != 0]
    relevant_coef_features = [x.replace("_encoded_vector","") for x in relevant_coef_features]

    #Creating reduced version of dataframe with only relevant features

    #Logarithmizing data
    skewed_columns = ["bedrooms", "bathrooms", "price", "nearest_station", "size_imputed", "mean_income"]

    for col in skewed_columns:
        df_spark_reduced = df_spark_reduced.withColumn(col, log(col))

    #One hot encoding data
    df_spark_reduced_ohe = df_spark_reduced.select("*")

    category_list = df_spark_reduced.select("type").distinct().rdd.flatMap(lambda x:x).collect()
    original_col_list = ["type"]*len(category_list)
    exprs = [F.when(F.col("type") == cat,1).otherwise(0).alias(str(original_col)+"_"+str(cat)) for cat, original_col in zip(category_list, original_col_list)]
    df_spark_reduced_ohe = df_spark_reduced_ohe.select(exprs + df_spark_reduced_ohe.columns)

    category_list = df_spark_reduced.select("postcode").distinct().rdd.flatMap(lambda x:x).collect()
    original_col_list = ["postcode"]*len(category_list)
    exprs = [F.when(F.col("postcode") == cat,1).otherwise(0).alias(str(original_col)+"_"+str(cat)) for cat, original_col in zip(category_list, original_col_list)]
    df_spark_reduced_ohe = df_spark_reduced_ohe.select(exprs + df_spark_reduced_ohe.columns)

    cat_columns = ["type", "postcode"]

    for col in cat_columns:
        df_spark_reduced_ohe = df_spark_reduced_ohe.drop(col)

    #Assembling features
    assembler = VectorAssembler(inputCols = relevant_coef_features, outputCol = 'features')
    df_spark_reduced_transformed = assembler.setHandleInvalid("skip").transform(df_spark_reduced_ohe)

    #Centering data
    scaler = StandardScaler().setInputCol("features").setOutputCol("scaled_features")
    df_spark_reduced_transformed_scaled = scaler.fit(df_spark_reduced_transformed).transform(df_spark_reduced_transformed)

    #Splitting data
    (training_data, test_data) = df_spark_reduced_transformed_scaled.randomSplit([0.8,0.2])

    #Fitting random forest
    rf = RandomForestRegressor(labelCol = 'price', 
                            featuresCol = 'scaled_features',
                            numTrees = 60,
                            featureSubsetStrategy = 'onethird',
                            maxDepth = 25)
    rf_model = rf.fit(training_data)

    #Evaluating random forest on test data
     
    #Transforming user input data 
    new_listing_list = new_listing.split(",")
    columns = ["type",
           "bedrooms",
           "bathrooms",
           "nearest_station",
           "postcode",
           "size_imputed"]
    new_listing_dict = {key: [value.strip()] for key, value in zip(columns, new_listing_list)}
    df_new_listing = pd.DataFrame(data = new_listing_dict)

    #Adding all relevant features
    for feature in relevant_coef_features:
        if feature not in df_new_listing.columns:
            df_new_listing[feature] = 0
    
    #One hot encoding type
    type = df_new_listing["type"][0]
    col_name = "type_"+type
    try:
        df_new_listing[col_name].iloc[0,:] = 1
    except:
        df_new_listing = df_new_listing
    df_new_listing.drop("type", axis = 1, inplace = True)

    #One hot encoding postcode
    postcode = df_new_listing["postcode"][0]
    col_name = "postcode_"+postcode
    try:
        df_new_listing[col_name].iloc[0,:] = 1
    except:
        df_new_listing = df_new_listing
    df_new_listing.drop("postcode", axis = 1, inplace = True)

    #Adding postcode-specific features
    relevant_pcd_features = [x for x in relevant_coef_features]
    relevant_listing_features = ['bedrooms', 'bathrooms', 'nearest_station', 'size_imputed',
                                'type_flat', 'type_maisonette', 'type_semi-detached house',
                                'type_detached house', 'type_penthouse', 'type_property', 
                                'type_mews house', 'type_end terrace house', 'type_duplex',
                                'type_terraced house', 'type_house']
    pcd_binary = [x for x in relevant_coef_features if "postcode_" in x]
    for feature in relevant_listing_features:
        if feature in relevant_pcd_features:
            relevant_pcd_features.remove(feature)
    for feature in pcd_binary:
        if feature in relevant_pcd_features:
            relevant_pcd_features.remove(feature)
    postcode_data = df[df["postcode"] == postcode].iloc[0,:]
    postcode_data = postcode_data[relevant_pcd_features]
    for col, value in zip(postcode_data.index, postcode_data.values):
        df_new_listing.loc[0,col] = value

    #Changing datatypes
    df_new_listing["bedrooms"] = df_new_listing["bedrooms"].astype('float').astype('Int64')
    df_new_listing["bathrooms"] = df_new_listing["bathrooms"].astype('float').astype('Int64')
    df_new_listing["nearest_station"] = df_new_listing["nearest_station"].astype('float')
    df_new_listing["size_imputed"] = df_new_listing["size_imputed"].astype('float').astype('Int64')
    
    #Creating Spark dataframe
    df_new_listing_spark = spark.createDataFrame(df_new_listing)

    #Assembling data
    assembler = VectorAssembler(inputCols = relevant_coef_features, outputCol = 'features')
    df_new_listing_transformed = assembler.setHandleInvalid("skip").transform(df_new_listing_spark)

    #Centering data
    df_new_listing_transformed_scaled = scaler.fit(df_spark_reduced_transformed).transform(df_new_listing_transformed)

    #Generating prediction
    predictions = rf_model.transform(df_new_listing_transformed_scaled)
    logged_prediction = predictions.select("prediction").toPandas().values[0][0]
    unlogged_prediction = math.exp(logged_prediction)
    proposed_price = str(round(unlogged_prediction))[0:-3] + "'" + str(round(unlogged_prediction))[-4:-1] + "£ per month"

    return proposed_price