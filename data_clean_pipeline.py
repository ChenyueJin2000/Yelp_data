# Databricks notebook source
# not sure if this is needed
# from pyspark.sql import SparkSession

#spark = SparkSession.builder \
#    .appName("My Spark Session") \
#    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Json Files

# COMMAND ----------

business_raw_data = spark.read.json("/FileStore/tables/yelp_data/yelp_academic_dataset_business.json")
tip_raw_data = spark.read.json("/FileStore/tables/yelp_data/yelp_academic_dataset_tip.json")
checkin_raw_data = spark.read.json("/FileStore/tables/yelp_data/yelp_academic_dataset_checkin.json")

# COMMAND ----------

# combine 3 user dataset
user_raw_data1 = spark.read.json("/FileStore/tables/yelp_data/yelp_user11.json")
user_raw_data2 = spark.read.json("/FileStore/tables/yelp_data/yelp_user12-1.json")
user_raw_data3 = spark.read.json("/FileStore/tables/yelp_data/yelp_user2.json")
user_raw_data = user_raw_data1.union(user_raw_data2).union(user_raw_data3)
# Test if the row number is right after combination
print(user_raw_data.count() == user_raw_data1.count() + user_raw_data2.count() + user_raw_data3.count())

# COMMAND ----------

# combine 6 review dataset
review_raw_data1 = spark.read.json("/FileStore/tables/yelp_data/yelp_review11.json")
review_raw_data2 = spark.read.json("/FileStore/tables/yelp_data/yelp_review12.json")
review_raw_data3 = spark.read.json("/FileStore/tables/yelp_data/yelp_review21.json")
review_raw_data4 = spark.read.json("/FileStore/tables/yelp_data/yelp_review22.json")
review_raw_data5 = spark.read.json("/FileStore/tables/yelp_data/yelp_review31.json")
review_raw_data6 = spark.read.json("/FileStore/tables/yelp_data/yelp_review32.json")
review_raw_data = review_raw_data1.union(review_raw_data2).union(review_raw_data3).union(review_raw_data4).union(review_raw_data5).union(review_raw_data6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets info

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import lit

# Get row number and col names
def get_info(df, name):
    return Row(
        dataset_name=name, 
        num_rows=df.count(), 
        column_names=", ".join(df.columns)
    )

datasets = {
    'Business Dataset': business_raw_data,
    'Tip Dataset': tip_raw_data,
    'Checkin Dataset': checkin_raw_data,
    'User Dataset': user_raw_data,
    'Review Dataset': review_raw_data
}

# Get info using function
rows = [get_info(df, name) for name, df in datasets.items()]

# Create a new DataFrame
info_df = spark.createDataFrame(rows)
display(info_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning (raw to bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create new functions for further cleaning

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_unixtime, unix_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType

def change_data_type(df: DataFrame, col_name: str, new_type: str) -> DataFrame:
    """
    Change the data type of a column in the DataFrame.

    Parameters:
    - df: Input DataFrame
    - col_name: Name of the column to be changed
    - new_type: The desired data type (as a string: 'string', 'int', 'double', 'date', 'timestamp', etc.)

    Returns:
    - DataFrame with the column's data type changed
    """

    type_mapping = {
        'string': StringType(),
        'int': IntegerType(),
        'double': DoubleType(),
        'date': DateType(),
        'boolean': BooleanType(),
        'timestamp': TimestampType()
        # ... you can add more types as needed
    }

    if new_type not in type_mapping:
        raise ValueError(f"Type {new_type} not supported. Choose from: {list(type_mapping.keys())}")

    # Special handling for 'date' and 'timestamp' due to the format of your date string
    if new_type == 'date':
        df = df.withColumn(col_name, from_unixtime(unix_timestamp(col_name, 'yyyy-MM-dd HH:mm:ss')).cast(DateType()))
    elif new_type == 'timestamp':
        df = df.withColumn(col_name, from_unixtime(unix_timestamp(col_name, 'yyyy-MM-dd HH:mm:ss')).cast(TimestampType()))
    else:
        df = df.withColumn(col_name, df[col_name].cast(type_mapping[new_type]))

    return df


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, sum

def check_negative_and_null_values(df: DataFrame, columns: list) -> DataFrame:
    """
    Check specified columns in a DataFrame for negative values and null values.
    
    Parameters:
    - df: Input DataFrame
    - columns: List of column names to check

    Returns:
    DataFrame with columns for negative and null counts
    """
    negative_checks = [sum(when(col(c) < 0, 1).otherwise(0)).alias(f"{c}_negative_count") for c in columns]
    null_checks = [sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_null_count") for c in columns]
    
    return df.select(*(negative_checks + null_checks))


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

def clean_columns_text(df: DataFrame, columns: list):
    """Clean multiple columns in a DataFrame.

    Parameters:
    - df: Input DataFrame
    - columns: List of column names to be cleaned

    Returns:
    - DataFrame with cleaned columns
    """

    for column_name in columns:
        df = df.withColumn(column_name, regexp_replace(col(column_name), '[^a-zA-Z0-9]', ''))
    return df

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def drop_negative_rows(df: DataFrame, cols: list) -> DataFrame:
    """
    Drop rows from a DataFrame based on negative values in specified columns.

    Parameters:
    - df: Input DataFrame
    - cols: List of column names to check for negative values

    Returns:
    - DataFrame with rows having negative values in specified columns dropped
    """

    for column in cols:
        df = df.filter(col(column) >= 0)
    return df


# COMMAND ----------

from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower, regexp_replace

def prepare_text_for_tfidf(df: DataFrame, text_col: str) -> DataFrame:
    """
    Prepares the specified text column for TF-IDF conversion.
    Not the final version, maybe add fuctions for stemming in the future.
    
    The function performs the following operations.
    1. Lowercase and performs word splitting on the text.
    2. removes stopwords and punctuation.
    
    Args.
    - df (DataFrame): The input DataFrame.
    - text_col (str): The name of the text column to be cleaned up.
    
    Returns: df (DataFrame): The input DataFrame.
    DataFrame: The cleaned up DataFrame where the original text columns are replaced with the cleaned up text.
    """
    
    # Convert all text to lowercase
    df = df.withColumn(text_col, lower(df[text_col]))
    
    # Remove punctuation
    df = df.withColumn(text_col, regexp_replace(df[text_col], "[^a-zA-Z\s]", ""))
    
    # splitting words
    tokenizer = Tokenizer(inputCol=text_col, outputCol="words_temp")
    words_df = tokenizer.transform(df)
    
    # Remove stopwords
    stop_words_remover = StopWordsRemover(inputCol="words_temp", outputCol="filtered_words_temp")
    filtered_df = stop_words_remover.transform(words_df)
    
    # Replace cleaned-up text and delete temporary columns
    final_df = filtered_df.withColumn(text_col, filtered_df["filtered_words_temp"]) \
                          .drop("words_temp", "filtered_words_temp")
    
    return final_df



# COMMAND ----------

# MAGIC %md
# MAGIC ## Business dataset cleaning

# COMMAND ----------

# Check data type before
business_raw_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import split, explode

def business_data_cleaning(df):
    """
    Clean raw json file for yelp business data. 
    Steps including flattern data, remove unwanted columns, check data types，
    check non-reasonable negative/null values, and drop dulicate rows.

    Parameters:
    - df (DataFrame): The DataFrame to clean.

    Returns:
    cleaned_df (DataFrame): A clean dataFrame for yelp business info.
    result (DataFrame): DataFrame with specific columns for negative and null counts.
    """

    # Flattern "attributes","categories", "hours" columns in business dataset
    # Extract only "ByAppointmentOnly" for "attribute" col
    df = df.withColumn("ByAppointmentOnly", col("attributes")["ByAppointmentOnly"]) 
    df = df.withColumn("categories", explode(split(df["categories"], ", ")))
    #df = df.select("*", 
    #               col("hours.Friday").alias("Friday_hours"),
    #               col("hours.Monday").alias("Monday_hours"),
    #               col("hours.Saturday").alias("Saturday_hours"),
    #               col("hours.Sunday").alias("Sunday_hours"),
    #               col("hours.Thursday").alias("Thursday_hours"),
    #               col("hours.Tuesday").alias("Tuesday_hours"),
    #               col("hours.Wednesday").alias("Wednesday_hours")) \
        #      .drop("hours")
    
    # Select needed columns
    df = df.select("business_id", "name", "address", "city", "state", "postal_code", "stars", "review_count", "categories",
                   "ByAppointmentOnly"
                   ) 
    
    # Check data type
    df = change_data_type(df, 'review_count', 'int')
    df = change_data_type(df, 'ByAppointmentOnly', 'boolean')

    # Clean text data
    string_cols = ["business_id", "city", "state", "postal_code", "categories"]
    df = clean_columns_text(df, string_cols)
    
    # Check non-reasonable negative/null values for numerical data
    numeric_cols = ["stars", "review_count"]
    result = check_negative_and_null_values(df, numeric_cols)

    # Drop negative rows
    df = drop_negative_rows(df, numeric_cols)

    # Remove duplicate rows and sort
    cleaned_df = df.dropDuplicates().sort("business_id")

    return cleaned_df, result


# COMMAND ----------

# Use cleaning function
business_cleaned_data, business_num_result = business_data_cleaning(business_raw_data)
business_cleaned_data.display()

# COMMAND ----------

# the result of negative and null values for numerical data
business_num_result.display()

# COMMAND ----------

business_cleaned_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkin dataset cleaning

# COMMAND ----------

checkin_raw_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count

def checkin_data_cleaning(df):
    """
    Clean raw json file for yelp checkin data. 
    Steps including flattern data, remove unwanted columns, change data types，
    generate new columns, and drop dulicate rows.

    Parameters:
    - df (DataFrame): The DataFrame to clean.

    Returns:
    agg_df (DataFrame): A clean dataFrame for yelp checkin info.
    """

    # Flattern "date" and rename
    df = df.withColumn("date", explode(split(checkin_raw_data["date"], ", "))).withColumnRenamed("date", "checkin_date")

    # Clean text data
    string_cols = ["business_id"]
    df = clean_columns_text(df, string_cols)

    # Remove duplicate rows 
    df = df.dropDuplicates()

    # Group by business_id and count the number of checkin_date
    agg_df = df.groupBy("business_id").agg(count("checkin_date").alias("checkin_count"))

    # Check data type
    #df = change_data_type(df, 'checkin_date', 'timestamp')
    agg_df = change_data_type(agg_df, 'checkin_count', 'int')

    return agg_df.sort("business_id")


# COMMAND ----------

checkin_cleaned_data = checkin_data_cleaning(checkin_raw_data)
checkin_cleaned_data.display()

# COMMAND ----------

checkin_cleaned_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review dataset cleaning

# COMMAND ----------

review_raw_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

def review_data_cleaning(df):
    """
    Clean raw json file for yelp review data. 
    Steps including remove unwanted columns, check data types，
    check non-reasonable negative/null values, 
    generate new columns, and drop dulicate rows.

    Parameters:
    - df (DataFrame): The DataFrame to clean.

    Returns:
    cleaned_df (DataFrame): A clean dataFrame for yelp review info.
    result (DataFrame): DataFrame with specific columns for negative and null counts.
    agg_df (DataFrame): DataFrame with new columns 
    """

    # Flttern data (no columns needed)

    # Select needed columns and rename 
    df = df.select("review_id","user_id","business_id","stars","useful","funny","cool","text","date").withColumnRenamed("date", "review_date")
    
    # Check data type
    df = change_data_type(df, 'useful', 'int')
    df = change_data_type(df, 'funny', 'int')
    df = change_data_type(df, 'cool', 'int')
    df = change_data_type(df, 'review_date', 'timestamp')

    # Clean text data
    string_cols = ["review_id","user_id","business_id"]
    df = clean_columns_text(df, string_cols)
    #df = prepare_text_for_tfidf(df, "text")
    
    # Check non-reasonable negative/null values for numerical data
    numeric_cols = ["stars", "useful","funny","cool"]
    result = check_negative_and_null_values(df, numeric_cols)

    # Drop negative rows
    df = drop_negative_rows(df, numeric_cols)

    # Remove duplicate rows
    cleaned_df = df.dropDuplicates().sort("business_id")

    # Generate new columns
    agg_df = cleaned_df.groupBy("business_id").agg(countDistinct("user_id").alias("num_of_user_who_reviewed")).sort("business_id")
    agg_df = change_data_type(agg_df, 'num_of_user_who_reviewed', 'int')

    # Join the original df with the aggregated df to add the new column
    #final_df = df.join(agg_df, on="business_id", how="left")

    return cleaned_df, agg_df, result 


# COMMAND ----------

# Use cleaning function
review_cleaned_data, reviewed_user_count_df, review_num_result = review_data_cleaning(review_raw_data)
review_cleaned_data.display()

# COMMAND ----------

reviewed_user_count_df.display()

# COMMAND ----------

# the result of negative and null values for numerical data
review_num_result.display()
# To be clear, even the reslut shows we have negative values, we have already drop those rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tip dataset cleaning

# COMMAND ----------

tip_raw_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count

def tip_data_cleaning(df):
    """
    Clean raw json file for yelp tip data. 
    Steps including remove unwanted columns, check data types，
    check non-reasonable negative/null values, 
    generate new columns, and drop dulicate rows.

    Parameters:
    - df (DataFrame): The DataFrame to clean.

    Returns:
    cleaned_df (DataFrame): A clean dataFrame for yelp review info.
    result (DataFrame): DataFrame with specific columns for negative and null counts.
    agg_df (DataFrame): DataFrame with new columns 
    """

    # Flttern data (no columns needed)

    # Select needed columns and rename 
    df = df.select("user_id","business_id","text","date","compliment_count").withColumnRenamed("date", "tip_date")

    # Remove duplicate rows
    df = df.dropDuplicates().sort("business_id")

    # Check data type
    df = change_data_type(df, 'compliment_count', 'int')
    df = change_data_type(df, 'tip_date', 'timestamp')

    # Clean text data
    string_cols = ["user_id","business_id"]
    df = clean_columns_text(df, string_cols)
    #df = prepare_text_for_tfidf(df, "text")
    
    # Check non-reasonable negative/null values for numerical data
    numeric_cols = ["compliment_count"]
    result = check_negative_and_null_values(df, numeric_cols)

    # Drop negative rows
    cleaned_df = drop_negative_rows(df, numeric_cols)

    # Generate new attributes
    agg_df = cleaned_df.groupBy("business_id").agg(count("user_id").alias("tip_count")).sort("business_id")
    agg_df = change_data_type(agg_df, 'tip_count', 'int')

    # Join the original df with the aggregated df to add the new column
    #final_df = cleaned_df.join(agg_df, on="business_id", how="left")

    return cleaned_df, agg_df, result 


# COMMAND ----------

# Use cleaning function
tip_cleaned_data, tip_count_df, tip_num_result = tip_data_cleaning(tip_raw_data)
tip_cleaned_data.display()

# COMMAND ----------

# the result of negative and null values for numerical data
tip_num_result.display()

# COMMAND ----------

tip_count_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## User dataset cleaning

# COMMAND ----------

user_raw_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import split, explode

def user_data_cleaning(df):
    """
    Clean raw json file for yelp review data. 
    Steps including flattern data, remove unwanted columns, check data types，
    check non-reasonable negative/null values, and drop dulicate rows.

    Parameters:
    - df (DataFrame): The DataFrame to clean.

    Returns:
    cleaned_df (DataFrame): A clean dataFrame for yelp user info.
    result (DataFrame): DataFrame with specific columns for negative and null counts.
    """
    
    # Flattern "elite", "friends" columns in business dataset
    df = df.withColumn("elite", explode(split(df["elite"], ", ")))
    df = df.withColumn("friends", explode(split(df["friends"], ", ")))

    # Select needed columns and rename
    df = df.select("user_id","name","review_count","yelping_since","average_stars",
                   #"useful", "funny", "cool",
                   "elite","friends","fans") \
        .withColumnRenamed("review_count", "user_review_count") \
        .withColumnRenamed("average_stars", "user_average_stars") \
        .withColumnRenamed("name", "user_name")
    
    # Remove duplicate rows
    df = df.dropDuplicates().sort("user_id")

    # Check data type
    df = change_data_type(df, 'user_review_count', 'int')
    #df = change_data_type(df, 'useful', 'int')
    #df = change_data_type(df, 'funny', 'int')
    #df = change_data_type(df, 'cool', 'int')
    df = change_data_type(df, 'fans', 'int')
    df = change_data_type(df, 'yelping_since', 'date')

    # Clean text data
    string_cols = ["user_id","friends"]
    df = clean_columns_text(df, string_cols)
    
    # Check non-reasonable negative/null values for numerical data
    numeric_cols = ["user_review_count", "user_average_stars", "fans"]
    result = check_negative_and_null_values(df, numeric_cols)

    # Drop negative rows
    cleaned_df = drop_negative_rows(df, numeric_cols)

    return cleaned_df, result


# COMMAND ----------

# Use cleaning function
user_cleaned_data, user_num_result = user_data_cleaning(user_raw_data)
user_cleaned_data.display()

# COMMAND ----------

# the result of negative and null values for numerical data
user_num_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Join all sub sources into one unified table

# COMMAND ----------

from pyspark.sql import functions as F

# Frist, Select and generate needed columns for futher join
business_table = business_cleaned_data.select("business_id", "name", "city", "state", "postal_code", "stars", "review_count","categories") \
    .dropDuplicates().withColumnRenamed("name", "business_name")
review_table = review_cleaned_data.select("user_id", "business_id", "stars", "review_date") \
    .groupBy("user_id", "business_id").agg(
        F.avg("stars").alias("avg_stars_single_user"),
        F.max("review_date").alias("latest_review_date")
)
user_table = user_cleaned_data.select("user_id", "name", "yelping_since") \
    .dropDuplicates().withColumnRenamed("name", "user_name")

# COMMAND ----------

# Join
df1 = tip_count_df.join(checkin_cleaned_data, on="business_id", how="left_outer")
df2 = df1.join(reviewed_user_count_df, on="business_id", how="left_outer")
df3 = df2.join(business_table, on="business_id", how="left_outer")
df4 = df3.join(review_table,on="business_id", how="left_outer")
final_df = df4.join(user_table, on="user_id", how="left_outer").dropDuplicates()

final_df.display()

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #  “Delta lake process” within SCD type II
# MAGIC (bronze to sliver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and reload Delta tables

# COMMAND ----------

from pyspark.sql.functions import lit, current_date, date_format
from delta.tables import DeltaTable

# Add "start_date", "end_date", and "flag" to fianl_df.
end_date_far_future = "9999-12-31"

# Use the current_date() function to get the current date.
# Set "start_date" to the current date, "end_date" to a future date, and "flag" 1 for the new record.
final_df = (final_df.withColumn("start_date", current_date())
          .withColumn("end_date", lit(end_date_far_future))
          .withColumn("flag", lit(1)))
# Change data types
final_df = change_data_type(final_df, 'end_date', 'date')
final_df = change_data_type(final_df, 'flag', 'boolean')

table_path = "/FileStore/tables/yelp_data/yelp_final_table2"

# Convert to Delta format and save
final_df.write.format("delta").mode("overwrite").save(table_path)

# Reload Delta table
delta_data = spark.read.format("delta").load(table_path)

# COMMAND ----------

# For each cleaned dataset
# Save into Delta format
#business_cleaned_data.write.format("delta").save("/FileStore/tables/yelp_delta/business_delta_table")
#checkin_cleaned_data.write.format("delta").save("/FileStore/tables/yelp_delta/checkin_delta_table")
#tip_cleaned_data.write.format("delta").save("/FileStore/tables/yelp_delta/tip_delta_table")
#user_cleaned_data.write.format("delta").save("/FileStore/tables/yelp_delta/user_delta_table")
#review_cleaned_data.write.format("delta").save("/FileStore/tables/yelp_delta/review_delta_table")

# Reload delta tables
#business_delta_data = spark.read.format("delta").load("/FileStore/tables/yelp_delta/business_delta_table")
#checkin_delta_data = spark.read.format("delta").load("/FileStore/tables/yelp_delta/checkin_delta_table")
#tip_delta_data = spark.read.format("delta").load("/FileStore/tables/yelp_delta/tip_delta_table")
#user_delta_data = spark.read.format("delta").load("/FileStore/tables/yelp_delta/user_delta_table")
#review_delta_data = spark.read.format("delta").load("/FileStore/tables/yelp_delta/review_delta_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy SCD Type 2

# COMMAND ----------

from pyspark.sql.functions import when, col
from pyspark.sql.functions import current_timestamp

# Artificially insert a new row of data to ensure that the following can be performed
# A signle change
final_changed_df = final_df.withColumn("stars", 
                               when((col("user_id") == "9KjFil1UwYlJwq6dc6oQ") & 
                                    (col("business_id") == "Aabt8S0RtL2R0LPQ1XTCA"), 4)
                               .otherwise(col("stars")))

# Add a new row
new_data = [("newUserId", "newBusinessId", 1, 1, 1, "newBuinessName","Arlington", "Vriginia", "22209",
             1, 1, "Game", 1.1, "2019-10-29 03:36:22", "Chenyue", "2019-10-29", 
             "2023-10-11", end_date_far_future, 1)]  
new_row_df = spark.createDataFrame(new_data, final_changed_df.columns)

# Change data types
new_row_df = change_data_type(new_row_df, 'latest_review_date', 'timestamp')
new_row_df = change_data_type(new_row_df, 'yelping_since', 'date')
new_row_df = change_data_type(new_row_df, 'start_date', 'date')
new_row_df = change_data_type(new_row_df, 'end_date', 'date')
new_row_df = change_data_type(new_row_df, 'flag', 'boolean')

# Merge this new row with the original DataFrame
final_changed_df = final_changed_df.union(new_row_df)


# COMMAND ----------

# Identify changes and new records
changes = final_changed_df.alias("a"). \
    join(delta_data.alias("b"), ["user_id", "business_id"], "outer") \
  .where((col("a.latest_review_date") > col("b.latest_review_date")) | col("b.user_id").isNull()) \
  .select("a.*")

changes.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Assuming that table_path is the path where your Delta table resides
delta_table = DeltaTable.forPath(spark, table_path)

# Now you can use the merge operation on delta_table
(delta_table.alias("a")
 .merge(changes.alias("b"), "a.user_id = b.user_id AND a.business_id = b.business_id")
 .whenMatchedUpdate(set = {
   "end_date": current_date(),
   "flag": lit(0)
 })
 .whenNotMatchedInsertAll()
 .execute())


# COMMAND ----------

# MAGIC %md
# MAGIC # End

# COMMAND ----------

spark.stop()
