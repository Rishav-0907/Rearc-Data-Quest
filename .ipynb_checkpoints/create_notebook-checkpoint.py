import nbformat as nbf

# Create a new notebook
nb = nbf.v4.new_notebook()

# Add markdown cell
nb.cells.append(nbf.v4.new_markdown_cell("""# BLS Data Analysis

This notebook contains the analysis of BLS data and population data, including:
1. Population statistics for years 2013-2018
2. Best years analysis for each series_id
3. Combined report for PRS30006032 and Q01"""))

# Add code cell for imports
nb.cells.append(nbf.v4.new_code_cell("""import os
import logging
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, when, max, mean, stddev, struct, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import boto3
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()"""))

# Add code cell for Spark initialization
nb.cells.append(nbf.v4.new_code_cell("""# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("BLS Data Analysis") \\
    .getOrCreate()

# Initialize S3 client
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
temp_dir = tempfile.mkdtemp()"""))

# Add markdown cell for Population Statistics section
nb.cells.append(nbf.v4.new_markdown_cell("## 1. Population Statistics (2013-2018)"))

# Add code cell for population analysis
nb.cells.append(nbf.v4.new_code_cell("""# Read population data
population_df = spark.read \\
    .option("multiline", "true") \\
    .json("population_data.json")

# Filter for years 2013-2018
filtered_df = population_df.filter(
    (col("year") >= 2013) & (col("year") <= 2018)
)

# Calculate statistics
stats = filtered_df.select(
    mean("value").alias("mean_population"),
    stddev("value").alias("stddev_population")
)

stats.show()"""))

# Add markdown cell for Best Years section
nb.cells.append(nbf.v4.new_markdown_cell("## 2. Best Years Analysis"))

# Add code cell for best years analysis
nb.cells.append(nbf.v4.new_code_cell("""# Read BLS data
bls_df = spark.read \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .option("delimiter", "\\t") \\
    .csv("bls_data.csv")

# Group by series_id and year, sum the values
yearly_sums = bls_df.groupBy("series_id", "year") \\
    .agg(sum("value").alias("yearly_sum"))

# Find the year with maximum sum for each series_id
best_years = yearly_sums.groupBy("series_id") \\
    .agg(
        max(struct(col("yearly_sum"), col("year")))["year"].alias("best_year"),
        max("yearly_sum").alias("max_value")
    )

best_years.show()"""))

# Add markdown cell for Combined Report section
nb.cells.append(nbf.v4.new_markdown_cell("## 3. Combined Report (PRS30006032, Q01)"))

# Add code cell for combined report
nb.cells.append(nbf.v4.new_code_cell("""# Filter for specific series_id and period
filtered_bls = bls_df.filter(
    (col("series_id") == "PRS30006032") & 
    (col("period") == "Q01")
)

# Rename columns in population_df to avoid ambiguity
pop_df = population_df.withColumnRenamed("value", "Population").withColumnRenamed("year", "pop_year")

# Join with population data
combined_df = filtered_bls.join(
    pop_df,
    filtered_bls.year == pop_df.pop_year,
    "left"
).select(
    filtered_bls["series_id"],
    filtered_bls["year"],
    filtered_bls["period"],
    filtered_bls["value"],
    col("Population")
)

combined_df.show()"""))

# Write the notebook to a file
with open('data_analysis.ipynb', 'w') as f:
    nbf.write(nb, f) 