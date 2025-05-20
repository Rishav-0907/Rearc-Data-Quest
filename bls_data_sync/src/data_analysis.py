import os
import logging
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, when, max, mean, stddev, struct, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import boto3
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '../logs/data_analysis.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DataAnalyzer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BLS Data Analysis") \
            .getOrCreate()
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.temp_dir = tempfile.mkdtemp()

    def download_from_s3(self, key, local_path):
        """Download a file from S3 to local path"""
        try:
            self.s3_client.download_file(self.bucket_name, key, local_path)
            logger.info(f"Successfully downloaded {key} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {key}: {str(e)}")
            return False

    def clean_column_names(self, df):
        """Clean column names by removing whitespace, tabs, and newlines"""
        # Print original column names for debugging
        logger.info(f"Original column names: {df.columns}")
        
        # Create a mapping of old column names to cleaned names
        column_mapping = {}
        for col in df.columns:
            # Remove tabs, newlines, and extra spaces
            cleaned_name = ' '.join(col.replace('\t', ' ').replace('\n', ' ').split())
            column_mapping[col] = cleaned_name
        
        # Apply the mapping
        for old_col, new_col in column_mapping.items():
            df = df.withColumnRenamed(old_col, new_col)
        
        # Print cleaned column names for debugging
        logger.info(f"Cleaned column names: {df.columns}")
        return df

    def clean_dataframe(self, df):
        """Clean both column names and values in a DataFrame"""
        # Clean column names
        df = self.clean_column_names(df)
        
        # Clean string values (trim whitespace)
        for col_name in df.columns:
            if df.schema[col_name].dataType == StringType():
                df = df.withColumn(col_name, trim(col(col_name)))
        
        return df

    def read_bls_data(self):
        """Read BLS data from S3"""
        try:
            # Download the file locally first
            local_path = os.path.join(self.temp_dir, "bls_data.csv")
            if not self.download_from_s3("bls_data/pr.data.0.Current", local_path):
                return None

            # Read the CSV file with tab delimiter
            bls_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", "\t") \
                .csv(local_path)

            # Clean the data
            bls_df = self.clean_dataframe(bls_df)
            bls_df = bls_df.withColumn("value", col("value").cast(DoubleType()))
            bls_df = bls_df.withColumn("year", col("year").cast(IntegerType()))
            
            # Print sample data for verification
            logger.info("Sample BLS data:")
            bls_df.show(5, truncate=False)
            
            logger.info("Successfully read BLS data")
            return bls_df
        except Exception as e:
            logger.error(f"Error reading BLS data: {str(e)}")
            return None

    def read_population_data(self):
        """Read population data from S3"""
        try:
            # Get the latest population data file
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="population_data/"
            )
            
            if 'Contents' not in response:
                logger.error("No population data files found in S3")
                return None

            # Sort files by LastModified and get the latest
            contents = response['Contents']
            contents.sort(key=lambda x: x['LastModified'])
            latest_file = contents[-1]
            file_key = latest_file['Key']

            # Download the file locally
            local_path = os.path.join(self.temp_dir, "population_data.json")
            if not self.download_from_s3(file_key, local_path):
                return None

            # Read the JSON file
            population_df = self.spark.read \
                .option("multiline", "true") \
                .json(local_path)
            
            # Clean the data
            population_df = self.clean_dataframe(population_df)
            
            # Print sample data for verification
            logger.info("Sample Population data:")
            population_df.show(5, truncate=False)

            logger.info("Successfully read population data")
            return population_df
        except Exception as e:
            logger.error(f"Error reading population data: {str(e)}")
            return None

    def analyze_population_data(self, population_df):
        """Analyze population data for years 2013-2018"""
        try:
            # Filter data for years 2013-2018
            filtered_df = population_df.filter(
                (col("year") >= 2013) & (col("year") <= 2018)
            )

            # Calculate mean and standard deviation
            stats = filtered_df.select(
                mean("value").alias("mean_population"),
                stddev("value").alias("stddev_population")
            )

            logger.info("Population statistics calculated successfully")
            return stats
        except Exception as e:
            logger.error(f"Error analyzing population data: {str(e)}")
            return None

    def find_best_years(self, bls_df):
        """Find the best year for each series_id"""
        try:
            # Group by series_id and year, sum the values
            yearly_sums = bls_df.groupBy("series_id", "year") \
                .agg(sum("value").alias("yearly_sum"))

            # Find the year with maximum sum for each series_id
            best_years = yearly_sums.groupBy("series_id") \
                .agg(
                    max(struct(col("yearly_sum"), col("year")))["year"].alias("best_year"),
                    max("yearly_sum").alias("max_value")
                )

            logger.info("Best years analysis completed successfully")
            return best_years
        except Exception as e:
            logger.error(f"Error finding best years: {str(e)}")
            return None

    def generate_combined_report(self, bls_df, population_df):
        """Generate report combining BLS and population data"""
        try:
            # First, verify the data exists
            logger.info("Checking for PRS30006032 in BLS data:")
            bls_df.filter(col("series_id") == "PRS30006032").show(5, truncate=False)
            
            # Filter for specific series_id and period
            filtered_bls = bls_df.filter(
                (col("series_id") == "PRS30006032") & 
                (col("period") == "Q01")
            )
            
            # Log the filtered data
            logger.info("Filtered BLS data:")
            filtered_bls.show(5, truncate=False)

            # Rename columns in population_df to avoid ambiguity
            pop_df = population_df.withColumnRenamed("value", "Population").withColumnRenamed("year", "pop_year")
            
            # Log population data
            logger.info("Population data:")
            pop_df.show(5, truncate=False)

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

            logger.info("Combined report generated successfully")
            return combined_df
        except Exception as e:
            logger.error(f"Error generating combined report: {str(e)}")
            return None

    def run_analysis(self):
        """Run all analyses"""
        logger.info("Starting data analysis")

        # Read data
        bls_df = self.read_bls_data()
        population_df = self.read_population_data()

        if bls_df is None or population_df is None:
            logger.error("Failed to read data. Aborting analysis.")
            return

        # Run analyses
        population_stats = self.analyze_population_data(population_df)
        best_years = self.find_best_years(bls_df)
        combined_report = self.generate_combined_report(bls_df, population_df)

        # Show results
        logger.info("\nPopulation Statistics (2013-2018):")
        population_stats.show()

        logger.info("\nBest Years for Each Series:")
        best_years.show()

        logger.info("\nCombined Report (PRS30006032, Q01):")
        combined_report.show()

def main():
    analyzer = DataAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main() 