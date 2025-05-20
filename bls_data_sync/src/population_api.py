import os
import logging
import requests
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '../logs/population_api.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class PopulationAPI:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        # Using the World Bank API for population data
        self.api_base_url = "https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL"
        self.headers = {
            'User-Agent': 'Your Name (your.email@example.com) - Data Engineering Project'
        }

    def test_aws_connection(self):
        """Test AWS S3 connection"""
        try:
            response = self.s3_client.list_buckets()
            logger.info("Successfully connected to AWS S3")
            logger.info(f"Available buckets: {[bucket['Name'] for bucket in response['Buckets']]}")
            
            try:
                self.s3_client.head_bucket(Bucket=self.bucket_name)
                logger.info(f"Successfully accessed bucket: {self.bucket_name}")
                return True
            except Exception as e:
                logger.error(f"Error accessing bucket {self.bucket_name}: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"Error connecting to AWS: {str(e)}")
            return False

    def fetch_population_data(self, start_year=2013, end_year=2018):
        """Fetch population data from World Bank API"""
        try:
            # Construct API URL with parameters
            url = f"{self.api_base_url}?format=json&date={start_year}:{end_year}"
            logger.info(f"Fetching data from: {url}")

            # Make API request
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raise exception for bad status codes

            # Parse JSON response
            data = response.json()
            
            # Extract relevant data
            population_data = []
            for item in data[1]:  # The actual data is in the second element
                population_data.append({
                    'year': item['date'],
                    'value': item['value'],
                    'country': item['country']['value']
                })

            logger.info(f"Successfully fetched {len(population_data)} years of population data")
            return population_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching population data: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error processing population data: {str(e)}")
            return None

    def save_to_s3(self, data):
        """Save population data to S3 as JSON"""
        try:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"population_data_{timestamp}.json"

            # Convert data to JSON string
            json_data = json.dumps(data, indent=2)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"population_data/{filename}",
                Body=json_data,
                ContentType='application/json'
            )

            logger.info(f"Successfully saved population data to S3: {filename}")
            return True

        except Exception as e:
            logger.error(f"Error saving population data to S3: {str(e)}")
            return False

    def process_population_data(self):
        """Main function to process population data"""
        logger.info("Starting population data processing")

        # Test AWS connection
        if not self.test_aws_connection():
            logger.error("AWS connection test failed. Aborting process.")
            return

        # Fetch population data
        population_data = self.fetch_population_data()
        if not population_data:
            logger.error("Failed to fetch population data")
            return

        # Save to S3
        if self.save_to_s3(population_data):
            logger.info("Population data processing completed successfully")
        else:
            logger.error("Failed to save population data to S3")

def main():
    processor = PopulationAPI()
    processor.process_population_data()

if __name__ == "__main__":
    main() 