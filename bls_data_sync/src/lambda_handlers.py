import os
import json
import logging
import boto3
from datetime import datetime
from bls_data_sync import sync_bls_data
from population_data import fetch_population_data
from data_analysis import analyze_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

def data_sync_handler(event, context):
    """
    Lambda handler for data sync (Parts 1 & 2)
    """
    try:
        # Get environment variables
        bucket_name = os.environ['S3_BUCKET_NAME']
        queue_url = os.environ['SQS_QUEUE_URL']

        # Sync BLS data (Part 1)
        logger.info("Starting BLS data sync...")
        sync_bls_data(bucket_name)
        logger.info("BLS data sync completed")

        # Fetch population data (Part 2)
        logger.info("Fetching population data...")
        population_data = fetch_population_data()
        
        # Upload population data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key='population_data.json',
            Body=json.dumps(population_data)
        )
        logger.info("Population data uploaded to S3")

        # Send message to SQS queue
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                'timestamp': datetime.now().isoformat(),
                'event': 'data_sync_completed'
            })
        )
        logger.info("Message sent to SQS queue")

        return {
            'statusCode': 200,
            'body': json.dumps('Data sync completed successfully')
        }

    except Exception as e:
        logger.error(f"Error in data sync: {str(e)}")
        raise

def analysis_handler(event, context):
    """
    Lambda handler for data analysis (Part 3)
    """
    try:
        # Get environment variables
        bucket_name = os.environ['S3_BUCKET_NAME']

        # Process each record from SQS
        for record in event['Records']:
            # Parse the message body
            message = json.loads(record['body'])
            logger.info(f"Processing message: {message}")

            # Run the analysis
            results = analyze_data(bucket_name)

            # Log the results
            logger.info("Analysis results:")
            logger.info(json.dumps(results, indent=2))

        return {
            'statusCode': 200,
            'body': json.dumps('Analysis completed successfully')
        }

    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}")
        raise 