import os

# Ensure logs directory exists
os.makedirs(os.path.join(os.path.dirname(__file__), '../logs'), exist_ok=True)

import logging
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import hashlib
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '../logs/bls_sync.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class BLSSync:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.bls_base_url = "https://download.bls.gov/pub/time.series/pr/"
        self.headers = {
            'User-Agent': 'Your Name (your.email@example.com) - Data Engineering Project'
        }
        self.sync_state_file = 'sync_state.json'
        self.sync_state = self.load_sync_state()

    def load_sync_state(self):
        """Load the last sync state from S3"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"bls_data/{self.sync_state_file}"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except:
            return {'files': {}}

    def save_sync_state(self):
        """Save the current sync state to S3"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"bls_data/{self.sync_state_file}",
                Body=json.dumps(self.sync_state),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Error saving sync state: {str(e)}")

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

    def get_file_list(self):
        """Fetch and parse list of files from BLS website"""
        try:
            response = requests.get(self.bls_base_url, headers=self.headers)
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links in the pre tag
            files = {}
            for link in soup.find('pre').find_all('a'):
                filename = link.text
                if filename.endswith('.txt') or filename.endswith('.Current') or filename.endswith('.AllData'):
                    # Get file size and last modified date
                    parent_text = link.parent.text
                    size = parent_text.split()[-2]  # File size is usually the second-to-last word
                    date = ' '.join(parent_text.split()[:3])  # First three words are usually the date
                    files[filename] = {
                        'size': size,
                        'last_modified': date
                    }
            
            logger.info(f"Found {len(files)} files in BLS directory")
            return files
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching file list: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Error parsing BLS directory: {str(e)}")
            return {}

    def get_file_hash(self, content):
        """Calculate MD5 hash of file content"""
        return hashlib.md5(content).hexdigest()

    def file_exists_in_s3(self, filename):
        """Check if file exists in S3 and get its metadata"""
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=f"bls_data/{filename}"
            )
            return {
                'exists': True,
                'etag': response.get('ETag', '').strip('"'),
                'last_modified': response.get('LastModified')
            }
        except:
            return {'exists': False}

    def download_file(self, filename):
        """Download a single file from BLS"""
        try:
            url = f"{self.bls_base_url}{filename}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading {filename}: {str(e)}")
            return None

    def upload_to_s3(self, filename, content):
        """Upload file content to S3"""
        try:
            # Calculate hash of new content
            new_hash = self.get_file_hash(content)
            
            # Check if file exists and compare hashes
            existing_file = self.file_exists_in_s3(filename)
            if existing_file['exists'] and existing_file['etag'] == new_hash:
                logger.info(f"File {filename} is unchanged, skipping upload")
                return True

            # Upload file with content hash as metadata
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"bls_data/{filename}",
                Body=content,
                Metadata={'md5': new_hash}
            )
            logger.info(f"Successfully uploaded {filename} to S3")
            return True
        except Exception as e:
            logger.error(f"Error uploading {filename} to S3: {str(e)}")
            return False

    def delete_from_s3(self, filename):
        """Delete a file from S3"""
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=f"bls_data/{filename}"
            )
            logger.info(f"Successfully deleted {filename} from S3")
            return True
        except Exception as e:
            logger.error(f"Error deleting {filename} from S3: {str(e)}")
            return False

    def sync_files(self):
        """Main sync function"""
        logger.info("Starting BLS data sync")
        
        # Test AWS connection first
        if not self.test_aws_connection():
            logger.error("AWS connection test failed. Aborting sync.")
            return
        
        # Get list of files from BLS
        current_files = self.get_file_list()
        if not current_files:
            logger.error("No files found to sync")
            return

        # Track changes
        changes = {
            'added': [],
            'updated': [],
            'deleted': []
        }

        # Process each file
        for filename, metadata in current_files.items():
            logger.info(f"Processing file: {filename}")
            
            # Download file
            content = self.download_file(filename)
            if content:
                # Check if file is new or changed
                existing_file = self.file_exists_in_s3(filename)
                if not existing_file['exists']:
                    changes['added'].append(filename)
                elif existing_file['etag'] != self.get_file_hash(content):
                    changes['updated'].append(filename)
                
                # Upload to S3
                self.upload_to_s3(filename, content)
                
                # Update sync state
                self.sync_state['files'][filename] = {
                    'hash': self.get_file_hash(content),
                    'size': metadata['size'],
                    'last_modified': metadata['last_modified']
                }

        # Check for deleted files
        for filename in list(self.sync_state['files'].keys()):
            if filename not in current_files:
                changes['deleted'].append(filename)
                self.delete_from_s3(filename)
                del self.sync_state['files'][filename]

        # Save sync state
        self.save_sync_state()

        # Log changes
        logger.info("Sync completed with the following changes:")
        logger.info(f"Added files: {changes['added']}")
        logger.info(f"Updated files: {changes['updated']}")
        logger.info(f"Deleted files: {changes['deleted']}")

def main():
    syncer = BLSSync()
    syncer.sync_files()

if __name__ == "__main__":
    main()
