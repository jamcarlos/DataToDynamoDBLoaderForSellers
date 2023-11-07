import os
import boto3
import pandas as pd
from io import BytesIO
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS service clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# Define your S3 bucket name and DynamoDB table name
# BUCKET_NAME = 'sellersinternal'
TABLE_NAME = 'Seller'

# Define the mapping of file names to column mappings
# Add more mappings as needed for other files (this one is based on marketPlaces Scraping)
file_column_mappings = {
    'sellers_testing.xlsx': {
        'Seller Name': ['SellerName'],
        'Listing URL': ['SellerUrl'],
        'Headquarter': ['SellerHq'],
        'Revenue': ['SellerRevenue'],
        'Year Founded': ['SellerYear'],
        'Number of Employees': ['SellerEmployeeCount'],
        'Icon URL': ['SellerOriginalIconUrl'],  # New column
        'Category L2': ['SellerCategory'],
        'Category L3': ['SellerSubCategory']
    }
}

def batch_write_items(table_name, items):
    dynamodb.batch_write_item(RequestItems={table_name: items})

def lambda_handler(event, context):
    logger.info("Starting Lambda function execution...")

    # Get the uploaded file from the S3 event
    file_obj = event['Records'][0]
    bucket_name = file_obj['s3']['bucket']['name']
    file_key = file_obj['s3']['object']['key']

    # Get the appropriate column mapping based on the file name
    column_mapping = file_column_mappings.get(os.path.basename(file_key), None)

    try:
        # Read the XLSX file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        excel_data = response['Body'].read()

        # Parse the XLSX file using Pandas
        excel_df = pd.read_excel(BytesIO(excel_data))

        # Capture the S3 bucket upload time
        s3_upload_time = file_obj['eventTime']

        # Initialize a list to store items for batch processing
        batch_items = []

        # Iterate through each row and prepare items for insertion
        for _, row in excel_df.iterrows():
            item = {
                'SellerTimestamp': {'S': str(datetime.now())}
            }

            for excel_column, dynamodb_attributes in column_mapping.items():
                for dynamodb_attribute in dynamodb_attributes:
                    if dynamodb_attribute not in item:
                        item[dynamodb_attribute] = {'S': str(row[excel_column])}
                    else:
                        if not isinstance(item[dynamodb_attribute], list):
                            item[dynamodb_attribute] = [item[dynamodb_attribute]]
                        item[dynamodb_attribute].append({'S': str(row[excel_column])})

                if 'SellerName' in item:
                    item['SellerNameLC'] = {'S': item['SellerName']['S'].lower()}
                    seller_name = item.get('SellerName', {}).get('S', '')
                    seller_name_lc = ''.join(seller_name.split()).lower()  # Remove spaces and make lowercase
                    item['SellerId'] = {'S': seller_name_lc}

            # Append the item to the batch_items list
            batch_items.append({'PutRequest': {'Item': item}})

            # If batch_items reaches the batch size (25), start a batch write request in parallel
            if len(batch_items) == 25:
                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = [executor.submit(batch_write_items, TABLE_NAME, batch_items)]
                batch_items = []

        # Perform a final batch write request for any remaining items
        if batch_items:
            batch_write_items(TABLE_NAME, batch_items)

    except Exception as e:
        logger.error("Error: %s", e)
        return {
            'statusCode': 500,
            'body': 'Error: ' + str(e)
        }
