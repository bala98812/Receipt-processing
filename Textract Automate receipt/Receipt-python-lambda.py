import boto3
import json
import uuid
from decimal import Decimal


s3 = boto3.client('s3')
textract = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Receipts')

def lambda_handler(event, context):
    # 1. Extract S3 file info from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # 2. Call Textract AnalyzeExpense API
    try:
        response = textract.analyze_expense(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}}
        )
    except textract.exceptions.UnsupportedDocumentException as e:
        logger.error(f"Unsupported document format: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps('Unsupported document format for expense analysis')
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }

    # 3. Extract key fields
    fields = response['ExpenseDocuments'][0]['SummaryFields']
    data = {
        'receipt_id': str(uuid.uuid4()),
        'merchant_name': 'Unknown',
        'total': Decimal('0.0'),
        'date': 'Unknown'
    }

    for field in fields:
        label = field['Type'].get('Text', '').lower()
        value = field.get('ValueDetection', {}).get('Text', '')

        if 'vendor' in label or 'merchant' in label:
            data['merchant_name'] = value
        elif 'total' in label:
            try:
                data['total'] = Decimal(value.replace('$', '').replace(',', '').strip())
            except:
                data['total'] = Decimal('0.0')  # Use fallback decimal
        elif 'date' in label:
            data['date'] = value

    # 4. Save to DynamoDB
    try:
        table.put_item(Item=data)
    except Exception as e:
        logger.error(f"Error writing to DynamoDB: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error saving to database')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Receipt processed successfully')
    }
