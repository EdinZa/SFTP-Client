import boto3
import io
import os
import paramiko
from botocore.exceptions import ClientError

def lambda_handler(event, context):

    # Extract parameters from the event JSON
    secret_name = event['secret_name']
    region_name = event['region_name']
    bucket_name = event['bucket_name']
    s3_key = event['s3_key']
    sftp_host = event['sftp_host']
    sftp_user = event['sftp_user']
    sftp_path = event['sftp_path']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Get the private key string
    private_key_str = get_secret_value_response['SecretString']

    # Create a file-like buffer for the private key string
    keyfile = io.StringIO(private_key_str)

    # Load the private key for SFTP authentication
    private_key = paramiko.RSAKey.from_private_key(keyfile)

    # Set up the SFTP client
    transport = paramiko.Transport((sftp_host, 22))
    transport.connect(username=sftp_user, pkey=private_key)
    sftp = transport.open_sftp()

    # Retrieve the S3 object
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, s3_key)

    # Upload the file to SFTP
    sftp.putfo(obj.get()['Body'], os.path.join(sftp_path, s3_key))

    # Close the SFTP client
    sftp.close()
    transport.close()

    return {
        'statusCode': 200,
        'body': 'File uploaded successfully to SFTP server.'
    }
