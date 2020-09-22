import time
import boto3
import json
import re
import datetime
import random

import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import numpy as np
from scipy.stats import poisson

lambda_client = boto3.client('lambda')
lambda_function = 'fraud-detection-event-processor'
region = boto3.Session().region_name

def generate_metadata():
    # generates random metadata for constructing an inference payload
    millisecond_regex = r'\.\d+'
    timestamp = re.sub(millisecond_regex, '', str(datetime.datetime.now()))
    source = random.choice(['Mobile', 'Web', 'Store'])
    result = [timestamp, 'random_id', source]
    return result

def get_data_payload(test_array):
    # create API payload by combining data and metadata
    return {'data':','.join(map(str, test_array)),
            'metadata': generate_metadata()}

def invoke_endpoint(payload):
    # Gets credentials from the IAM role of the notebook instance, 
    # then uses them to create a signed request to the API Gateway
    
    auth = BotoAWSRequestsAuth(
        aws_host="1bd3rm2so6.execute-api.{}.amazonaws.com".format(region),
        aws_region=region,
        aws_service='execute-api'
    )

    invoke_url = "https://1bd3rm2so6.execute-api.{}.amazonaws.com/prod/invocations".format(region)

    response = requests.post(invoke_url, json=payload, auth=auth)
    
    return response



def get_api_response(example):
    
    data_payload = get_data_payload(example)
    response = invoke_endpoint(data_payload)
    
    print('Status code:', response.status_code)
    print('Url:', response.url)
    print('JSON response:', response.json())
    
    
    return response

    

def generate_traffic(X_test):
    while True:
        np.random.shuffle(X_test)
        for example in X_test:
            data_payload = get_data_payload(example)
            invoke_endpoint(data_payload)
            # We invoke the function according to a shifted Poisson distribution
            # to simulate data arriving at random intervals
            time.sleep(poisson.rvs(1, size=1)[0] + np.random.rand())


