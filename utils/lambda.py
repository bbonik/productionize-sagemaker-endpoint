##############################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.   #
#                                                                            #
#  Licensed under the Amazon Software License (the "License"). You may not   #
#  use this file except in compliance with the License. A copy of the        #
#  License is located at                                                     #
#                                                                            #
#      http://aws.amazon.com/asl/                                            #
#                                                                            #
#  or in the "license" file accompanying this file. This file is distributed #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,        #
#  express or implied. See the License for the specific language governing   #
#  permissions and limitations under the License.                            #
##############################################################################
import json
import os
import random
import datetime
import re
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(event)
    metadata = event.get('metadata', None)
    assert metadata, "Request did not include metadata!"
    data_payload = event.get('data', None)
    assert data_payload, "Payload did not include a data field!"

    output = {}
    output["fraud_classifier"] = get_fraud_prediction(data_payload)

    success = store_data_prediction(output, metadata)
    return output


def get_fraud_prediction(data, threshold=0.5):
    sagemaker_endpoint_name = 'fraud-detection-endpoint'
    sagemaker_runtime = boto3.client('sagemaker-runtime')
    response = sagemaker_runtime.invoke_endpoint(EndpointName=sagemaker_endpoint_name, 
                                                 ContentType='text/csv',
                                                 Body=data)
    pred_proba = json.loads(response['Body'].read().decode())
    prediction = 0 if pred_proba < threshold else 1
    # Note: XGBoost returns a float as a prediction, a linear learner would require different handling.
    logger.info("classification pred_proba: {}, prediction: {}".format(pred_proba, prediction))

    return {"pred_proba": pred_proba, "prediction": prediction}


def store_data_prediction(output_dict, metadata):
    firehose_delivery_stream = 'fraud-detection-firehose-stream'
    firehose = boto3.client('firehose', region_name=os.environ['AWS_REGION'])

    # Extract score and classifier prediction
    fraud_pred = output_dict["fraud_classifier"]["prediction"] if 'fraud_classifier' in output_dict else ""

    record = ','.join(metadata + [str(fraud_pred)]) + '\n'

    success = firehose.put_record(DeliveryStreamName=firehose_delivery_stream, Record={'Data': record})
    logger.info("Record logged: {}".format(record))
    return success
