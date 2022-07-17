#!/usr/bin/env python
# coding: utf-8

import sys
import os
from datetime import datetime
import pickle
import pandas as pd

import batch


def localstack_check():
    # S3_ENDPOINT_URL = "http://localhost:4566"
    if os.getenv('S3_ENDPOINT_URL'): 
        options = {
            'client_kwargs': {
                'endpoint_url': os.getenv('S3_ENDPOINT_URL')
            }
        }
        print("Using Localstack")
    else:
        options = {}
        print("Using AWS")
    return options


def write_test_dataset_tos3(df, input_file):
    
    options = localstack_check()
    
    df.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
    )
 
   
def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)
    

def test_dataset():
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']

    df = pd.DataFrame(data, columns=columns)
    return df

year=2021
month=1

input_file = batch.get_input_path(year, month)
output_file = batch.get_output_path(year, month)
    
test_df = test_dataset()
write_test_dataset_tos3(test_df, input_file)
