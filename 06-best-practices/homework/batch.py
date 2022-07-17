#!/usr/bin/env python
# coding: utf-8

import sys
import os
from datetime import datetime
import pickle
import pandas as pd


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
    
    
def read_data(filename, options={}):
    df = pd.read_parquet(filename, storage_options=options)
    
    return df

    
def prepare_data(df, categorical):
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def get_input_path(year, month):
    default_input_pattern = 'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

 
def main(year, month):
    year = int(year)
    month = int(month)
    
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)
    
    
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ['PUlocationID', 'DOlocationID']

    #input_file = f'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    # output_file = f's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    #output_file = f'taxi_type=fhv_year={year:04d}_month={month:02d}.parquet'
    
    options = localstack_check()

    df = read_data(input_file, options)
    df = prepare_data(df, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(output_file, engine='pyarrow', index=False,storage_options=options)
    

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])