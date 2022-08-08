#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pickle
from datetime import datetime

import pandas as pd


def localstack_check():
    # S3_ENDPOINT_URL = "http://localhost:4566"
    if os.getenv("S3_ENDPOINT_URL"):
        options = {"client_kwargs": {"endpoint_url": os.getenv("S3_ENDPOINT_URL")}}
        print("Using Localstack")
    else:
        options = {}
        print("Using AWS")
    return options


def read_data(filename, options={}):
    df = pd.read_parquet(filename, storage_options=options)

    return df


def save_data(df, output_file, options={}):
    result = df.to_parquet(
        output_file, engine="pyarrow", index=False, storage_options=options
    )
    print(f"save_data {result}")


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


def prepare_data(df, categorical):
    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def get_input_path(year, month):
    default_input_pattern = "https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet"
    input_pattern = os.getenv("INPUT_FILE_PATTERN", default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = "s3://nyc-duration/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet"
    output_pattern = os.getenv("OUTPUT_FILE_PATTERN", default_output_pattern)
    return output_pattern.format(year=year, month=month)


def main(year, month):
    # pylint: disable=too-many-locals
    year = int(year)
    month = int(month)

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    with open("model.bin", "rb") as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ["PUlocationID", "DOlocationID"]

    # input_file = f'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    # output_file = f's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    # output_file = f'taxi_type=fhv_year={year:04d}_month={month:02d}.parquet'

    ## Test DF
    test_df = test_dataset()
    test_df_transformed = prepare_data(test_df, categorical)
    print("test_DF", test_df_transformed.info())

    options = localstack_check()

    df = read_data(input_file, options)
    print(df.info())
    df = prepare_data(df, categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print("predicted mean duration:", y_pred.mean())
    print("predicted duration sum:", y_pred.sum())

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predicted_duration"] = y_pred

    df_result.to_parquet(
        output_file, engine="pyarrow", index=False, storage_options=options
    )
    save_data(df_result, output_file, options)
    return y_pred.mean


def lambda_handler(event, context):
    # pylint: disable=unused-argument
    prediction = main(sys.argv[1], sys.argv[2])
    print(prediction)
    return prediction


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
