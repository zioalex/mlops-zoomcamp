#!/usr/bin/env python
# coding: utf-8

from datetime import datetime

import batch
import pandas as pd


def write_test_dataset_tos3(df, output_file):

    options = batch.localstack_check()

    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression=None,
        index=False,
        storage_options=options,
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

    columns = ["PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime"]

    df = pd.DataFrame(data, columns=columns)
    return df


year = 2021
month = 1

input_file = batch.get_input_path(year, month)
output_file = batch.get_output_path(year, month)

df_input = batch.read_data(input_file)
test_df = test_dataset()
write_test_dataset_tos3(test_df, output_file)

print("test_DF", test_df.info())
print("df_input", df_input.info())

assert test_df.equals(df_input)
