#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import os
import pandas as pd

import batch

def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
]

columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']

df = pd.DataFrame(data, columns=columns)


df.to_parquet("test_data.parquet", engine='pyarrow', index=False)


df_read = batch.read_data("test_data.parquet")

def test_batch():
    print("This is the DF Info", df.info(), df.head())
    print(f'DF rows {df.shape}')

    assert df.equals(df_read)