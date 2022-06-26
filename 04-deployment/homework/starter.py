import os
import sys
import pickle
# from nbformat import read
import pandas as pd
import numpy as np


with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)

categorical = ['PUlocationID', 'DOlocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def get_data_from_internet(year:int, month:int):
    print(f'Downloading the file https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year:4d}-{month:02d}.parquet')
    df = read_data(f'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year:4d}-{month:02d}.parquet')

    return df


def run():
    year=int(sys.argv[1])
    month=int(sys.argv[2])
    df = get_data_from_internet(year, month)

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)



    mean = np.array(y_pred).mean()
    print(f'The prediction mean for {year:4d}-{month:02d} dataset is {mean}')


    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['prediction'] = y_pred


    output_file = "output_file.parquet"
    df_result.to_parquet(
        output_file,
        engine='pyarrow',   
        compression=None,
        index=False
    )

    file_size = os.system('ls -lh output_file.parquet')
    print(file_size)

if __name__ == '__main__':
    run()