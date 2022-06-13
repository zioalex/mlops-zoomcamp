import pandas as pd

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
import pickle
import datetime

@task
def read_data(path):
    df = pd.read_parquet(path)
    # print(df.head())
    return df

@task
def prepare_features(df, categorical, train=True):
    
    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    # print("################ INFO",df.head, df.columns)
    mean_duration = df.duration.mean()
    if train:
        print(f"The mean duration of training is {mean_duration}")
    else:
        print(f"The mean duration of validation is {mean_duration}")
    
    df[categorical] = df[categorical].fillna(-1).astype('str').astype('str')
    # print(categorical == ['PULocationID', 'DOLocationID'])
    # # print(categorical)
    # # df.info
    return df

@task
def train_model(df, categorical):
  
    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts) 
    y_train = df.duration.values

    print(f"The shape of X_train is {X_train.shape}")
    print(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    print(f"The MSE of training is: {mse}")
    return lr, dv

@task
def run_model(df, categorical, dv, lr):
    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts) 
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    print(f"The MSE of validation is: {mse}")
    return

@task
def get_paths(date):
    # https://pythonguides.com/convert-a-string-to-datetime-in-python/
    # https://stackoverflow.com/questions/9724906/python-date-of-the-previous-month
    logger = get_run_logger("logger")
    
    if date == None:
        # train_time = strftime("%Y-%m-%d", gmtime())
        # val_time
        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        val_date = lastMonth.strftime("%Y-%m")
        first = lastMonth.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        train_date = lastMonth.strftime("%Y-%m")
    else:
        format = "%Y-%m-%d"
        dt_object = datetime.datetime.strptime(date, format)
        first = dt_object.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        val_date = lastMonth.strftime("%Y-%m")
        first = lastMonth.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        train_date = lastMonth.strftime("%Y-%m")
        
    train_path: str="./data/green_tripdata_" + train_date + ".parquet"
    val_path: str="./data/green_tripdata_" + val_date + ".parquet"
    print(train_path, val_path)    
    
    return train_path, val_path
@flow(task_runner=SequentialTaskRunner())
def main(date=None):

    categorical = ['PULocationID', 'DOLocationID']
    train_path, val_path = get_paths(date).result()
    df_train = read_data(train_path)
    print(train_path, val_path, df_train)
    df_train_processed = prepare_features(df_train, categorical)#.result()

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr)
    with open('models/model-' + date + '.bin', 'wb') as f_out:
        pickle.dump((dv, lr), f_out)
    with open('models/dv-' + date + '.b', 'wb') as f_out:
        pickle.dump(dv, f_out)

main(date="2021-08-15")
