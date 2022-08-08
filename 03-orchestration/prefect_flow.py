import pandas as pd
import pickle
from pendulum import date

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.metrics import mean_squared_error

import xgboost as xgb

from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll import scope

import mlflow

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from time import strftime,gmtime
import datetime

@task
def read_dataframe(filename):

    df = pd.read_parquet(filename)

    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    return df

@task
def add_features(df_train, df_val):
    # df_train = read_dataframe(train_path)
    # df_val = read_dataframe(val_path)
    logger = get_run_logger("logger")
    logger.info(len(df_train))
    logger.info(len(df_val))

    df_train['PU_DO'] = df_train['PULocationID'] + '_' + df_train['DOLocationID']
    df_val['PU_DO'] = df_val['PULocationID'] + '_' + df_val['DOLocationID']

    categorical = ['PU_DO'] #'PULocationID', 'DOLocationID']
    numerical = ['trip_distance']

    dv = DictVectorizer()

    train_dicts = df_train[categorical + numerical].to_dict(orient='records')
    X_train = dv.fit_transform(train_dicts)

    val_dicts = df_val[categorical + numerical].to_dict(orient='records')
    X_val = dv.transform(val_dicts)

    target = 'duration'
    y_train = df_train[target].values
    y_val = df_val[target].values

    return X_train, X_val, y_train, y_val, dv

@task
def train_model_search(train, valid, y_val):
    def objective(params):
        with mlflow.start_run():
            mlflow.set_tag("model", "xgboost")
            mlflow.log_params(params)
            booster = xgb.train(
                params=params,
                dtrain=train,
                num_boost_round=100,
                evals=[(valid, 'validation')],
                early_stopping_rounds=50
            )
            y_pred = booster.predict(valid)
            rmse = mean_squared_error(y_val, y_pred, squared=False)
            mlflow.log_metric("rmse", rmse)

        return {'loss': rmse, 'status': STATUS_OK}

    search_space = {
        'max_depth': scope.int(hp.quniform('max_depth', 4, 100, 1)),
        'learning_rate': hp.loguniform('learning_rate', -3, 0),
        'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
        'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
        'min_child_weight': hp.loguniform('min_child_weight', -1, 3),
        'objective': 'reg:linear',
        'seed': 42
    }

    best_result = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,
        max_evals=1,
        trials=Trials()
    )
    return

@task
def train_best_model(train, valid, y_val, dv):
    with mlflow.start_run():

        best_params = {
            'learning_rate': 0.09585355369315604,
            'max_depth': 30,
            'min_child_weight': 1.060597050922164,
            'objective': 'reg:linear',
            'reg_alpha': 0.018060244040060163,
            'reg_lambda': 0.011658731377413597,
            'seed': 42
        }

        mlflow.log_params(best_params)

        booster = xgb.train(
            params=best_params,
            dtrain=train,
            num_boost_round=100,
            evals=[(valid, 'validation')],
            early_stopping_rounds=50
        )

        y_pred = booster.predict(valid)
        rmse = mean_squared_error(y_val, y_pred, squared=False)
        mlflow.log_metric("rmse", rmse)

        with open("models/preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)
        mlflow.log_artifact("models/preprocessor.b", artifact_path="preprocessor")

        mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")
        return booster

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
    # , train_path: str="./data/green_tripdata_2021-01.parquet",
        # val_path: str="./data/green_tripdata_2021-02.parquet"
    train_path, val_path = get_paths(date).result()
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("nyc-taxi-experiment")
    X_train = read_dataframe(train_path)
    X_val = read_dataframe(val_path)
    X_train, X_val, y_train, y_val, dv = add_features(X_train, X_val).result()
    train = xgb.DMatrix(X_train, label=y_train)
    valid = xgb.DMatrix(X_val, label=y_val)
    model_search = train_model_search(train, valid, y_val)
    model_best = train_best_model(train, valid, y_val, dv)
    
    # with open('models/model_search-' + date + '.bin', 'wb') as f_out:
    #     pickle.dump((dv, model_search), f_out)
    with open('models/model-' + date + '.bin', 'wb') as f_out:
        pickle.dump((dv, model_best), f_out)
    with open('models/dv-' + date + '.b', 'wb') as f_out:
        pickle.dump(dv, f_out)

main(date="2021-07-15")