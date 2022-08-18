from fileinput import filename
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
def read_dataframe(filename='./data/RUStoWorldTrade.csv'):
    dataframe = pd.read_csv(filename,
    usecols=['Year','Aggregate Level','Reporter ISO','Partner','Partner ISO','Commodity Code','Commodity','Qty Unit','Qty','Netweight (kg)','Trade Value (US$)'])
    dataframe = dataframe.convert_dtypes()

    # Optimize DF memory consumption
    for col in dataframe.columns:
      if dataframe[col].dtype == 'Float64':
        dataframe[col] = dataframe[col].astype('float16')
    try :
        if dataframe[col].dtype == 'Int64':
            dataframe[col] = dataframe[col].astype('int16')
    except :
        dataframe[col] = dataframe[col].astype('float16')
    dataframe.drop(dataframe[dataframe['Commodity Code'] == 'TOTAL'].index, inplace=True)
    dataframe['Commodity Code'] = dataframe['Commodity Code'].astype('float16')

    return dataframe

@task
def data_enrichment(dataframe, filename='./data/iso3.csv'):
    df = dataframe[dataframe['Aggregate Level']==2]
    iso = pd.read_csv(filename)
    iso.drop(axis=1, columns=['FIPS','ISO (2)','ISO (No)','Internet','Note','Capital'], inplace=True)

    continents = ['Asia', 'Europe', 'Africa', 'Oceania', 'Americas']
    for x in continents:
        y = iso[iso['Continent'] == x]
        m = df['Partner ISO'].isin(y['ISO (3)'])
        df.loc[m, 'Continent'] = x

    Region = ['South Asia', 'South East Europe', 'Northern Africa', 'Pacific',
        'South West Europe', 'Southern Africa', 'West Indies',
        'South America', 'South West Asia', 'Central Europe',
        'Eastern Europe', 'Western Europe', 'Central America',
        'Western Africa', 'South East Asia', 'Central Africa',
        'North America', 'East Asia', 'Indian Ocean', 'Northern Europe',
        'Eastern Africa', 'Southern Europe', 'Central Asia',
        'Northern Asia']

    for x in Region:
        y = iso[iso['Region'] == x]
        m = df['Partner ISO'].isin(y['ISO (3)'])
        df.loc[m, 'Region'] = x

    return df


@task
def prepare_df(df):
    categorical = ['Partner ISO', 'Commodity Code']
    numerical = ['Year']

    # Available years 2007 - 2020
    df_train = df.loc[(df['Year']>=2007) & (df['Year']<=2015)]
    df_valid = df.loc[(df['Year']>=2016) & (df['Year']<=2020)]

    #df = read_dataset(2008) -
    # I must convert to str otherwise the DV fail. Explain why?

    dv = DictVectorizer()
    target = 'Trade Value (US$)'

    # Prepare train data
    df_train[categorical] = df_train[categorical].astype(str)
    train_dicts = df_train[categorical + numerical].to_dict(orient='records')
    X_train = dv.fit_transform(train_dicts)
    y_train = df_train[target].values

    # Prepare validation data
    df_valid[categorical] = df_valid[categorical].astype(str)
    valid_dicts = df_valid[categorical + numerical].to_dict(orient='records')
    X_val = dv.transform(valid_dicts)
    y_val = df_valid[target].values

    return X_train, X_val, y_train, y_val, dv

# @task
# def add_features(df_train, df_val):
#     # df_train = read_dataframe(train_path)
#     # df_val = read_dataframe(val_path)
#     logger = get_run_logger("logger")
#     logger.info(len(df_train))
#     logger.info(len(df_val))

#     df_train['PU_DO'] = df_train['PULocationID'] + '_' + df_train['DOLocationID']
#     df_val['PU_DO'] = df_val['PULocationID'] + '_' + df_val['DOLocationID']

#     categorical = ['PU_DO'] #'PULocationID', 'DOLocationID']
#     numerical = ['trip_distance']

#     dv = DictVectorizer()

#     train_dicts = df_train[categorical + numerical].to_dict(orient='records')
#     X_train = dv.fit_transform(train_dicts)

#     val_dicts = df_val[categorical + numerical].to_dict(orient='records')
#     X_val = dv.transform(val_dicts)

#     target = 'duration'
#     y_train = df_train[target].values
#     y_val = df_val[target].values

#     return X_train, X_val, y_train, y_val, dv



@task
def train_model_search(train, valid, y_val):
    def objective(params):
        with mlflow.start_run():
            mlflow.set_tag("model", "xgboost")
            mlflow.set_tag("developer", "Alessandro")
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
            # mlflow.log_artifact(local_path="models/lin_reg.bin", artifact_path="models_pickle")

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
            'learning_rate': 0.08023673823188773,
            'max_depth': 5,
            'min_child_weight': 19.53568450887438,
            'objective': 'reg:linear',
            'reg_alpha': 0.026605630217501015,
            'reg_lambda': 0.0035016901628043395,
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

# @task
# def get_paths(date):
#     # https://pythonguides.com/convert-a-string-to-datetime-in-python/
#     # https://stackoverflow.com/questions/9724906/python-date-of-the-previous-month
#     logger = get_run_logger("logger")

#     if date == None:
#         # train_time = strftime("%Y-%m-%d", gmtime())
#         # val_time
#         today = datetime.date.today()
#         first = today.replace(day=1)
#         lastMonth = first - datetime.timedelta(days=1)
#         val_date = lastMonth.strftime("%Y-%m")
#         first = lastMonth.replace(day=1)
#         lastMonth = first - datetime.timedelta(days=1)
#         train_date = lastMonth.strftime("%Y-%m")
#     else:
#         format = "%Y-%m-%d"
#         dt_object = datetime.datetime.strptime(date, format)
#         first = dt_object.replace(day=1)
#         lastMonth = first - datetime.timedelta(days=1)
#         val_date = lastMonth.strftime("%Y-%m")
#         first = lastMonth.replace(day=1)
#         lastMonth = first - datetime.timedelta(days=1)
#         train_date = lastMonth.strftime("%Y-%m")

#     train_path: str="./data/green_tripdata_" + train_date + ".parquet"
#     val_path: str="./data/green_tripdata_" + val_date + ".parquet"
#     print(train_path, val_path)

#     return train_path, val_path

@flow(task_runner=SequentialTaskRunner())
def main(date=None):
    # , train_path: str="./data/green_tripdata_2021-01.parquet",
        # val_path: str="./data/green_tripdata_2021-02.parquet"
    # train_path, val_path = get_paths(date).result()
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("russiaWorldTrade")
    raw_dataframe = read_dataframe()
    df_enriched = data_enrichment(raw_dataframe)
    X_train, X_val, y_train, y_val, dv = prepare_df(df_enriched).result()
    train = xgb.DMatrix(X_train, label=y_train)
    valid = xgb.DMatrix(X_val, label=y_val)
    model_search = train_model_search(train, valid, y_val)
    model_best = train_best_model(train, valid, y_val, dv)

    # with open('models/model_search-' + date + '.bin', 'wb') as f_out:
    #     pickle.dump((dv, model_search), f_out)
    # with open('models/model-' + date + '.bin', 'wb') as f_out:
    #     pickle.dump((dv, model_best), f_out)
    # with open('models/dv-' + date + '.b', 'wb') as f_out:
    #     pickle.dump(dv, f_out)

main()
