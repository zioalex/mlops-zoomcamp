Q1 -

def main(year, month):
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


Q2 - __init__.py
Q3 - 4
Q4 - --endpoint-url=http://localhost:4566
Q5 - 3504 2021-01.parquet
Q6 - 16.25
