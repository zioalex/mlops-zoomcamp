import json
import uuid
from datetime import datetime

import requests
import pyarrow.parquet as pq

table = (
    pq.read_table("./data/cleaned_data_set.parquet")
    .to_pandas()
    .sample(n=5000, random_state=42)
)  # 5000 rows sampled
data = table[['Partner ISO', 'Commodity Code', 'Year', 'Trade Value (US$)']]


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


with open("target.csv", 'w') as f_target:
    for index, row in data.iterrows():
        row['id'] = str(uuid.uuid4())
        # duration = (
        #     row['lpep_dropoff_datetime'] - row['lpep_pickup_datetime']
        # ).total_seconds() / 60
        #if duration >= 1 and duration <= 60:
        f_target.write(f"{row['id'],row['Trade Value (US$)']}\n")
        
        resp = requests.post(
            "http://127.0.0.1:9696/predict-trade",
            headers={"Content-Type": "application/json"},
            data=row.to_json(),
        ).json()
        print(f"prediction: {resp['data']['trade_value']}")
