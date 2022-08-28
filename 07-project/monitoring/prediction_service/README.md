# how to test the prediction service

The file data.json contains the feature

    {
        "Partner ISO": "ZWE",
        "Commodity Code": 68.0,
        "Year": 2018
    }


Calling the local endpoint returns the Million Dollars prediction:

    curl -X  POST -H "Content-Type: application/json" --data @data.json http://localhost:9696/predict-trade | jq
    {
      "data": {
        "Commodity Code": 68,
        "Partner ISO": "ZWE",
        "model_version": "2",
        "status": 200,
        "trade_value": 1854.0041121851355
      },
      "statusCode": 200
    }