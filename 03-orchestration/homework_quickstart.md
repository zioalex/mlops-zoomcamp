port 4200 Udp/tcp for Prefect

# Download the needed files from:
    https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page


# PreReq
## Start MLFLOW
    cd ~/github/mlops-zoomcamp/02-experiment-tracking
    mlflow ui --backend-store-uri sqlite:///mlflow.db --serve-artifacts --artifacts-destination ./artifacts
## Install prefect
    pip install prefect==2.0b5

## Set dir from previous exercise
    ln -s ../02-experiment-tracking/data/
    ln -s ../02-experiment-tracking/models/ .
    ln -s ../02-experiment-tracking/mlflow.db
    ln -s ../02-experiment-tracking/mlruns/ .
    
## Start the server config
    cd 03-orchestration/
    prefect config set PREFECT_ORION_UI_API_URL="http://localhost:4200/api"
    prefect orion start --host 0.0.0.0

## On client side
    prefect config set PREFECT_API_URL="http://localhost:4200/api"
    prefect config view

If PREFECT_API URL config is not set correctly: 
    prefect config unset PREFECT_API_URL     
    prefect config set PREFECT_API_URL="http://localhost:4200/api"
    prefect config view
    prefect storage ls

## Set the storage
Select the local storage for the test purpose
    prefect storage create
    prefect storage ls

## Start a simple flow
    python prefect_flow.py

# Create the deployment
    prefect  deployment create prefect_deploy.py 

# Create the Work queue on the GUI

# Start the work queue
prefect work-queue preview ba5b0352-75f6-4e91-8781-e6f093b69f46
prefect agent start ba5b0352-75f6-4e91-8781-e6f093b69f46