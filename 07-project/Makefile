start_mlflow:
	(cd ~/github/mlops-zoomcamp/07-project; mlflow ui --backend-store-uri sqlite:///mlflow.db --serve-artifacts --artifacts-destination ./artifacts &)

start_prefect_server: start_mlflow
	(cd ~/github/mlops-zoomcamp/07-project; prefect config set PREFECT_ORION_UI_API_URL="http://localhost:4200/api"; prefect orion start --host 0.0.0.0 &)

prefect_client_side: start_mlflow
# prefect_client_side: start_prefect_server
	(cd ~/github/mlops-zoomcamp/07-project; prefect config set PREFECT_API_URL="http://localhost:4200/api"; prefect config view)
