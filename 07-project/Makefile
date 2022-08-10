MLFLOW := $(shell pgrep mlflow)
PREFECT := $(shell pgrep prefect)

all: start_prefect_server
	echo "Start everything"

prepare_setup:
	@-mkdir data models models_pickle
	pipenv sync
	if [ -z ${VIRTUAL_ENV} ]; then  echo "start the pipenv environment with **pipenv shell** before using the make file"; fi

start_mlflow: prepare_setup
	if [ "$(MLFLOW)" = "  " ]; then \
		 echo "Start mlflow"; \
		 (cd ~/github/mlops-zoomcamp/07-project; mlflow ui --backend-store-uri sqlite:///mlflow.db --serve-artifacts --artifacts-destination ./artifacts &); \
	else \
	 	echo "mlflow process ${MLFLOW} already running"; \
	fi


start_prefect_server: start_mlflow
	if [ "$(PREFECT)" = "  " ]; then \
		echo "Start mlflow"; \
		(cd ~/github/mlops-zoomcamp/07-project; prefect config set PREFECT_ORION_UI_API_URL="http://localhost:4200/api"; prefect orion start --host 0.0.0.0 &); \
	else \
	 	echo "prefect process $(PREFECT) already running"; \
	fi

prefect_client_side: start_prefect_server
	(cd ~/github/mlops-zoomcamp/07-project; prefect config set PREFECT_API_URL="http://localhost:4200/api"; prefect config view)

prefect_storage: prefect_client_side
	echo "Select local storage with path ./prefect and set it as default"
	prefect storage create
	prefect storage ls

stop_prefect:
	echo "Stop prefect"

stop_mlflow:
	echo "Stop Mlflow"

stop_all: stop_prefect stop_mlflow

cleanup_prefect_db:
	# prefect orion database reset
	rm -Rf /home/asurace/.prefect/*
