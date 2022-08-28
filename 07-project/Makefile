MLFLOW := $(shell pgrep mlflow)
PREFECT := $(shell pgrep prefect)

all: start_prefect_server
	echo "Start everything"

prepare_setup:
	@-mkdir data models models_pickle
	pipenv sync
	if [ -z ${VIRTUAL_ENV} ]; then  echo "start the pipenv environment with **pipenv shell** before using the make file"; fi

start_mlflow: prepare_setup
	if [ "$(MLFLOW)" = "" ]; then \
		 echo "Start mlflow"; \
		 (cd ~/github/mlops-zoomcamp/07-project; mlflow ui --backend-store-uri sqlite:///mlflow.db --serve-artifacts --artifacts-destination ./artifacts &); \
	else \
	 	echo "mlflow process ${MLFLOW} already running"; \
	fi


start_prefect_server: start_mlflow
	if [ "$(PREFECT)" = "" ]; then \
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
	@-pkill -f prefect

stop_mlflow:
	echo "Stop Mlflow"
	@-pkill -f mlflow

stop_all: stop_prefect stop_mlflow

cleanup_mlflow_db: stop_mlflow
	rm -Rf ./mlflow.db

cleanup_prefect_db: stop_prefect
	# prefect orion database reset
	rm -Rf .prefect/

prefect_flow: prefect_client_sidec
	python prefect_flow.py

prefect_deployment:
	prefect deployment create prefect_deploy.py

mongo_db_flask:
	(cd monitoring/p; docker-compose -f docker-compose.yml up --build)

drop_alembic_table:
	# When you got:
	# alembic.util.exc.CommandError: Can't locate revision identified by '061c7e518b40'
	# sqlite> SELECT * FROM alembic_version;
	# bd07f7e963c5                                                                        
	sqlite3 ~/.prefect/orion.db "DROP  TABLE alembic_version ;"
	# or sqlite3 ~/.prefect/orion.db "UPDATE alembic_version SET version_num = 'bd07f7e963c5' WHERE version_num = '061c7e518b40';"
	# or rm -Rf ~/.prefect/orion.db

send_data: # send data to predcton service and save to MongoDB
	(cd monirotng/prefect_monitoring; python send_data.py)

prefect_monitoring:
	# starting all with the pipenv in cd monirotng/prefect_monitoring
	(cd monirotng/prefect_monitoring; python prefect_monitoring_project.py)
