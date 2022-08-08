localstack:
	docker-compose -f docker-compose.yaml up -d

test_localstack_s3: localstack
	aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration
	aws s3 ls --endpoint-url=http://localhost:4566

run_batch: localhost
	export S3_ENDPOINT_URL="http://localhost:4566"
	python batch.py 2021 1

test: test_localstack_s3
	export S3_ENDPOINT_URL="http://localhost:4566"
	export INPUT_FILE_PATTERN=S3://nyc-duration/taxi_type=fhv/year=2021/month=01/predictions.parquet
	pytest tests/

prepare_lamda_function:
	#cd mylambda
	pip install --target ./packages -r requirements.txt --no-deps
	(cd packages; zip -r ../my-lambda-package.zip .)
	zip -g my-lambda-package.zip batch.py
	#pipenv lock -r | sed 's/-e //g' | pipenv run pip install --upgrade -r /dev/stdin --target python/lib/python3.9/#site-packages/
	#zip -r my-lambda-package.zip python batch.py
	rm -rf packages

	#docker save stg_stream_model_duration_mlops-zoomcamp | zip > stg_stream_model_duration_mlops-zoomcamp.zip
	#aws s3 --endpoint-url=http://localhost:4566 cp stg_stream_model_duration_mlops-zoomcamp.zip s3://stg-mlflow-models-mlops-zoomcamp/
	# s3://stg-mlflow-models-mlops-zoomcamp/stg_stream_model_duration_mlops-zoomcamp.zip

full_stack: prepare_lamda_function
	(cd infrastructure; terraform init; terraform plan -var-file=./vars/stg.tfvars; terraform apply -var-file=./vars/stg.tfvars --auto-approve)
