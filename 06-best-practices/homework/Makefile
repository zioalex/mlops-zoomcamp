localstack:
	docker-compose -f docker-compose.yaml up -d

test_localstack_s3: localstack
	aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration
	aws s3 ls --endpoint-url=http://localhost:4566

run_batch: localhost
	export S3_ENDPOINT_URL="http://localhost:4566"
	python batch.py 2021 1


full_stack:
	docker save stg_stream_model_duration_mlops-zoomcamp | zip > stg_stream_model_duration_mlops-zoomcamp.zip
	aws s3 --endpoint-url=http://localhost:4566 cp stg_stream_model_duration_mlops-zoomcamp.zip s3://stg-mlflow-models-mlops-zoomcamp/
	# s3://stg-mlflow-models-mlops-zoomcamp/stg_stream_model_duration_mlops-zoomcamp.zip
