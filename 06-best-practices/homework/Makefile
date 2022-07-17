localstack:
	docker-compose -f docker-compose.yaml up -d

test_localstack_s3: localstack
	aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration
	aws s3 ls --endpoint-url=http://localhost:4566

run_batch: localhost
	export S3_ENDPOINT_URL="http://localhost:4566"
	python batch.py 2022 2
