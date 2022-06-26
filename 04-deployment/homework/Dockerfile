FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

WORKDIR /app
RUN pip install -U pip
RUN pip install pipenv


COPY [ "Pipfile", "Pipfile.lock", "starter.py", "./" ]
RUN pipenv install --system --deploy

ENTRYPOINT [ "python", "starter.py", "2021", "4" ]

# docker build -t mlops-zoomcamp-deplyment-week4 .
# docker  run --rm -it mlops-zoomcamp-deplyment-week4
# docker tag mlops-zoomcamp-deplyment-week4 zioalex/mlops-zoomcamp-deplyment-week4:v1
# docker push zioalex/mlops-zoomcamp-deplyment-week4:v1
