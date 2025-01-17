- [Week1](#week1)
  - [MLOps intro by Google](#mlops-intro-by-google)
  - [Mocking / running your AWS stack locally](#mocking--running-your-aws-stack-locally)
  - [The data set used in the duration prediction notebook](#the-data-set-used-in-the-duration-prediction-notebook)
  - [Convert files from parquet to csv](#convert-files-from-parquet-to-csv)
  - [MLOps Maturity level by Microsoft](#mlops-maturity-level-by-microsoft)
  - [How to use my GPU in Jupyter notebook and Docker](#how-to-use-my-gpu-in-jupyter-notebook-and-docker)
  - [How to add an Anaconda env as Jupyter kernel](#how-to-add-an-anaconda-env-as-jupyter-kernel)
  - [Other courses](#other-courses)


# Week1
## MLOps intro by Google
https://cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning#mlops_level_2_cicd_pipeline_automation

## Mocking / running your AWS stack locally
https://localstack.cloud/

## The data set used in the duration prediction notebook
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
## Convert files from parquet to csv
https://www.youtube.com/watch?v=r94QjpX9vSE&list=PL3MmuxUbc_hIUISrluw_A7wDSmfOhErJK

## MLOps Maturity level by Microsoft
https://docs.microsoft.com/en-us/azure/architecture/example-scenario/mlops/mlops-maturity-model

## How to use my GPU in Jupyter notebook and Docker
https://cschranz.medium.com/set-up-your-own-gpu-based-jupyterlab-e0d45fcacf43
## How to add an Anaconda env as Jupyter kernel
    conda activate base
    conda install -c anaconda ipykernel
    python -m ipykernel install --user --name base-anaconda --display-name "(base) Anacoda Py 3.9.12"
    jupyter kernelspec list
    jupyter notebook

https://gdcoder.com/how-to-create-and-add-a-conda-environment-as-jupyter-kernel/
## Other courses
https://www.coursera.org/specializations/machine-learning-engineering-for-production-mlops