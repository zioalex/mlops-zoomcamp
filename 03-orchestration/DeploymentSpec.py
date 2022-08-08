#!/home/asurace/anaconda3/bin/python

from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner

DeploymentSpec(
    name="cron-schedule-deployment",
    flow_location="./prefect_flow.py",
    flow_runner=SubprocessFlowRunner(),
    schedule=CronSchedule(
        cron="0 9 15 * *",
        timezone="Europe/Zurich"),
)

