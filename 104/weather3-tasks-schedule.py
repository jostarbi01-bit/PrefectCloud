from datetime import timedelta
import httpx
from prefect import flow, task ,deploy
from prefect.client.schemas.schedules import CronSchedule

@task(retries=2 ,retry_delay_seconds=5, log_prints=True)
def fetch_temperature():
    """Task to fetch current temperature for Bangkok from Open-Meteo API"""
    # url = "https://open-meteo.com"
    url = "https://api.open-meteo.com/v1/forecast/"
    with httpx.Client() as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()
        return f"{data['current']['temperature_2m']}{data['current_units']['temperature_2m']}"

@task(log_prints=True)
def save_temperature(temp_str: str):
    print(f"🌡️ Current temperature recorded: {temp_str}")

# @flow( name="Bangkok-Temperature-Monitor" , log_prints=True)
@flow( name="Bangkok-Temperature-Monitor" )
def temperature_schedule_flow():
    temp = fetch_temperature()
    save_temperature(temp)

if __name__ == "__main__":
    
    temperature_schedule_flow()
    # Register the deployment to Prefect Cloud without running it locally
    deploy(
        name="bangkok-2-min-cron-deploy",
        work_pool_name="pool01" , # Must match a work pool created in Prefect Cloud
        schedule=CronSchedule(
            cron="*/2 * * * *",
            timezone="Asia/Bangkok"
        ) ,
        # image="my-custom-docker-image:latest" # Optional: Specify Docker image if using a Docker/K8s pool
    )
    
