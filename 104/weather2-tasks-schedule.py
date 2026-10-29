import httpx
from prefect import  flow, task
from datetime import timedelta
from prefect.client.schemas.schedules    import IntervalSchedule

@task(retries=2 ,retry_delay_seconds=5, log_prints=True)
def task_fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    temps = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    forecasted_temp = float(temps.json()["hourly"]["temperature_2m"][0])
    print(f"Forecasted temp C: {forecasted_temp} degrees")
    return forecasted_temp

# @task(retries=2 ,retry_delay_seconds=5, log_prints=True)
# def save_weather(temp: float):
#     with open("weather.csv", "w+") as w:
#         w.write(str(temp))
#     return "Successfully wrote temp"


@flow( log_prints=True )
def pipeline_fetch_weather(lat: float = 13.7563, lon: float = 100.5018):
    temp = task_fetch_weather(lat, lon)
    # result = save_weather(temp)
    # print(result)
    return temp

if __name__ == "__main__":          

    Bangkok_schedule=IntervalSchedule(
        interval=timedelta(minutes=2),
        timezone="Asia/Bangkok"
    )
## .serve cannot use work_pool_name="pool01"
    pipeline_fetch_weather.serve(
        name="deploy-fetch-weather-scheduled",
        # work_pool_name="pool01",    
        schedule=Bangkok_schedule,
        # parameters={"lat": 35, "lon": 40}    
        # cron="*/1 * * * *",      
        # cron="0 5 * * * *",   (ทุกวัน 5.00 น.)   
        # timezone="Asia/Bangkok"
    )

#  fetch_weather.serve(name="deploy-params", parameters={"lat": 11, "lon": 12})
#  fetch_weather.serve(name="deploy-scheduled", cron="* * * * *")     

## Git deploy
# flow.from_source(
#         source="https://github.com/jostarbi01-bit/PrefectCloud.git",
#         entrypoint="102/weather2-tasks.py:pipeline",
#     ).deploy(
#         name="my-first-managed-deployment",        
#         work_pool_name="pool01",
#     )
