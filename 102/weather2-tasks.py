import httpx
from prefect import flow, task

@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    temps = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    forecasted_temp = float(temps.json()["hourly"]["temperature_2m"][0])
    print(f"Forecasted temp C: {forecasted_temp} degrees")
    return forecasted_temp

@task
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"

@flow(retries=2 ,retry_delay_seconds=2, log_prints=True)
def pipeline(lat: float = 35, lon: float = 40):
    # raise Exception("This is a test exception")
    temp = fetch_weather(lat, lon)
    result = save_weather(temp)
    print(result)
    return result

if __name__ == "__main__":
    pipeline()