import httpx
from prefect import flow, task

@task(retries=4, retry_delay_seconds=2 )  # or [1, 2, 5])
def fetch_random_code():
    random_code = httpx.get("https://httpstat.us/Random/200,500", verify=False)
    if random_code.status_code >= 400:
        raise Exception()
    print(random_code.text)

@flow
def fetch():
    fetch_random_code()

if __name__ == "__main__":
    fetch() 