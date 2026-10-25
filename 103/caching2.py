from datetime import timedelta
from time import sleep
from prefect import flow, task
from prefect.cache_policies import INPUTS
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.futures import wait

@task(cache_policy=INPUTS, cache_expiration=timedelta(seconds=10))
def hello_task(name_input: str):
    print(f"***** What's up? {name_input}")
    return name_input

@flow(log_prints=True)
def hello_flow(name_input: str):
    hello_task(name_input)

if __name__ == "__main__":
    hello_flow(
        "world2"
    )  # This will not use the cache (assuming you didn't run it within the past 10 seconds   )
    sleep(15)
    hello_flow("world2")  # This will use the cache
    hello_flow("world2")  # This will not use the cache
     