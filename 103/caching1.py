from prefect import task
from prefect.cache_policies import INPUTS

@task(cache_policy=INPUTS, log_prints=True)
def my_cached_task(x: int):
    print(f"Result is: {x + 42}")

my_cached_task(8)  # Task runs
my_cached_task(8)  # Task doesn't run, uses the cached result
my_cached_task(33)  # Task runs