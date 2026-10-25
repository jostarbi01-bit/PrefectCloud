import time
from prefect import flow, serve

@flow
def slow_flow(sleep: int = 5):
    "Sleepy flow - sleeps the provided amount of time (in seconds)."
    time.sleep(sleep)
@flow
def fast_flow():
    "Fastest flow this side of the Atlantic."
    return
if __name__ == "__main__":
    slow_deploy2 = slow_flow.to_deployment(name="sleeper-scheduling2", interval=8)
    fast_deploy2 = fast_flow.to_deployment(name="fast-scheduling2", cron="* * * * *")
    serve(slow_deploy2, fast_deploy2)