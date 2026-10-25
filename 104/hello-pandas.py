from prefect import flow
import pandas as pd

@flow(log_prints=True)
def my_table_flow():
    """demo flow that prints a pandas DataFrame to the logs"""

    df = pd.DataFrame({"calories": [420, 380, 390], "duration": [50, 40, 45]})
    print(df)