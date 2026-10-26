from prefect import flow
from prefect.deployments import run_deployment

@flow
def run_deployment_from_flow():
    print("Running deployment from a flow")
    run_deployment(
        name="pipeline/my-first-managed-deployment", parameters={"lat": 1, "lon": 2}
    )
    return

if __name__ == "__main__":
    run_deployment_from_flow() 