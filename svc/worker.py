import asyncio, os
from temporalio.client import Client
from temporalio.worker import Worker
from workflows import ContinentIngestWorkflow, UserContextWorkflow
from activities import (
    validate_continent, cache_continent_data, compute_continent_diff, 
    commit_continent_data, fanout_continent_update,
    store_user_ctx, project_view
)

async def main():
    client = await Client.connect(os.getenv("TEMPORAL_TARGET","temporal:7233"))
    worker = Worker(
        client,
        task_queue=os.getenv("TASK_QUEUE","edge-tq"),
        workflows=[ContinentIngestWorkflow, UserContextWorkflow],
        activities=[
            validate_continent, cache_continent_data, compute_continent_diff,
            commit_continent_data, fanout_continent_update,
            store_user_ctx, project_view
        ],
    )
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
