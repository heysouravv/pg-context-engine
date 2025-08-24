import asyncio, os
from temporalio.client import Client
from temporalio.worker import Worker
from workflows import MirrorIngestWorkflow, UserContextWorkflow
from activities import (
    validate_mirror, commit_global_mirror, fanout_global_update,
    store_user_ctx, project_view
)

async def main():
    client = await Client.connect(os.getenv("TEMPORAL_TARGET","temporal:7233"))
    worker = Worker(
        client,
        task_queue=os.getenv("TASK_QUEUE","edge-tq"),
        workflows=[MirrorIngestWorkflow, UserContextWorkflow],
        activities=[
            validate_mirror, commit_global_mirror, fanout_global_update,
            store_user_ctx, project_view
        ],
    )
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
