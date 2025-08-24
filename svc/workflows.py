from datetime import timedelta
from temporalio import workflow, activity

@workflow.defn
class MirrorIngestWorkflow:
    @workflow.run
    async def run(self, dataset_id: str, version: str, checksum: str, rows: list[dict]):
        await workflow.execute_activity(
            "validate_mirror",
            args=[dataset_id, version, checksum, len(rows)],
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            "commit_global_mirror",
            args=[dataset_id, version, checksum, rows],
            schedule_to_close_timeout=timedelta(seconds=120),
        )
        await workflow.execute_activity(
            "fanout_global_update",
            args=[dataset_id, version],
            schedule_to_close_timeout=timedelta(seconds=15),
        )

@workflow.defn
class UserContextWorkflow:
    @workflow.run
    async def run(self, user_id: str, dataset_id: str, ctx: dict):
        await workflow.execute_activity(
            "store_user_ctx",
            args=[user_id, dataset_id, ctx],
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.execute_activity(
            "project_view",
            args=[user_id, dataset_id],
            schedule_to_close_timeout=timedelta(seconds=60),
        )
