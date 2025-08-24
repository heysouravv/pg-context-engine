from datetime import timedelta
from temporalio import workflow, activity

@workflow.defn
class ContinentIngestWorkflow:
    @workflow.run
    async def run(self, dataset_id: str, version: str, checksum: str, rows: list[dict]):
        # Step 1: Validate the data
        await workflow.execute_activity(
            "validate_continent",
            args=[dataset_id, version, checksum, len(rows)],
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        
        # Step 2: Immediately cache the data for fast GET responses
        await workflow.execute_activity(
            "cache_continent_data",
            args=[dataset_id, version, checksum, rows],
            schedule_to_close_timeout=timedelta(seconds=60),
        )
        
        # Step 3: Compute diffs asynchronously
        diff_result = await workflow.execute_activity(
            "compute_continent_diff",
            args=[dataset_id, version, checksum, rows],
            schedule_to_close_timeout=timedelta(seconds=120),
        )
        
        # Step 4: Commit to database asynchronously (after caching)
        await workflow.execute_activity(
            "commit_continent_data",
            args=[dataset_id, version, checksum, rows, diff_result.get("parent_version"), diff_result.get("diff_checksum")],
            schedule_to_close_timeout=timedelta(seconds=180),
        )
        
        # Step 5: Notify subscribers
        await workflow.execute_activity(
            "fanout_continent_update",
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
