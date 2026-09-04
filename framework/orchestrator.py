from .audit import AuditLogger
from .engine import run_task
from .metadata import MetadataRepository


class Orchestrator:
    def __init__(self, spark, catalog: str):
        self.metadata = MetadataRepository(spark, catalog)
        self.audit = AuditLogger(spark, catalog)

    def run(self, group_id: str, layer: str, task_id: str | None = None):
        group_id = group_id.strip().upper()
        layer = layer.strip().upper()
        header = self.metadata.validate_group(group_id, layer)
        tasks = self.metadata.tasks(group_id, layer, task_id)
        results = []
        for task in sorted(tasks, key=lambda item: item.row.get("PRIORITY") or 999):
            results.append(run_task(self.metadata.spark, task, header, self.audit))
        return results