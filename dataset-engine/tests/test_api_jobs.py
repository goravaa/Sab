import json

from api.services.jobs import progress_from_receipts


class ReceiptStore:
    def __init__(self):
        self.read = []

    def list_keys(self, prefix):
        assert prefix == "jobs/job-1/progress/"
        return [
            "jobs/job-1/progress/batch_10000.json",
            "jobs/job-1/progress/batch_9999.json",
        ]

    def read_bytes(self, key):
        self.read.append(key)
        end_index = 10_000 if key.endswith("10000.json") else 9_999
        return json.dumps({"end_index": end_index}).encode()


def test_progress_reads_only_the_numerically_latest_receipt():
    store = ReceiptStore()

    progress = progress_from_receipts(store, "job-1", total_items=10_100)

    assert progress.processed_items == 10_000
    assert progress.completed_batches == 2
    assert store.read == ["jobs/job-1/progress/batch_10000.json"]
