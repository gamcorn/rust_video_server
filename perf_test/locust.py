from locust import HttpUser, task
import uuid
class ExtremeUser(HttpUser):
    @task
    def test_load(self):
        self.client.post(
            url="http://localhost:3030",
            headers={"Content-Type": "application/json"},
            json={
            "jsonrpc": "2.0",
            "method": "getVideoDimensions",
            "params": {
                    "file_name": "test_1.avi",
                    "date": "2024-02-01T00:00:00Z",
                    "identifier": str(uuid.uuid4()),
                    "url": "http://localhost:8080/videos/test_1.avi"
                    },
                    "id": str(uuid.uuid4())
                }
            )
