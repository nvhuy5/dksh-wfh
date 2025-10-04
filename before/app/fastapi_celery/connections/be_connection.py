import httpx

# ===
import logging
from utils import log_helpers

# ===
# Set up logging
logger_name = "Backend API Connection"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===


class BEConnector:
    def __init__(self, api_url, body_data=None):
        self.api_url = api_url
        self.body_data = body_data or {}
        self.metadata = {}

    async def post(self):
        return await self._request("POST")

    async def get(self):
        return await self._request("GET")

    async def put(self):
        return await self._request("PUT")

    async def _request(self, method):
        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(method, self.api_url, json=self.body_data)
                response.raise_for_status()
                return response.json().get("data", {})
            except httpx.HTTPStatusError as e:
                logger.error(f"{method} error {self.api_url}: {e.response.status_code} - {e.response.text}")
            except Exception as e:
                logger.exception(f"Unexpected error during {method} request: {str(e)}")
        return None

    def get_field(self, key):
        """
        Get a specific field from the metadata.
        """
        return self.metadata.get(key)

    def __repr__(self):
        return f"<POTemplateMetadata keys={list(self.metadata.keys())}>"
