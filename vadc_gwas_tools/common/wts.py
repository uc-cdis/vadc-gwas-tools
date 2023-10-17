"""Small class for interacting with the workspace token service to get a refresh token.
This tool works only for internal URLs.
"""
import os
from typing import Dict

import requests

from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY
from vadc_gwas_tools.common.logger import Logger


class WorkspaceTokenServiceClient:
    def __init__(self):
        self.gen3_environment = os.environ.get(GEN3_ENVIRONMENT_KEY, "default")
        self.service_url = f"http://workspace-token-service.{self.gen3_environment}"
        self.logger = Logger.get_logger("WorkspaceTokenServiceClient")

    def get_refresh_token(self, _di=requests) -> Dict[str, str]:
        """Hits the WTS endpoint to get the refresh token."""
        req = _di.get(f"{self.service_url}/token/", params={"idp": "default"})
        req.raise_for_status()
        return req.json()
