"""Small class for interacting with the indexd service to create the indexd
record.  This tool works only for internal URLs.
"""
import os
from typing import Dict, List

import requests

from vadc_gwas_tools.common.const import (
    GEN3_ENVIRONMENT_KEY,
    INDEXD_PASSWORD,
    INDEXD_USER,
)
from vadc_gwas_tools.common.logger import Logger


class IndexdServiceClient:
    def __init__(self):
        self.gen3_environment = os.environ.get(GEN3_ENVIRONMENT_KEY, "default")
        self.service_url = f"http://indexd-service.{self.gen3_environment}"
        self.logger = Logger.get_logger("IndexdServiceClient")

    def get_auth(self) -> List[str]:
        "Get indexd authentication"
        indexd_user = os.environ.get(INDEXD_USER, "")
        indexd_password = os.environ.get(INDEXD_PASSWORD, "")
        auth = (indexd_user, indexd_password)
        return auth

    def create_indexd_record(self, metadata="", _di=requests):
        """
        Creates indexd record from the metadata provided.
        Metadata should be a valid JSON. More information on indexd metadata
        can be found:
            https://github.com/uc-cdis/indexd#indexd-records
        Returns JSON with assigned Globally Unique Identifier (GUID) in the
        'did' field.
        """

        response = _di.post(
            f"{self.service_url}/index", json=metadata, auth=self.get_auth()
        )
        response.raise_for_status()
        guid = response.json()["did"]
        self.logger.info(f"Assigned GUID (did) for Indexd record: {guid}")
        return response.json()
