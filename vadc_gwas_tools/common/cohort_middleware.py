"""Small class for interacting with the workspace token service to get a refresh token.
This tool works only for internal URLs.
"""
import json
import os
from typing import Dict, List, Union

import requests

from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.common.wts import WorkspaceTokenServiceClient


class CohortServiceClient:
    def __init__(self):
        self.gen3_environment = os.environ.get(GEN3_ENVIRONMENT_KEY, "default")
        self.service_url = f"http://cohort-middleware-service.{self.gen3_environment}"
        self.logger = Logger.get_logger("CohortServiceClient")
        self.wts = WorkspaceTokenServiceClient()

    def get_header(self) -> Dict[str, str]:
        """Generates the request header."""
        tkn = self.wts.get_refresh_token()["token"]
        hdr = {"Content-Type": "application/json", "Authorization": f"Bearer {tkn}"}
        return hdr

    def get_cohort_csv(
        self,
        source_id: int,
        cohort_definition_id: int,
        local_path: str,
        prefixed_concept_ids: List[str],
        _di=requests,
    ) -> None:
        """
        Hits the cohort middleware /cohort-data endpoint to get the CSV.
        Takes the prefixed concept ids (ID_...).
        """
        self.logger.info(f"Source - {source_id}; Cohort - {cohort_definition_id}")
        self.logger.info(f"Prefixed Concept IDs - {prefixed_concept_ids}")
        payload = {"PrefixedConceptIds": prefixed_concept_ids}
        req = _di.post(
            f"{self.service_url}/cohort-data/by-source-id/{source_id}/by-cohort-definition-id/{cohort_definition_id}",
            data=json.dumps(payload),
            headers=self.get_header(),
            stream=True,
        )
        req.raise_for_status()
        self.logger.info(f"Writing output to {local_path}...")
        with open(local_path, "wb") as o:
            for chunk in req.iter_content(chunk_size=128):
                o.write(chunk)

    def strip_concept_prefix(
        self, prefixed_concept_ids: Union[List[str], str]
    ) -> List[int]:
        """
        Removes the prefix from the Concept ID and convert to integer.
        """
        pass
