"""Small class for interacting with the workspace token service to get a refresh token.
This tool works only for internal URLs.
"""
import gzip
import json
import os
from typing import Dict, List, NamedTuple, Union

import requests

from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.common.wts import WorkspaceTokenServiceClient


class CohortDefinitionResponse(NamedTuple):
    cohort_definition_id: int
    cohort_name: str
    cohort_description: str


class ConceptDescriptionResponse(NamedTuple):
    concept_id: int
    prefixed_concept_id: str
    concept_name: str
    domain_id: str
    domain_name: str


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
        Takes the prefixed concept ids (ID_...). If the local_path ends with '.gz'
        the file will be gzipped.
        """
        self.logger.info(f"Source - {source_id}; Cohort - {cohort_definition_id}")
        self.logger.info(f"Prefixed Concept IDs - {prefixed_concept_ids}")
        payload = {"PrefixedConceptIds": prefixed_concept_ids}
        req = _di.post(
            f"{self.service_url}/cohort-data/by-source-id/{source_id}/by-cohort-definition-id/{cohort_definition_id}",  # pylint: disable=C0301
            data=json.dumps(payload),
            headers=self.get_header(),
            stream=True,
        )
        req.raise_for_status()
        self.logger.info(f"Writing output to {local_path}...")
        open_func = gzip.open if local_path.endswith('.gz') else open
        with open_func(local_path, "wb") as o:  # pylint: disable=C0103
            for chunk in req.iter_content(chunk_size=128):
                o.write(chunk)

    def get_cohort_definition(
        self, cohort_definition_id: int, _di=requests
    ) -> CohortDefinitionResponse:
        """
        Makes cohort middleware request to get the cohort definition metadata
        and format into CohortDefinitionResponse object.
        """
        self.logger.info(f"Cohort - {cohort_definition_id}")
        req = _di.get(
            f"{self.service_url}/cohortdefinition/by-id/{cohort_definition_id}",
            headers=self.get_header(),
        )
        req.raise_for_status()
        response = req.json()
        return CohortDefinitionResponse(
            cohort_definition_id=response["cohort_definition"]["cohort_definition_id"],
            cohort_name=response["cohort_definition"]["cohort_name"],
            cohort_description=response["cohort_definition"]["cohort_description"],
        )

    def get_concept_descriptions(
        self, source_id: int, prefixed_concept_ids: List[str], _di=requests
    ) -> List[ConceptDescriptionResponse]:
        """
        Makes cohort middleware request to get descriptions of concept IDs
        and formats into a list of ConceptDescriptionResponse objects.
        """
        self.logger.info(f"Prefixed Concept IDs: {prefixed_concept_ids}")
        payload = {
            "ConceptIds": CohortServiceClient.strip_concept_prefix(prefixed_concept_ids)
        }
        req = _di.post(
            f"{self.service_url}/concept/by-source-id/{source_id}",
            data=json.dumps(payload),
            headers=self.get_header(),
        )
        req.raise_for_status()
        response = req.json()
        fmt_response = list(
            [
                ConceptDescriptionResponse(
                    concept_id=i["concept_id"],
                    prefixed_concept_id=i["prefixed_concept_id"],
                    concept_name=i["concept_name"],
                    domain_id=i["domain_id"],
                    domain_name=i["domain_name"],
                )
                for i in response["concepts"]
            ]
        )
        return fmt_response

    @staticmethod
    def strip_concept_prefix(prefixed_concept_ids: Union[List[str], str]) -> List[int]:
        """
        Removes the prefix from the Concept ID and convert to integer.
        """
        if isinstance(prefixed_concept_ids, str):
            prefixed_concept_ids = [prefixed_concept_ids]
        return list(map(lambda x: int(x.lstrip('ID_')), prefixed_concept_ids))
