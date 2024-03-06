"""Small class for interacting with the cohort middleware server.
This class works only for internal URLs.
"""
import gzip
import json
import os
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Union

import requests

from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.common.wts import WorkspaceTokenServiceClient


@dataclass
class CohortDefinitionResponse:
    cohort_definition_id: int
    cohort_name: str
    cohort_description: Optional[str] = None


@dataclass
class ConceptDescriptionResponse:
    concept_id: int
    concept_name: str
    prefixed_concept_id: Optional[str] = None
    concept_code: Optional[str] = None
    concept_type: Optional[str] = None


@dataclass
class ConceptVariableObject:
    variable_type: str
    concept_id: int
    concept_name: Optional[str] = None
    prefixed_concept_id: Optional[str] = None


@dataclass
class CustomDichotomousVariableObject:
    variable_type: str
    cohort_ids: List[int]
    provided_name: Optional[str] = None


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
        variable_objects: List[
            Union[ConceptVariableObject, CustomDichotomousVariableObject]
        ],
        _di=requests,
    ) -> None:
        """
        Hits the cohort middleware /cohort-data endpoint to get the CSV.
        Takes the list of variable object definitions. If the local_path ends with '.gz'
        the file will be gzipped.
        """
        self.logger.info(f"Source - {source_id}; Cohort - {cohort_definition_id}")
        payload = {"variables": [asdict(i) for i in variable_objects]}
        req = _di.post(
            f"{self.service_url}/cohort-data/by-source-id/{source_id}/by-cohort-definition-id/{cohort_definition_id}",  # pylint: disable=C0301
            data=json.dumps(payload),
            headers=self.get_header(),
            stream=True,
            timeout=(6.05, 1800),
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
        self, source_id: int, concept_ids: List[int], _di=requests
    ) -> List[ConceptDescriptionResponse]:
        """
        Makes cohort middleware request to get descriptions of concept IDs
        and formats into a list of ConceptDescriptionResponse objects.
        """
        self.logger.info(f"Concept IDs: {concept_ids}")
        payload = {"ConceptIds": concept_ids}
        req = _di.post(
            f"{self.service_url}/concept/by-source-id/{source_id}",
            data=json.dumps(payload),
            headers=self.get_header(),
        )
        req.raise_for_status()
        response = req.json()
        fmt_response = [ConceptDescriptionResponse(**i) for i in response["concepts"]]
        return fmt_response

    def get_attrition_breakdown_csv(
        self,
        source_id: int,
        cohort_definition_id: int,
        local_path: str,
        variable_objects: List[
            Union[ConceptVariableObject, CustomDichotomousVariableObject]
        ],
        prefixed_breakdown_concept_id: str,
        _di=requests,
    ) -> None:
        """
        Hits the cohort middleware endpoint that generates an attrition table that is broken down by
        a particular concept ID. This is most relevant for breaking down by HARE concept ID. This will
        generate a CSV file.
        """
        self.logger.info(f"Source - {source_id}; Cohort - {cohort_definition_id}")
        self.logger.info(f"Variables - {variable_objects}")
        self.logger.info(
            f"Prefixed Breakdown Concept ID - {prefixed_breakdown_concept_id}"
        )
        payload = {"variables": [asdict(i) for i in variable_objects]}
        breakdown_concept_id = CohortServiceClient.strip_concept_prefix(
            prefixed_breakdown_concept_id
        )[0]
        req = _di.post(
            f"{self.service_url}/concept-stats/by-source-id/{source_id}/by-cohort-definition-id/{cohort_definition_id}/breakdown-by-concept-id/{breakdown_concept_id}/csv",
            data=json.dumps(payload),
            headers=self.get_header(),
            stream=True,
            timeout=(6.05, 1800),
        )
        req.raise_for_status()
        self.logger.info(f"Writing output to {local_path}...")
        open_func = gzip.open if local_path.endswith('.gz') else open
        with open_func(local_path, "wb") as o:  # pylint: disable=C0103
            for chunk in req.iter_content(chunk_size=128):
                o.write(chunk)

    @staticmethod
    def strip_concept_prefix(prefixed_concept_ids: Union[List[str], str]) -> List[int]:
        """
        Removes the prefix from the Concept ID and convert to integer.
        """
        if isinstance(prefixed_concept_ids, str):
            prefixed_concept_ids = [prefixed_concept_ids]
        return list(map(lambda x: int(x.lstrip('ID_')), prefixed_concept_ids))

    @staticmethod
    def decode_concept_variable_json(
        obj: Union[
            List[Dict[str, Union[str, int, List[int]]]],
            Dict[str, Union[str, int, List[int]]],
        ]
    ) -> List[Union[ConceptVariableObject, CustomDichotomousVariableObject]]:
        """
        JSON decoder for covariates/outcomes in new JSON format.
        """
        result = None
        if isinstance(obj, list):
            result = []
            for item in obj:
                if item['variable_type'] == "concept":
                    val = ConceptVariableObject(**item)
                    result.append(val)
                elif item['variable_type'] == "custom_dichotomous":
                    val = CustomDichotomousVariableObject(**item)
                    result.append(val)
                else:
                    msg = (
                        "Currently we only support 'concept' and 'custom_dichotomous' variable "
                        "types, but you provided {}".format(item.get('variable_type'))
                    )
                    self.logger.error(msg)
                    raise RuntimeError(msg)

        elif isinstance(obj, dict):
            result = {}
            if obj['variable_type'] == "concept":
                result = ConceptVariableObject(**obj)
            elif obj['variable_type'] == "custom_dichotomous":
                result = CustomDichotomousVariableObject(**obj)
            else:
                msg = (
                    "Currently we only support 'concept' and 'custom_dichotomous' variable "
                    "types, but you provided {}".format(obj.get('variable_type'))
                )
                raise RuntimeError(msg)
        return result
