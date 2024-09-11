"""This modules tests `vadc_gwas_tools.common.cohort_middleware.CohortServiceClient` class."""
import dataclasses
import gzip
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import requests
from utils import cleanup_files

from vadc_gwas_tools.common.cohort_middleware import CohortDefinitionResponse
from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient as MOD
from vadc_gwas_tools.common.cohort_middleware import (
    ConceptDescriptionResponse,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY


class TestCohortServiceClient(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.mocks = SimpleNamespace(requests=mock.MagicMock(spec_set=requests))

    def test_init_noenv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        obj = MOD()
        self.assertEqual(obj.gen3_environment, "default")
        self.assertEqual(obj.service_url, "http://cohort-middleware-service.default")

    def test_init_wenv(self):
        os.environ[GEN3_ENVIRONMENT_KEY] = "something-else"
        obj = MOD()

        self.assertEqual(obj.gen3_environment, "something-else")
        self.assertEqual(
            obj.service_url, "http://cohort-middleware-service.something-else"
        )

    def test_get_header(self):
        expected = {"Content-Type": "application/json", "Authorization": "Bearer abc"}
        obj = MOD()
        obj.wts.get_refresh_token = mock.MagicMock(return_value={"token": "abc"})
        res = obj.get_header()
        self.assertEqual(res, expected)

    def _return_generator(self, items):
        for item in items:
            yield item

    def test_get_cohort_csv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        fake_items = [b"sample.id,ID_1001,ID_1002,ID_10_20\n", b"1001,0.01,1.5,1\n"]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.iter_content.return_value = self._return_generator(fake_items)
        self.mocks.requests.post.return_value = mock_proc

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        (fd1, fpath1) = tempfile.mkstemp()
        try:
            variables = [
                ConceptVariableObject(
                    variable_type="concept",
                    concept_id=1001,
                    prefixed_concept_id="ID_1001",
                ),
                ConceptVariableObject(
                    variable_type="concept",
                    concept_id=1002,
                    prefixed_concept_id="ID_1002",
                ),
                CustomDichotomousVariableObject(
                    variable_type="custom_dichotomous", cohort_ids=[10, 20]
                ),
            ]
            obj.get_cohort_csv(1, 2, fpath1, variables, _di=self.mocks.requests)
            exp_payload = {
                "variables": [
                    {
                        "variable_type": "concept",
                        "concept_id": 1001,
                        "concept_name": None,
                        "prefixed_concept_id": "ID_1001",
                    },
                    {
                        "variable_type": "concept",
                        "concept_id": 1002,
                        "concept_name": None,
                        "prefixed_concept_id": "ID_1002",
                    },
                    {
                        "variable_type": "custom_dichotomous",
                        "cohort_ids": [10, 20],
                        "provided_name": None,
                    },
                ]
            }
            self.mocks.requests.post.assert_called_with(
                "http://cohort-middleware-service.default/cohort-data/by-source-id/1/by-cohort-definition-id/2",
                data=json.dumps(exp_payload),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer abc",
                },
                stream=True,
                timeout=(6.05, 200),
            )

            with self.assertRaises(OSError) as _:
                with gzip.open(fpath1, "rt") as fh:
                    for line in fh:
                        pass

            with open(fpath1, "rt") as fh:
                header = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(
                    header, ["sample.id", "ID_1001", "ID_1002", "ID_10_20"]
                )

                dat1 = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(dat1, ["1001", "0.01", "1.5", "1"])

        finally:
            cleanup_files(fpath1)

    def test_get_cohort_csv_gzip(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        fake_items = [b"sample.id,ID_1001,ID_1002,ID_10_20\n", b"1001,0.01,1.5,1\n"]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.iter_content.return_value = self._return_generator(fake_items)
        self.mocks.requests.post.return_value = mock_proc

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        (fd1, fpath1) = tempfile.mkstemp(suffix=".csv.gz")
        try:
            variables = [
                ConceptVariableObject(
                    variable_type="concept",
                    concept_id=1001,
                    prefixed_concept_id="ID_1001",
                ),
                ConceptVariableObject(
                    variable_type="concept",
                    concept_id=1002,
                    prefixed_concept_id="ID_1002",
                ),
                CustomDichotomousVariableObject(
                    variable_type="custom_dichotomous", cohort_ids=[10, 20]
                ),
            ]
            obj.get_cohort_csv(1, 2, fpath1, variables, _di=self.mocks.requests)
            exp_payload = {
                "variables": [
                    {
                        "variable_type": "concept",
                        "concept_id": 1001,
                        "concept_name": None,
                        "prefixed_concept_id": "ID_1001",
                    },
                    {
                        "variable_type": "concept",
                        "concept_id": 1002,
                        "concept_name": None,
                        "prefixed_concept_id": "ID_1002",
                    },
                    {
                        "variable_type": "custom_dichotomous",
                        "cohort_ids": [10, 20],
                        "provided_name": None,
                    },
                ]
            }
            self.mocks.requests.post.assert_called_with(
                "http://cohort-middleware-service.default/cohort-data/by-source-id/1/by-cohort-definition-id/2",
                data=json.dumps(exp_payload),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer abc",
                },
                stream=True,
                timeout=(6.05, 200),
            )
            with gzip.open(fpath1, "rt") as fh:
                header = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(
                    header, ["sample.id", "ID_1001", "ID_1002", "ID_10_20"]
                )

                dat1 = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(dat1, ["1001", "0.01", "1.5", "1"])

        finally:
            cleanup_files(fpath1)

    def test_strip_concept_prefix(self):
        pfx_concept = 'ID_2000000001'
        expected = [2000000001]
        ret = MOD.strip_concept_prefix(pfx_concept)
        self.assertEqual(ret, expected)

        pfx_concept = ['ID_2000000001', 'ID_2000000002']
        expected = [2000000001, 2000000002]
        ret = MOD.strip_concept_prefix(pfx_concept)
        self.assertEqual(ret, expected)

        pfx_concept = "2000000002"
        expected = [2000000002]
        ret = MOD.strip_concept_prefix(pfx_concept)
        self.assertEqual(ret, expected)

    def test_get_cohort_definition(self):
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.json.return_value = {
            "cohort_definition": {
                "cohort_definition_id": 9,
                "cohort_name": "Test",
                "cohort_description": "Some test cohort",
                "Expression":"{\"CriteriaList\":\"\Observation\"}"
            }
        }
        self.mocks.requests.get.return_value = mock_proc
        expected = CohortDefinitionResponse(
            cohort_definition_id=9,
            cohort_name="Test",
            cohort_description="Some test cohort",
            Expression="{\"CriteriaList\":\"\Observation\"}"
        )

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        res = obj.get_cohort_definition(9, _di=self.mocks.requests)
        self.assertEqual(res, expected)

        self.mocks.requests.get.assert_called_with(
            "http://cohort-middleware-service.default/cohortdefinition/by-id/9",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            },
        )

    def test_get_concept_description(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.json.return_value = {
            "concepts": [
                {
                    "concept_id": 2000000001,
                    "prefixed_concept_id": "ID_2000000001",
                    "concept_name": "Fake 1",
                    "concept_code": "TEST",
                    "concept_type": "MVP Continuous",
                },
                {
                    "concept_id": 2000000002,
                    "prefixed_concept_id": "ID_2000000002",
                    "concept_name": "Fake 2",
                    "concept_code": "TEST 2",
                    "concept_type": "MVP Continuous",
                },
            ]
        }
        self.mocks.requests.post.return_value = mock_proc
        expected = [
            ConceptDescriptionResponse(
                concept_id=2000000001,
                prefixed_concept_id="ID_2000000001",
                concept_name="Fake 1",
                concept_code="TEST",
                concept_type="MVP Continuous",
            ),
            ConceptDescriptionResponse(
                concept_id=2000000002,
                prefixed_concept_id="ID_2000000002",
                concept_name="Fake 2",
                concept_code="TEST 2",
                concept_type="MVP Continuous",
            ),
        ]

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        res = obj.get_concept_descriptions(
            2, [2000000001, 2000000002], _di=self.mocks.requests
        )
        self.assertEqual(res, expected)

        self.mocks.requests.post.assert_called_with(
            "http://cohort-middleware-service.default/concept/by-source-id/2",
            data=json.dumps({"ConceptIds": [2000000001, 2000000002]}),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            },
        )

    def test_get_concept_description_nulls(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.json.return_value = {
            "concepts": [
                {
                    "concept_id": 2000000001,
                    "prefixed_concept_id": "ID_2000000001",
                    "concept_name": "Fake 1",
                },
                {
                    "concept_id": 2000000002,
                    "concept_name": "Fake 2",
                },
            ]
        }
        self.mocks.requests.post.return_value = mock_proc
        expected = [
            ConceptDescriptionResponse(
                concept_id=2000000001,
                prefixed_concept_id="ID_2000000001",
                concept_name="Fake 1",
                concept_code=None,
                concept_type=None,
            ),
            ConceptDescriptionResponse(
                concept_id=2000000002,
                prefixed_concept_id=None,
                concept_name="Fake 2",
                concept_code=None,
                concept_type=None,
            ),
        ]

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        res = obj.get_concept_descriptions(
            2, [2000000001, 2000000002], _di=self.mocks.requests
        )
        self.assertEqual(res, expected)

        self.mocks.requests.post.assert_called_with(
            "http://cohort-middleware-service.default/concept/by-source-id/2",
            data=json.dumps({"ConceptIds": [2000000001, 2000000002]}),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            },
        )

    def test_get_attrition_breakdown_csv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        fake_items = [
            b"Cohort,Size,AFR,ASN,EUR,HIS,NA\n",
            b"cases,55,5,16,12,11,11\n",
            b"Age group [MVP Demographics],18,1,3,4,5,5\n",
        ]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.iter_content.return_value = self._return_generator(fake_items)
        self.mocks.requests.post.return_value = mock_proc

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        (fd1, fpath1) = tempfile.mkstemp()
        try:
            variables = [
                ConceptVariableObject(
                    variable_type="concept",
                    concept_id=1001,
                    prefixed_concept_id="ID_1001",
                ),
                ConceptVariableObject(
                    variable_type="concept",
                    concept_id=1002,
                    prefixed_concept_id="ID_1002",
                ),
                CustomDichotomousVariableObject(
                    variable_type="custom_dichotomous", cohort_ids=[10, 20]
                ),
            ]
            obj.get_attrition_breakdown_csv(
                1, 2, fpath1, variables, "ID_6000", _di=self.mocks.requests
            )
            self.mocks.requests.post.assert_called_with(
                "http://cohort-middleware-service.default/concept-stats/by-source-id/1/by-cohort-definition-id/2/breakdown-by-concept-id/6000/csv",
                data=json.dumps(
                    {"variables": [dataclasses.asdict(i) for i in variables]}
                ),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer abc",
                },
                stream=True,
                timeout=(6.05, 540),
            )

            with self.assertRaises(OSError) as _:
                with gzip.open(fpath1, "rt") as fh:
                    for line in fh:
                        pass

            with open(fpath1, "rt") as fh:
                for n, item in enumerate(fake_items):
                    line = fh.readline().rstrip("\r\n").split(",")
                    self.assertEqual(
                        line, fake_items[n].decode('utf8').strip("\r\n").split(",")
                    )

        finally:
            cleanup_files(fpath1)


class TestCohortServiceClientVariableObjects(unittest.TestCase):
    def test_decode_concept_variable_json_concept(self):
        # Dict like concept_id outcome would be
        obj = {"variable_type": "concept", "concept_id": 20000001}
        obj_json_str = json.dumps(obj)
        expected = ConceptVariableObject(
            variable_type=obj['variable_type'],
            concept_id=obj['concept_id'],
            prefixed_concept_id=None,
        )

        result = json.loads(obj_json_str, object_hook=MOD.decode_concept_variable_json)
        self.assertEqual(expected, result)

        # List of Dicts like covariates
        obj = [
            {"variable_type": "concept", "concept_id": 20000001},
            {
                "variable_type": "concept",
                "concept_id": 20000001,
                "prefixed_concept_id": "ID_20000001",
            },
        ]
        obj_json_str = json.dumps(obj)
        expected = [
            ConceptVariableObject(
                variable_type=i['variable_type'],
                concept_id=i['concept_id'],
                prefixed_concept_id=i.get('prefixed_concept_id'),
            )
            for i in obj
        ]

        result = json.loads(obj_json_str, object_hook=MOD.decode_concept_variable_json)
        self.assertEqual(expected, result)

    def test_decode_concept_variable_json_custom_dichotomous(self):
        # Dict like concept_id outcome would be
        obj = {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]}
        obj_json_str = json.dumps(obj)
        expected = CustomDichotomousVariableObject(
            variable_type=obj['variable_type'], cohort_ids=obj['cohort_ids']
        )

        result = json.loads(obj_json_str, object_hook=MOD.decode_concept_variable_json)
        self.assertEqual(expected, result)

        # List of Dicts like covariates
        obj = [
            {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]},
            {
                "variable_type": "custom_dichotomous",
                "provided_name": "TEST",
                "cohort_ids": [30, 40],
            },
        ]
        obj_json_str = json.dumps(obj)
        expected = [CustomDichotomousVariableObject(**i) for i in obj]

        result = json.loads(obj_json_str, object_hook=MOD.decode_concept_variable_json)
        self.assertEqual(expected, result)

    def test_decode_concept_variable_json_mixed_types(self):
        # List of Dicts like covariates
        obj = [
            {
                "variable_type": "custom_dichotomous",
                "provided_name": "My custom variable 1",
                "cohort_ids": [10, 20],
            },
            {
                "variable_type": "custom_dichotomous",
                "provided_name": "My custom variable 2",
                "cohort_ids": [30, 40],
            },
            {
                "variable_type": "concept",
                "concept_id": 20000001,
                "prefixed_concept_id": "ID_20000001",
            },
        ]
        obj_json_str = json.dumps(obj)
        expected = [
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous",
                provided_name="My custom variable 1",
                cohort_ids=[10, 20],
            ),
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous",
                provided_name="My custom variable 2",
                cohort_ids=[30, 40],
            ),
            ConceptVariableObject(
                variable_type="concept",
                concept_id=20000001,
                prefixed_concept_id="ID_20000001",
            ),
        ]

        result = json.loads(obj_json_str, object_hook=MOD.decode_concept_variable_json)
        self.assertEqual(expected, result)

    def test_decode_concept_variable_json_error(self):
        # Dict like concept_id outcome would be
        obj = {"variable_type": "other", "concept_id": 20000001}
        obj_json_str = json.dumps(obj)

        with self.assertRaises(RuntimeError) as e:
            result = json.loads(
                obj_json_str, object_hook=MOD.decode_concept_variable_json
            )

        # List of Dicts like covariates
        obj = [
            {"variable_type": "concept", "concept_id": 20000001},
            {
                "variable_type": "other",
                "concept_id": 20000001,
                "prefixed_concept_id": "ID_20000001",
            },
        ]
        obj_json_str = json.dumps(obj)
        with self.assertRaises(RuntimeError) as e:
            result = json.loads(
                obj_json_str, object_hook=MOD.decode_concept_variable_json
            )
