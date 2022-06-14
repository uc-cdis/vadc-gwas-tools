"""Interacts with indexd service to generate the indexd record for the file.

@author: Viktorija Zaksas <vzpgb@uchicago.edu>
"""

import hashlib
import json
import os
from argparse import ArgumentParser, Namespace

from vadc_gwas_tools.common.indexd import IndexdServiceClient
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class CreateIndexdRecord(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add the subcommand params"""
        parser.add_argument(
            "--gwas_archive",
            required=True,
            type=str,
            help=("Path to gwas archive. Required parameter."),
        )
        parser.add_argument(
            "--s3_uri",
            required=True,
            type=str,
            help=(
                "S3 URI for the gwas tar archive on the downloadable bucket. "
                "Required parameter."
            ),
        )
        parser.add_argument(
            "--arborist_resource",
            required=True,
            type=str,
            nargs="+",
            help=("One or more arborist authorization resource. Required parameter."),
        )
        parser.add_argument(
            "-o",
            "--output",
            required=True,
            type=str,
            help=(
                "Path to write out the JSON response, containing generated "
                "record and Globally Unique Identifier (GUID)."
            ),
        )

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Takes GWAS archive, calculates hash and size, and genereates "
            "Indexd record with hash, size, provided arborist authorization, "
            "and provided S3 destination. Returns JSON response containing "
            "assigned Globally Unique Indentifier (GUID) in the 'did' field. "
            "Set the INDEXD_USER, INDEXD_PASSWORD variables for accessing "
            "Indexd endpoint"
        )

    @classmethod
    def _get_md5_sum(cls, fil):
        """
        Helper to calculate hash for the provided file
        """
        md5 = hashlib.md5()
        with open(fil, 'rb') as fh:
            while True:
                r = fh.read(8192)
                if not r:
                    break
                md5.update(r)
        return {"md5": str(md5.hexdigest())}

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for CreateIndexdRecord
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())
        client = IndexdServiceClient()

        gwas_name = os.path.basename(options.gwas_archive)
        logger.info(f"Calculating hashes for {gwas_name}...")
        hash_meta = cls._get_md5_sum(options.gwas_archive)
        logger.info(f"Hash calculated: {hash_meta}")
        logger.info(f"Calculating file size for {gwas_name}...")
        file_size = os.path.getsize(options.gwas_archive)
        logger.info(f"Size calculated: {file_size}")
        logger.info(f"Preparing Indexd record for {gwas_name}...")
        metadata = {
            "file_name": gwas_name,
            "authz": options.arborist_resource,
            "hashes": hash_meta,
            "size": file_size,
            "urls": [options.s3_uri],
            "urls_metadata": {options.s3_uri: {}},
            "form": "object",
        }
        logger.info(f"Indexd record data: \n{metadata}")
        logger.info(f"Creating Indexd record...:")
        record_json = client.create_indexd_record(metadata=metadata)
        logger.info(f"Indexd record created.")

        with open(options.output, 'w', encoding='utf-8') as o:
            json.dump(record_json, o, ensure_ascii=False, indent=4)
        logger.info(f"JSON response saved in {options.output}")
