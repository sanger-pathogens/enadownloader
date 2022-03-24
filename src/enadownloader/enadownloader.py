#!/usr/bin/env python3
"""
Robust tool to download fastq.gz files and metadata from ENA
"""

import asyncio
import hashlib
import logging
import shutil
import urllib.request as urlrequest
from os.path import basename, exists, join
from pathlib import Path
from time import sleep
from typing import Iterable
from urllib.error import URLError

from enadownloader.enametadata import ENAMetadata
from enadownloader.utils import ENAObject


class ENADownloader:
    class InvalidRow(ValueError):
        pass

    def __init__(
        self,
        accessions: Iterable,
        accession_type: str,
        output_dir: Path,
        create_study_folders: bool,
        project_id: str,
        metadata_obj: ENAMetadata,
        retries: int = 5,
    ):
        self.accessions = accessions
        self.accession_type = accession_type
        self.output_dir = output_dir
        self.create_study_folders = create_study_folders
        self.retries = retries
        self.metadata_obj = metadata_obj
        self.project_id = project_id

        self.response_file = join(output_dir, f".{project_id}.csv")
        self.progress_file = join(output_dir, f".{project_id}.progress.csv")

    def validate_accession(self, accession, accession_type):
        if accession_type == "run":
            if not accession.startswith(("SRR", "ERR", "DRR")):
                raise ValueError(f"Invalid run accession: {accession}")
        elif accession_type == "sample":
            if not accession.startswith(("ERS", "DRS", "SRS", "SAM")):
                raise ValueError(f"Invalid sample accession: {accession}")
        elif accession_type == "study":
            if not accession.startswith(("SRP", "ERP", "DRP", "PRJ")):
                raise ValueError(f"Invalid study accession: {accession}")
        else:
            raise ValueError(f"Invalid accession_type: {accession_type}")

    def parse_accessions(self, accessions, accession_type="run"):
        parsed_accessions = []
        for accession in accessions:
            try:
                self.validate_accession(accession, accession_type)
            except ValueError as err:
                logging.warning(f"{err}. Skipping...")
                continue
            else:
                parsed_accessions.append(accession)
        return parsed_accessions

    def parse_ftp_metadata(self, metadata):
        parsed_metadata = []
        for row in metadata:
            try:
                new_rows = self.flatten_multivalued_ftp_attrs(row)
            except self.InvalidRow as err:
                logging.warning(
                    f"Found invalid metadata for run accession {row['run_accession']}. Reason: {err}. Skipping."
                )
                continue
            parsed_metadata.extend(new_rows)
        return parsed_metadata

    def flatten_multivalued_ftp_attrs(self, row):
        if "fastq_ftp" in row and not row["fastq_ftp"].strip():
            raise self.InvalidRow("No FTP URL was found")
        ftp_links = row["fastq_ftp"].split(";")
        md5s = row["fastq_md5"].split(";")
        if len(md5s) != len(ftp_links):
            raise self.InvalidRow(
                "The number of FTP URLs does not match the number of MD5 checksums"
            )
        rows = []
        for f, m in zip(ftp_links, md5s):
            new_row = row.copy()
            new_row["fastq_ftp"] = f
            new_row["fastq_md5"] = m
            rows.append(new_row)
        return rows

    def get_ftp_paths(self):
        if exists(self.response_file):
            response_parsed = self.load_response()
            logging.info("Loaded existing response file")
        else:
            accessions = self.parse_accessions(
                self.accessions, accession_type=self.accession_type
            )
            self.metadata_obj.accessions = accessions
            self.metadata_obj.get_metadata()
            filtered_metadata = self.metadata_obj.filter_metadata(
                fields=("run_accession", "study_accession", "fastq_ftp", "fastq_md5")
            )
            ftp_metadata = self.parse_ftp_metadata(filtered_metadata)
            response_parsed = {}
            for row in ftp_metadata:
                obj = ENAObject(
                    row["run_accession"],
                    row["study_accession"],
                    row["fastq_ftp"],
                    row["fastq_md5"],
                )
                response_parsed[obj.key] = obj
            self.write_response_file(response_parsed)
            logging.info("Parsed metadata into response file")
        return response_parsed

    def wget(self, url, filename, tries=0):
        logging.info(f"Downloading {basename(filename)}")

        try:
            with urlrequest.urlopen(url) as response, open(filename, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
        except URLError as err:
            if tries <= self.retries:
                sleeptime = 2**tries
                logging.warning(
                    f"Download of {basename(filename)} failed. Reason: {err.reason}. Retrying after {sleeptime} seconds..."
                )
                sleep(sleeptime)
                self.wget(url, filename, tries + 1)
            else:
                # We probably don't want the program to terminate upon one failure,
                # but give the users a unique value to search for
                logging.warning(f"Download of {basename(filename)} failed entirely!")

    def load_response(self):
        response_parsed = {}
        with open(self.response_file) as fp:
            # skip header
            fp.readline()

            for line in fp:
                line = line.strip().split(",")
                obj = ENAObject(*line)
                response_parsed[obj.key] = obj

        if exists(self.progress_file):
            with open(self.progress_file) as prf:
                # skip header
                prf.readline()

                for line in prf:
                    line = line.strip().split(",")
                    obj = ENAObject(*line)
                    response_parsed[obj.key].md5_passed = line[-1]

        return response_parsed

    def write_response_file(self, response_parsed):
        with open(self.response_file, "w") as fp:
            fp.write(f"{ENAObject.header}\n")
            for data in response_parsed.values():
                fp.write(str(data) + "\n")

    def listener(self, m: str = None):
        if not exists(self.progress_file):
            with open(self.progress_file, "w") as f:
                f.write(f"{ENAObject.header}\n")

        if m is not None:
            with open(self.progress_file, "a") as f:
                f.write(str(m) + "\n")
                f.flush()

    @staticmethod
    def md5_check(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_fastqs(self, ena: ENAObject):
        url = "ftp://" + ena.ftp
        file_dir = self.output_dir
        if self.create_study_folders:
            file_dir = file_dir / ena.study_accession
            file_dir.mkdir(parents=True, exist_ok=True)
        outfile = file_dir / basename(ena.ftp)
        self.wget(url, outfile)
        md5_f = self.md5_check(outfile)

        ena.md5_passed = md5_f == ena.md5
        self.listener(str(ena))

    async def download_project_fastqs(self):
        response = self.get_ftp_paths()
        # Initialise file with header
        self.listener()

        to_dos = [item for item in response.values() if not item.md5_passed]
        # Run asyncio.to_thread because urllib.urlopen down in self.wget is not supported by asyncio,
        # nor is there any alternative that is
        await asyncio.gather(
            *[asyncio.to_thread(self.download_fastqs, item) for item in to_dos]
        )
