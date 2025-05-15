#!/usr/bin/env python3
"""
Robust tool to download fastq.gz files and metadata from ENA
"""

import asyncio
import hashlib
import logging
import shutil
import urllib.request as urlrequest
from os.path import basename, exists
from pathlib import Path
from time import sleep
from urllib.error import URLError

from enadownloader.enametadata import ENAMetadata
from enadownloader.utils import ENAFTPContainer


class ENADownloader:
    class InvalidRow(ValueError):
        pass

    class NoSuccessfulDownloads(ValueError):
        pass

    def __init__(
        self,
        metadata_obj: ENAMetadata,
        output_dir: Path,
        retries: int = 5,
        log_full_path: bool = False,
        cache: bool = True,
    ):
        self.metadata_obj = metadata_obj
        self.output_dir = output_dir
        self.retries = retries
        self.log_full_path = log_full_path
        self.cache = cache

        self.progress_file = self.output_dir / ".progress.csv"

    def parse_ftp_metadata(self, metadata, file_type) -> list[dict[str, str]]:
        parsed_metadata = []
        for row in metadata:
            try:
                new_rows = self.flatten_multivalued_ftp_attrs(row, file_type)
            except self.InvalidRow as err:
                logging.warning(
                    f"{self.__class__.__name__} - Found invalid metadata for run accession {row['run_accession']}. Reason: {err}. Skipping."
                )
                continue
            parsed_metadata.extend(new_rows)
        return parsed_metadata

    def flatten_multivalued_ftp_attrs(self, row, file_type) -> list[dict[str, str]]:
        if f"{file_type}_ftp" in row and not row[f"{file_type}_ftp"].strip():
            raise self.InvalidRow("No FTP URL was found")

        ftp_links = row[f"{file_type}_ftp"].split(";")
        md5s = row[f"{file_type}_md5"].split(";")

        if len(md5s) != len(ftp_links):
            raise self.InvalidRow(
                "The number of FTP URLs does not match the number of MD5 checksums"
            )

        rows = []
        for f, m in zip(ftp_links, md5s):
            new_row = row.copy()
            new_row[f"{file_type}_ftp"] = f
            new_row[f"{file_type}_md5"] = m
            rows.append(new_row)

        return rows

    def filter_metadata(self, fields: list[str]) -> list[dict[str, str]]:
        filtered_metadata = []
        self.metadata_obj.get_metadata()

        for row in self.metadata_obj.metadata.values():
            try:
                new_row = {field: row[field] for field in fields}
            except KeyError as err:
                raise ValueError(
                    f"Missing field in given fields: {err.args[0]}. Got: {list(row.keys())}"
                ) from None
            else:
                filtered_metadata.append(new_row)

        return filtered_metadata

    def get_ftp_paths(self, file_type) -> dict[str, ENAFTPContainer]:
        filtered_metadata = self.filter_metadata(
            fields=(
                "run_accession",
                "study_accession",
                f"{file_type}_ftp",
                f"{file_type}_md5",
            )
        )

        md5_passed_files = self.load_progress()

        ftp_metadata = self.parse_ftp_metadata(filtered_metadata, file_type)

        response_parsed = {}
        for row in ftp_metadata:
            obj = ENAFTPContainer(
                row["run_accession"],
                row["study_accession"],
                row[f"{file_type}_ftp"],
                row[f"{file_type}_md5"],
            )

            if obj in md5_passed_files:
                base = basename(obj.ftp)
                path = base if not self.log_full_path else self.output_dir / base
                logging.info("%s already exists. Skipping.", path)
                continue

            response_parsed[obj.key] = obj

        return response_parsed

    def wget(self, url: str, filename: str, tries: int = 0) -> bool:
        filebase = basename(filename)
        logging.info(f"Downloading {filebase}")

        try:
            with urlrequest.urlopen(url) as response, open(filename, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
        except URLError as err:
            if tries <= self.retries:
                sleeptime = 2**tries
                logging.warning(
                    f"Download of {filebase} failed. Reason: {err.reason}. Retrying after {sleeptime} seconds..."
                )
                sleep(sleeptime)
                self.wget(url, filename, tries + 1)
            else:
                # We probably don't want the program to terminate upon one failure,
                # but give the users a unique value to search for
                logging.warning(f"Download of {filebase} failed entirely!")
                return False
        else:
            if self.log_full_path:
                logging.info(f"{filename} downloaded")
            else:
                logging.info(f"{filebase} downloaded")
            return True

    def load_progress(self) -> set[ENAFTPContainer]:
        md5_passed_files = set()
        if self.cache:
            if exists(self.progress_file):
                with open(self.progress_file) as prf:
                    # skip header
                    prf.readline()

                    for line in prf:
                        line = line.strip().split(",")
                        obj = ENAFTPContainer(*line)
                        if obj.md5_passed:
                            md5_passed_files.add(obj)

        return md5_passed_files

    def write_progress_file(self, message: str = None):
        if not exists(self.progress_file):
            with open(self.progress_file, "w") as f:
                f.write(f"{ENAFTPContainer.header}\n")

        if message is not None:
            with open(self.progress_file, "a") as f:
                f.write(str(message) + "\n")
                f.flush()

    @staticmethod
    def md5_check(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_from_ftp(self, ena: ENAFTPContainer) -> bool:
        url = "ftp://" + ena.ftp
        outfile = self.output_dir / basename(ena.ftp)
        success = self.wget(url, outfile)
        if success:
            md5_f = self.md5_check(outfile)

            ena.md5_passed = md5_f == ena.md5
            self.write_progress_file(str(ena))
        return success

    async def download_all_files(self, file_type):
        ftp_paths = self.get_ftp_paths(file_type)

        to_dos = [item for item in ftp_paths.values() if not item.md5_passed]

        # Initialise files with header
        self.write_progress_file()

        # Limit concurrency to ENA 50 rate limit
        semaphore = asyncio.Semaphore(50)

        async def limited_download(item):
            async with semaphore:
                return await asyncio.to_thread(self.download_from_ftp, item)

        # Run asyncio.to_thread because urllib.urlopen down in self.wget is not supported by asyncio,
        # nor is there any alternative that is
        download_result = await asyncio.gather(
            *[limited_download(item) for item in to_dos]
        )

        # Raise an error if at least one download was attempted, but none were successful.
        if download_result and not any(download_result):
            raise self.NoSuccessfulDownloads("All scheduled downloads failed")
