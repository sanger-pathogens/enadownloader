#!/usr/bin/env python3
"""
Robust tool to download fastq.gz files and metadata from ENA
"""

import argparse
import csv
import hashlib
import io
import logging
import multiprocessing as mp
import os
import shutil
from typing import Iterable
import urllib.request as urlrequest
from distutils.util import strtobool
from os.path import basename, exists, join, splitext
from pathlib import Path
from time import sleep
from urllib.error import URLError

import requests
import xmltodict


class ENAObject:
    header = "run_accession,fastq_ftp,fastq_md5,md5_passed"

    def __init__(
        self, run_accession: str, ftp: str, md5: str, md5_passed: bool = False
    ):
        self.run_accession = run_accession
        self.ftp = ftp
        self.md5 = md5
        self.md5_passed = md5_passed
        self.key = splitext(basename(ftp))[0]

    @property
    def md5_passed(self):
        pass

    @md5_passed.setter
    def md5_passed(self, value):
        self._md5_passed = (
            bool(strtobool(value)) if not isinstance(value, bool) else value
        )

    @md5_passed.getter
    def md5_passed(self):
        return self._md5_passed

    def __str__(self):
        return ",".join([self.run_accession, self.ftp, self.md5, str(self.md5_passed)])

    def __repr__(self):
        return f"{self.__class__.__name__}: {str(self)}"


class ENAMetadata:
    class InvalidRow(ValueError):
        pass

    def __init__(self, output_dir: Path, metadata_filename: str, retries: int):
        self.output_dir = output_dir
        self.metadata_file = output_dir / metadata_filename
        self.retries = retries

    def get_available_fields(self):
        result_type = "read_run"
        url = f"https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=ena&format=json&result={result_type}"
        response = requests.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            logging.error(
                f"Could not get available fields for ENA result type: {result_type}. Reason: {err}."
            )
            exit(1)
        fields = [entry["columnId"] for entry in response.json()]
        return fields

    def get_metadata(
        self,
        accessions: Iterable[str],
        accession_type: str = "run",
        fields: Iterable[str] = None,
        tries: int = 0,
    ) -> requests.Response:
        """Note run_accession and sample_accession fields are always included for run accession metadata
        (even when these are not specified in `fields` arg)
        """
        if fields is None:
            fields = self.get_available_fields()
        url = (
            "https://www.ebi.ac.uk/ena/portal/api/search?"
            "result=read_run"
            f"&fields={','.join(fields)}"
            f"&includeAccessionType={accession_type}"
            f"&includeAccessions={','.join(accessions)}"
            f"&limit=0"
            f"&format=tsv"
        )
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as err:
            if tries <= self.retries:
                sleeptime = 2**tries
                logging.warning(
                    f"Download of metadata failed. Reason: {err}. Retrying after {sleeptime} seconds..."
                )
                sleep(sleeptime)
                self.get_metadata(accessions, accession_type, fields, tries + 1)
            else:
                logging.error(f"Failed to download metadata (tried {tries} times)")
                exit(1)
        else:
            return response

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

    def parse_metadata(self, response):
        parsed_metadata = []
        csv.register_dialect("unix-tab", delimiter="\t")
        reader = csv.DictReader(io.StringIO(response.text), dialect="unix-tab")
        for row in reader:
            try:
                new_rows = self.flatten_multivalued_ftp_attrs(row)
            except self.InvalidRow as err:
                logging.warning(
                    f"Found invalid metadata for run accession {row['run_accession']}. Reason: {err}. Skipping."
                )
                continue
            for new_row in new_rows:
                parsed_metadata.append(new_row)
        return parsed_metadata

    def write_metadata_file(self, parsed_metadata):
        csv.register_dialect("unix-tab", delimiter="\t")
        fieldnames = sorted(parsed_metadata[0].keys())

        with open(self.metadata_file, "w") as f:
            writer = csv.DictWriter(f, fieldnames, dialect="unix-tab")
            writer.writeheader()
            for row in parsed_metadata:
                writer.writerow(row)

    def get_taxonomy(self, taxon_id):
        url = f"https://www.ebi.ac.uk/ena/browser/api/xml/{taxon_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as err:
            logging.error(f"Could not get taxonomy information for taxon id {taxon_id}. Reason: {err}.")
            exit(1)
        else:
            root = xmltodict.parse(response.content.strip())
            return root["TAXON_SET"]

    def get_scientific_name(self, taxonomy):
        return taxonomy["taxon"]["@scientificName"]

    def split_scientific_name(self, name: str):
        names = [n.strip() for n in name.split(maxsplit=1)]
        try:
            genus, species_subspecies = names
        except ValueError:
            logging.error(
                f"Unexpected number of taxonomy names found in scientific name: {name}"
            )
            raise
        return names


class ENADownloader:
    def __init__(
        self,
        input_file: str,
        accession_type: str,
        threads: int,
        metadata_file: str,
        output_dir: Path,
        retries: int = 5,
    ):
        self.input_file = input_file
        self.input_filename = splitext(basename(input_file))[0]
        self.accessions_type = accession_type
        self.threads = threads
        self.output_dir = output_dir
        self.retries = retries
        self.metadata_getter = ENAMetadata(self.output_dir, "metadata.tsv", self.retries)

        self.response_file = join(output_dir, f".{self.input_filename}.csv")
        self.progress_file = join(output_dir, f".{self.input_filename}.progress.csv")

    def validate_accession(self, accession, accession_type):
        if accession_type == "run":
            if not accession.startswith(("SRR", "ERR", "DRR")):
                raise ValueError(f"Invalid run accession: {accession}")
        elif accession_type == "study":
            if not accession.startswith(("SRP", "ERP", "DRP", "PRJ")):
                raise ValueError(f"Invalid study accession: {accession}")
        else:
            raise ValueError(f"Invalid accession_type: {accession_type}")

    def parse_accessions(self, filepath, accession_type="run"):
        accessions = set()
        with open(filepath) as f:
            for line in f:
                accession = line.strip()
                try:
                    self.validate_accession(accession, accession_type)
                except ValueError as err:
                    logging.warning(f"{err}. Skipping...")
                    continue
                accessions.add(accession)
        return accessions

    def get_ftp_paths(self, filepath, accession_type="run"):
        if exists(self.response_file):
            response_parsed = self.load_response()
            logging.info("Loaded existing response file")
        else:
            accessions = self.parse_accessions(filepath, accession_type=accession_type)
            response = self.metadata_getter.get_metadata(
                accessions,
                accession_type=accession_type,
                fields=("fastq_ftp", "fastq_md5", "tax_id"),
                tries=1,
            )
            parsed_metadata = self.metadata_getter.parse_metadata(response)
            response_parsed = {}
            for row in parsed_metadata:
                obj = ENAObject(
                    row["run_accession"], row["fastq_ftp"], row["fastq_md5"]
                )
                response_parsed[obj.key] = obj
            self.write_response_file(response_parsed)
            logging.info("Parsed metadata into response file")
        return response_parsed

    def wget(self, url, filename, tries=0):
        print(f"Downloading {filename}")

        try:
            with urlrequest.urlopen(url) as response, open(filename, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
        except URLError as err:
            if tries <= self.retries:
                sleeptime = 2**tries
                print(
                    f"Download of {filename} failed. Reason: {err.reason}. Retrying after {sleeptime} seconds..."
                )
                sleep(sleeptime)
                self.wget(url, filename, tries + 1)
            else:
                raise

    @staticmethod
    def parse_file_report(response: requests.Response):
        response_files = {}
        keys = None

        for line in response.text.split("\n"):
            if keys is None:
                keys = line
                continue
            try:
                run_accession, fastq_ftp, fastq_md5 = line.strip().split()
            except ValueError:
                continue

            file_links = fastq_ftp.split(";")
            md5s = fastq_md5.split(";")

            assert len(file_links) == len(md5s)

            for f, m in zip(file_links, md5s):
                obj = ENAObject(run_accession, f, m)
                response_files[obj.key] = obj

        logging.info("fastq files parsed")
        return response_files

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

    def listener(self, queue: mp.Queue):
        if not exists(self.progress_file):
            with open(self.progress_file, "w") as f:
                f.write(f"{ENAObject.header}\n")

        with open(self.progress_file, "a") as f:
            while True:
                m = queue.get()
                assert isinstance(
                    m, (str, ENAObject)
                ), f"Unrecognised type sent in queue: {m} of type {m.__class__.__name__}"
                if m == "kill":
                    queue.task_done()
                    break

                else:
                    f.write(str(m) + "\n")
                    f.flush()
                    queue.task_done()

    @staticmethod
    def md5_check(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_fastqs(self, ena: ENAObject, queue: mp.Queue, output_dir: Path):
        url = "ftp://" + ena.ftp
        outfile = output_dir / basename(ena.ftp)
        self.wget(url, outfile)
        md5_f = self.md5_check(outfile)

        ena.md5_passed = md5_f == ena.md5
        queue.put(ena)

    def download_project_fastqs(self):
        response = self.get_ftp_paths(
            self.input_file, accession_type=self.accession_type
        )

        number_of_threads = self.threads
        manager = mp.Manager()
        queue = manager.Queue()
        with mp.Pool(processes=number_of_threads) as pool:
            pool.apply_async(self.listener, (queue,))

            res = pool.starmap_async(
                self.download_fastqs,
                [
                    (item, queue, args.output_dir)
                    for item in response.values()
                    if not item.md5_passed
                ],
            )
            res.get()

            queue.put("kill")
            queue.join()


class Parser:
    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "-i",
            "--input",
            required=True,
            type=cls.validate_input,
            help="Path to file containing ENA accessions",
        )
        parser.add_argument(
            "-t",
            "--type",
            required=True,
            choices=["run", "study"],
            help="Type of ENA accessions",
        )
        parser.add_argument(
            "-o",
            "--output_dir",
            default=os.getcwd(),
            type=cls.validate_dir,
            help="Directory in which to save downloaded files",
        )
        parser.add_argument(
            "-@",
            "--threads",
            default=2,
            type=cls.validate_threads,
            help="Number of threads to use for download",
        )
        parser.add_argument(
            "-v",
            "--verbosity",
            action="count",
            default=0,
            help="Use the option multiple times to increase output verbosity",
        )
        args = parser.parse_args()

        # Set log_level arg
        if args.verbosity >= 2:
            args.log_level = logging.DEBUG
        elif args.verbosity >= 1:
            args.log_level = logging.INFO
        else:
            args.log_level = logging.WARN

        return args

    @staticmethod
    def validate_input(filepath: str):
        filepath = Path(filepath)
        if not filepath.is_file():
            raise argparse.ArgumentTypeError(f"input file of accessions does not exist or is not a file: {filepath}")
        return filepath.resolve()

    @staticmethod
    def validate_dir(path: str):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as err:
            raise argparse.ArgumentTypeError(f"cannot create dir: {str(err)}")
        return Path(path).resolve()

    @staticmethod
    def validate_threads(threads: str):
        try:
            threads = int(threads)
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid int value: {threads!r}")
        if 1 < threads <= 100:
            return threads
        else:
            raise argparse.ArgumentTypeError(
                f"invalid int value (must be between 2 and 100): {threads!r}"
            )


if __name__ == "__main__":
    args = Parser.arg_parser()
    this_file = join(args.output_dir, basename(splitext(__file__)[0]))

    # Set up logging
    fh = logging.FileHandler(f"{this_file}.log", mode="w")
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()

    # noinspection PyArgumentList
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[fh, sh],
    )

    enadownloader = ENADownloader(
        input_file=args.input,
        accession_type=args.type,
        threads=args.threads,
        metadata_file="metadata.tsv",
        output_dir=args.output_dir,
    )
    enadownloader.download_project_fastqs()

    # Various bits of test code...
    # enadownloader = ENADownloader(
    #     accession="nonsense", threads=2, output_dir="output_dir"
    # )
    # accessions = enadownloader.parse_accessions("sra_ids_test.txt")
    # metadata = enadownloader.get_metadata(accessions)
    # parsed_metadata = enadownloader.parse_metadata(metadata)
    # enadownloader.write_metadata_file(parsed_metadata)
    # for row in parsed_metadata:
    #     print(row['tax_id'])
    #
    # # Note: cannot handle GEO accessions (from NCBI's Gene Expression Omnibus) e.g. "GSM4907283"
    # # enadownloader.get_metadata(["SRR9984183",
    # #                             "SRR13191702",
    # #                             "ERR1160846",
    # #                             "ERR1109373",
    # #                             "DRR028935",
    # #                             "DRR026872",
    # #                             "SRR12848126",
    # #                             "SRR14593545",
    # #                             "SRR14709033",])
    #
    # # Check we can get taxonomy
    # # json_taxonomy = enadownloader.get_taxonomy(408170)
    # # name = enadownloader.get_scientific_name(json_taxonomy)
    # # names = enadownloader.split_scientific_name(name)
    # # print(names)
    #
    # run_accessions = enadownloader.parse_run_accessions("sra_ids_test.txt")
    # response = enadownloader.get_metadata(run_accessions, fields=None, retries=1)
    # parsed_metadata = enadownloader.parse_metadata(response)
    # enadownloader.write_metadata_file(parsed_metadata, "metadata.tsv")
    # # for row in parsed_metadata:
    # #     print(row)
    # # fields = enadownloader.get_available_fields()
    # # print(fields)
    #
    # # get_metadata works for a study/sample accessions too...
    # response2 = enadownloader.get_metadata(["PRJDB4356"], accession_type="study", fields=("fastq_ftp", "fastq_md5", "tax_id"), retries=1)
    # parsed_metadata2 = enadownloader.parse_metadata(response2)
    # print(parsed_metadata2)
