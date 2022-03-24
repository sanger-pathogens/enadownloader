#!/usr/bin/env python3
"""
Robust tool to download fastq.gz files and metadata from ENA
"""

import argparse
import csv
import asyncio
import hashlib
import io
import logging
import os
import re
import shutil
from collections import defaultdict
from typing import Iterable
import urllib.request as urlrequest
from os.path import basename, exists, join, splitext
from pathlib import Path
from time import sleep
from urllib.error import URLError

import requests
import xmltodict

from excel import Data, ExcelWriter, FileHeader


def strtobool(val: str):
    if val in ("y", "yes", "true", "on", "1"):
        return True
    elif val in ("n", "no", "false", "off", "0"):
        return False
    else:
        raise ValueError(f"Unrecognised value: {val}")


class ENAObject:
    header = "run_accession,study_accession,fastq_ftp,fastq_md5,md5_passed"

    def __init__(
        self,
        run_accession: str,
        study_accession: str,
        ftp: str,
        md5: str,
        md5_passed: bool = False,
    ):
        self.run_accession = run_accession
        self.study_accession = study_accession
        self.ftp = ftp
        self.md5 = md5
        self.md5_passed = md5_passed
        self.key = splitext(basename(ftp))[0]

    @property
    def run_accession(self):
        return self._run_accession

    @run_accession.setter
    def run_accession(self, value):
        if value is None:
            raise ValueError("run_accession cannot be None")
        try:
            value = value.strip()
        except AttributeError:
            raise ValueError("run_accession must be a str")
        else:
            if not value:
                raise ValueError("run_accession must not be an empty str")
        self._run_accession = value

    @property
    def study_accession(self):
        return self._study_accession

    @study_accession.setter
    def study_accession(self, value):
        if value is None:
            raise ValueError("study_accession cannot be None")
        try:
            value = value.strip()
        except ValueError:
            raise ValueError("study_accession must be a str")
        else:
            if not value:
                raise ValueError("study_accession must not be an empty str")
        self._study_accession = value

    @property
    def ftp(self):
        return self._ftp

    @ftp.setter
    def ftp(self, value):
        if value is None:
            raise ValueError("ftp cannot be None")
        try:
            value = value.strip()
        except AttributeError:
            raise ValueError("ftp must be a str")
        else:
            if not value:
                raise ValueError("ftp must not be an empty str")
        self._ftp = value

    @property
    def md5(self):
        return self._md5

    @md5.setter
    def md5(self, value):
        if value is None:
            raise ValueError("md5 cannot be None")
        try:
            value = value.strip()
        except AttributeError:
            raise ValueError("md5 must be a str")
        else:
            if not value:
                raise ValueError("md5 must not be an empty str")
        self._md5 = value

    @property
    def md5_passed(self):
        pass

    @md5_passed.setter
    def md5_passed(self, value):
        self._md5_passed = (
            bool(strtobool(str(value).lower()))
            if not isinstance(value, bool)
            else value
        )

    @md5_passed.getter
    def md5_passed(self):
        return self._md5_passed

    def __str__(self):
        return ",".join(
            [
                self.run_accession,
                self.study_accession,
                self.ftp,
                self.md5,
                str(self.md5_passed),
            ]
        )

    def __repr__(self):
        return f"{self.__class__.__name__}: {str(self)}"


class ENAMetadata:
    def __init__(
        self,
        accessions: Iterable[str],
        accession_type: str,
        retries: int = 5,
    ):
        self.accessions = accessions
        self.accession_type = accession_type
        self.retries = retries
        self.metadata = None

    def get_available_fields(self, result_type: str = "read_run"):
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

    def get_metadata(self):
        response = self._get_metadata_response(self.accessions, self.accession_type)
        parsed_metadata = self._parse_metadata(response)
        self.metadata = parsed_metadata

    def _get_metadata_response(
        self,
        accessions: Iterable[str],
        accession_type: str,
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
                self._get_metadata_response(
                    accessions, accession_type, fields, tries + 1
                )
            else:
                logging.error(f"Failed to download metadata (tried {tries} times)")
                exit(1)
        else:
            return response

    def _parse_metadata(self, response):
        csv.register_dialect("unix-tab", delimiter="\t")
        reader = csv.DictReader(io.StringIO(response.text), dialect="unix-tab")
        return {row["run_accession"]: row for row in reader}

    def filter_metadata(self, fields=None):
        filtered_metadata = []
        if self.metadata is None:
            self.get_metadata()
        if fields is None:
            fields = []
        for run, row in self.metadata.items():
            try:
                new_row = {field: row[field] for field in fields}
            except KeyError as err:
                raise ValueError(
                    f"Invalid field in given fields: {err.args[0]}"
                ) from None
            else:
                filtered_metadata.append(new_row)
        return filtered_metadata

    @staticmethod
    def _validate_output_path(output_path, overwrite):
        output_path = Path(output_path).resolve()
        if output_path.exists():
            if not overwrite:
                raise ValueError(
                    "Output filepath already exists and overwrite is set to False"
                )
            else:
                os.makedirs(output_path.parent, exist_ok=True)
        return output_path

    def _validate_columns(self, columns):
        if self.metadata is None:
            self.get_metadata()
        available_columns = next(self.metadata.values()).keys()
        if columns is None:
            columns = sorted(available_columns)
        invalid_columns = set(columns).difference(available_columns)
        if invalid_columns:
            raise ValueError(f"Columns not available: {sorted(invalid_columns)}")
        return columns

    def write_metadata_file(
        self, output: str, overwrite: bool = False, columns: Iterable[str] = None
    ):
        output_path = ENAMetadata._validate_output_path(output, overwrite)
        columns = self._validate_columns(columns)
        csv.register_dialect("unix-tab", delimiter="\t")
        if self.metadata is None:
            self.get_metadata()

        with open(output_path, "w") as f:
            writer = csv.DictWriter(
                f, columns, extrasaction="ignore", dialect="unix-tab"
            )
            writer.writeheader()
            for run, row in self.metadata.items():
                writer.writerow(row)

        logging.info(f"Wrote metadata to {output_path}")

    @staticmethod
    def _get_taxonomy(taxon_id):
        url = f"https://www.ebi.ac.uk/ena/browser/api/xml/{taxon_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as err:
            logging.error(
                f"Could not get taxonomy information for taxon id {taxon_id}. Reason: {err}."
            )
            exit(1)
        else:
            root = xmltodict.parse(response.content.strip())
            return root["TAXON_SET"]

    def get_scientific_name(self, taxon_id: str):
        taxonomy = self._get_taxonomy(taxon_id)
        return taxonomy["taxon"]["@scientificName"]

    def to_dict(self):
        studies = defaultdict(list)
        if not self.metadata:
            self.get_metadata()

        for run, row in self.metadata.items():
            studies[row["study_accession"]].append(row)

        return studies

    def to_excel(self, output_dir: Path):
        """Generates one .xls file per ENA project to be fed into PathInfo legacy pipelines"""

        studies = self.to_dict()

        for study in studies.values():
            fh = FileHeader(
                "Pathogen Informatics",
                "PaM",
                "path-help",
                study[0]["instrument_platform"],
                study[0]["study_title"],
                1,
                "18/03/2025",
                study[0]["study_accession"],
            )

            data = []
            for row in study:
                if not row["fastq_ftp"].strip():
                    logging.warning(
                        f"Can't find ftp for accession: {row['run_accession']}. Skipping."
                    )
                    continue

                files = row["fastq_ftp"].split(";")

                if len(files) == 1:
                    filename = basename(files[0])
                    matefile = None
                else:
                    try:
                        filename = basename([f for f in files if "_1" in f][0])
                        matefile = basename([f for f in files if "_2" in f][0])
                    except IndexError:
                        logging.warning(
                            f"Can't correctly extract filename and matefile paths from row: {row}."
                        )
                        continue

                data.append(
                    Data(
                        filename=filename,
                        mate_file=matefile,
                        sample_name=row["sample_accession"],
                        taxon=int(row["tax_id"]),
                    )
                )

                writer = ExcelWriter(fh, data)

                outfile = str(output_dir / f"{fh.study_accession_number.value}.xls")
                writer.write(outfile)

                logging.info(f"Wrote Excel file to {outfile}")


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


class LegacyPathBuilder:
    def __init__(
        self, root_dir: str, db: str, metadata_obj: ENAMetadata, filepath: str
    ):
        self.root_dir = root_dir
        self.db = db
        self.metadata_obj = metadata_obj
        self.filepath = Path(filepath)
        self.filename = self.filepath.name

    def build_path(self):
        if self.metadata_obj.metadata is None:
            self.metadata_obj.get_metadata()
        run = re.sub(r"(?:_1|_2)?\..*$", "", self.filename)
        try:
            row = self.metadata_obj.metadata[run]
        except KeyError:
            raise ValueError(
                f"Could not find run_accession in metadata: {run}"
            ) from None
        # TODO: study identifier is retrieved from tracking database, ENA study_accession probably not appropriate
        study = row["study_accession"]
        sample = row["sample_accession"]
        # TODO: Original path in perl expected some library identifier here. This was typically supplied by
        #  the user in the input spreadsheet. Since ENA metadata does not supply any "library accession",
        #  we could use experiment_accession as a surrogate, unless there is a more appropriate value
        #  available from somewhere. Don't believe this has any bearing on pf functionality etc.
        experiment = row["experiment_accession"]
        taxon_scientific_name = self.metadata_obj.get_scientific_name(row["tax_id"])
        genus, species_subspecies = self._split_scientific_name(taxon_scientific_name)
        path_components = [
            self.root_dir,
            self.db,
            "seq-pipelines",
            genus,
            species_subspecies,
            "TRACKING",
            study,
            sample,
            "SLX",
            experiment,
            run,
            self.filename,
        ]
        return join(*path_components)

    @staticmethod
    def _split_scientific_name(name: str):
        names = [n.strip() for n in name.split(maxsplit=1)]
        try:
            genus, species_subspecies = names
        except ValueError:
            logging.error(
                f"Unexpected number of taxonomy names found in scientific name: {name}"
            )
            raise
        return names


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
            choices=["run", "sample", "study"],
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
            "-c",
            "--create-study-folders",
            action="store_true",
            help="Organise the downloaded files by study",
        )
        parser.add_argument(
            "-r",
            "--retries",
            default=5,
            type=cls.validate_retries,
            help="Amount to retry each fastq file upon download interruption",
        )
        parser.add_argument(
            "-v",
            "--verbosity",
            action="count",
            default=1,
            help="Use the option multiple times to increase output verbosity",
        )
        parser.add_argument(
            "-m",
            "--write-metadata",
            action="store_true",
            help="Output a metadata tsv for the given ENA accessions",
        )
        parser.add_argument(
            "-d",
            "--download-files",
            action="store_true",
            help="Download fastq files for the given ENA accessions",
        )
        parser.add_argument(
            "-e",
            "--write-excel",
            action="store_true",
            help="Create an External Import-compatible Excel file for legacy pipelines for the given ENA accessions, stored by project",
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
            raise argparse.ArgumentTypeError(
                f"input file of accessions does not exist or is not a file: {filepath}"
            )
        return filepath.resolve()

    @staticmethod
    def validate_dir(path: str):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as err:
            raise argparse.ArgumentTypeError(f"cannot create dir: {str(err)}")
        return Path(path).resolve()

    @staticmethod
    def validate_retries(retries: str):
        try:
            retries = int(retries)
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid int value: {retries!r}")
        if retries >= 0:
            return retries
        else:
            raise argparse.ArgumentTypeError(
                f"invalid int value (must be nonnegative): {retries!r}"
            )


if __name__ == "__main__":
    args = Parser.arg_parser()
    this_file = join(os.getcwd(), basename(splitext(__file__)[0]))

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

    with open(args.input) as f:
        accessions = set()
        for line in f:
            accession = line.strip()
            accessions.add(accession)

    enametadata = ENAMetadata(accessions=accessions, accession_type=args.type)

    if args.write_metadata:
        enametadata.write_metadata_file(
            args.output_dir / "metadata.tsv", overwrite=True
        )

    if args.write_excel:
        enametadata.to_excel(args.output_dir)

    if args.download_files:
        for project, rows in enametadata.to_dict().items():
            run_accessions = [row["run_accession"] for row in rows]
            enametadata_obj = ENAMetadata(
                accessions=run_accessions, accession_type="run"
            )
            enadownloader = ENADownloader(
                accessions=run_accessions,
                accession_type="run",
                output_dir=args.output_dir,
                create_study_folders=args.create_study_folders,
                metadata_obj=enametadata_obj,
                project_id=project,
                retries=args.retries,
            )
            asyncio.run(enadownloader.download_project_fastqs())

    # Test legacy path building
    legacy_path = LegacyPathBuilder(
        root_dir="/lustre/scratch118/infgen/pathogen/pathpipe",
        db="pathogen_prok_external",
        metadata_obj=enametadata,
        filepath="/path/to/some/cache/DRR028935_2.fastq.gz",
    ).build_path()
    print(legacy_path)
