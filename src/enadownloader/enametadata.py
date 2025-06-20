import csv
import io
import logging
import re
from collections import defaultdict
from datetime import datetime
from os.path import basename
from pathlib import Path
from time import sleep
from typing import Iterable

import requests
import xmltodict

from enadownloader.excel import Data, ExcelWriter, FileHeader


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
        self.api_link = "https://www.ebi.ac.uk/ena/portal/api"

    def get_available_fields(self, result_type: str = "read_run"):
        url = f"{self.api_link}/returnFields?dataPortal=ena&format=json&result={result_type}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.ConnectionError as err:
            logging.error(f"Failed to connect to ENA server. Reason: {err}.")
            exit(1)
        except requests.HTTPError as err:
            logging.error(
                f"Could not get available fields for ENA result type: {result_type}. Reason: {err}."
            )
            exit(1)
        fields = [entry["columnId"] for entry in response.json()]
        return fields

    def get_metadata(self):
        if self.metadata is not None:
            return self.metadata

        # If this gets called more than once per session we're doing something wrong
        logging.info("Retrieving metadata from ENA")
        response = self._get_metadata_response(self.accessions, self.accession_type)
        parsed_metadata = self._parse_metadata(io.StringIO(response.text))

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
        post_data = self._build_post_data(fields, accession_type, accessions)
        response = requests.post(f"{self.api_link}/search", data=post_data)
        try:
            response.raise_for_status()
        except (requests.ConnectionError, requests.HTTPError) as err:
            if tries < self.retries:
                sleeptime = 2**tries
                logging.warning(
                    f"Download of metadata failed. Reason: {err}. Retrying after {sleeptime} seconds..."
                )
                sleep(sleeptime)
                self._get_metadata_response(
                    accessions, accession_type, fields, tries + 1
                )
            else:
                logging.error(f"Failed to download metadata (tried {tries + 1} times)")
                exit(1)
        else:
            response.encoding = "UTF-8"
            return response

    @staticmethod
    def _build_post_data(fields, accession_type, accessions):
        post_data = {
            "result": "read_run",
            "fields": ",".join(fields),
            "limit": 0,
            "format": "tsv",
        }

        # See https://ena-docs.readthedocs.io/en/latest/submit/general-guide/accessions.html
        # and https://regex101.com/r/W0ldhu/1
        secondary_regex = re.compile("^(?:[EDS]RP|[EDS]RS)[0-9]{6,}$")

        primary, secondary = [], []
        for accession in accessions:
            if secondary_regex.fullmatch(accession):
                secondary.append(accession)
            else:
                primary.append(accession)

        if primary:
            post_data["includeAccessionType"] = accession_type
            post_data["includeAccessions"] = ",".join(primary)

        if secondary:
            query_key = f"secondary_{accession_type}_accession"  # E.g. secondary_sample_accession
            post_data["query"] = " OR ".join(
                f'{query_key}="{accession}"' for accession in secondary
            )

        return post_data

    def _parse_metadata(self, metadata: io.TextIOBase) -> dict[str, dict[str, str]]:
        csv.register_dialect("unix-tab", delimiter="\t")
        reader = csv.DictReader(metadata, dialect="unix-tab")
        return {row["run_accession"]: row for row in reader}

    @property
    def columns(self):
        self.get_metadata()
        return next(iter(self.metadata.values())).keys()

    def write_metadata_file(self, output_path: Path):
        csv.register_dialect("unix-tab", delimiter="\t")
        self.get_metadata()

        output_file = output_path / "metadata.tsv"
        with open(output_file, "w") as f:
            writer = csv.DictWriter(
                f, self.columns, extrasaction="ignore", dialect="unix-tab"
            )
            writer.writeheader()
            for row in self.metadata.values():
                writer.writerow(row)

        logging.info(f"Wrote metadata to {output_file.name}")

    def _get_taxonomy(self, taxon_id):
        url = f"{self.api_link}/xml/{taxon_id}"
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

    def group_by_project(self):
        studies = defaultdict(list)
        self.get_metadata()

        for row in self.metadata.values():
            studies[row["study_accession"]].append(row)

        return studies

    # TODO now that this is a static method it might as well get moved to the Bridger
    @staticmethod
    def to_excel(output_dir: Path, rows: list[dict[str, str]]):
        """Generates a .xls file to be fed into PathInfo legacy pipelines"""

        today = datetime.today()
        fh = FileHeader(
            "Pathogen Informatics",
            "PaM",
            "path-help",
            rows[0]["instrument_platform"],
            rows[0]["study_title"],
            1,
            "/".join(map(str, (today.day, today.month, today.year + 10))),
            rows[0]["study_accession"],
        )

        data = []
        for row in rows:
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
