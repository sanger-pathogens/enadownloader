import csv
import io
import logging
import os
from collections import defaultdict
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
        post_data = {
            "result": "read_run",
            "fields": f"{','.join(fields)}",
            "includeAccessionType": f"{accession_type}",
            "includeAccessions": f"{','.join(accessions)}",
            "limit": 0,
            "format": "tsv",
        }
        try:
            response = requests.post(
                "https://www.ebi.ac.uk/ena/portal/api/search", data=post_data
            )
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
        return list(reader)

    def filter_metadata(self, fields=None):
        filtered_metadata = []
        if self.metadata is None:
            self.get_metadata()
        if fields is None:
            fields = []
        for row in self.metadata:
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
        available_columns = self.metadata[0].keys()
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
            for row in self.metadata:
                writer.writerow(row)

        logging.info(f"Wrote metadata to {output_path}")

    def get_taxonomy(self, taxon_id):
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

    def to_dict(self):
        studies = defaultdict(list)
        if not self.metadata:
            self.get_metadata()

        for row in self.metadata:
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
