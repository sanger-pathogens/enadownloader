import logging
import re
from os.path import join
from pathlib import Path

from enadownloader.enametadata import ENAMetadata


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
            logging.warning(
                f"Only one name found in scientific name: {name}. Using genus 'unknown' to resolve."
            )
            if len(names) == 1:
                genus, species_subspecies = "unknown", names[0]
            else:
                logging.error(
                    f"Unexpected number of taxonomy names found in scientific name: {name}"
                )
                raise
        species_subspecies = species_subspecies.replace(" ", "_")
        return genus, species_subspecies
