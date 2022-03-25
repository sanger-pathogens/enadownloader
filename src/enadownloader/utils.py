import logging
from os.path import basename, splitext


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
        return self._md5_passed

    @md5_passed.setter
    def md5_passed(self, value):
        self._md5_passed = (
            bool(strtobool(str(value).lower()))
            if not isinstance(value, bool)
            else value
        )

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

    def __hash__(self):
        return hash(self.ftp)

    def __eq__(self, other):
        return self.ftp == other.ftp


class AccessionValidator:
    @staticmethod
    def validate_accession(accession, accession_type):
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

    @classmethod
    def parse_accessions(cls, accessions, accession_type="run"):
        parsed_accessions = []
        for accession in accessions:
            try:
                cls.validate_accession(accession, accession_type)
            except ValueError as err:
                logging.warning(f"{err}. Skipping...")
                continue
            else:
                parsed_accessions.append(accession)
        return parsed_accessions
