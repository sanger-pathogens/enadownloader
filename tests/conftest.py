from io import BytesIO

import pytest

from enadownloader import ENADownloader, ENAMetadata
from enadownloader.utils import ENAFTPContainer
from enadownloader.enadownloader import urlrequest


"""
Specially named conftest.py allows fixtures to be shared among other files
"""


@pytest.fixture
def output_path(tmp_path):
    o = tmp_path / "test"
    o.mkdir()
    yield o


@pytest.fixture
def ftp_metadata():
    yield [
        {
            "run_accession": "SRR25042885",
            "study_accession": "PRJNA123456",
            "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/SRR250/001/SRR25042885/SRR25042885.fastq.gz",
            "fastq_md5": "5fc34f3bd5a7f2696902d661d8b21981",
        },
        {
            "run_accession": "ERR25042885",
            "study_accession": "PRJNA123456",
            "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR250/001/ERR25042885/ERR25042885.fastq.gz",
            "fastq_md5": "b92dedfe7e6ddb3dbc3ce7ecc12e1f8b",
        },
        {
            "run_accession": "DRR25042885",
            "study_accession": "PRJNA123456",
            "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/DRR250/001/DRR25042885/DRR25042885.fastq.gz",
            "fastq_md5": "e113c5016866f0f0cfc782c51352d368",
        },
    ]


@pytest.fixture
def ENAFTPContainers(ftp_metadata):
    yield [
        ENAFTPContainer(
            run["run_accession"],
            run["study_accession"],
            run["fastq_ftp"],
            run["fastq_md5"],
        )
        for run in ftp_metadata
    ]


@pytest.fixture
def accessions(ftp_metadata):
    yield [x["run_accession"] for x in ftp_metadata]


@pytest.fixture
def metadata(accessions):
    yield ENAMetadata(accessions, accession_type="run")


@pytest.fixture
def downloader(metadata, output_path, ftp_metadata):
    e = ENADownloader(metadata, output_path)
    e.metadata_obj.metadata = {
        container["run_accession"]: container for container in ftp_metadata
    }
    yield e


@pytest.fixture
def mock_urlopen(mocker):
    mocked = mocker.patch.object(urlrequest, "urlopen")
    mocked.return_value = BytesIO(b"I am a fastq file")
    yield mocked


@pytest.fixture
def run_accessions():
    accessions = {"SRR9984183", "SRR13191702", "ERR1160846"}
    yield accessions


@pytest.fixture
def sample_accessions():
    accessions = {
        "SAMD00002711",
        "SRS7053897",
        "SAMN15546073",
        "SRS7053865",
        "SAMD00013986",
        "DRS000237",
    }
    yield accessions


@pytest.fixture
def study_accessions():
    accessions = {"SRP25042885", "ERP25042885", "DRP25042885", "PRJ25042885"}
    yield accessions
