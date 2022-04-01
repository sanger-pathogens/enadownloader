import pytest
import copy
from enadownloader.utils import strtobool, AccessionValidator, ENAFTPContainer

""" Unit tests for the utils module """

STUDY_TYPE = "study"
SAMPLE_TYPE = "sample"
RUN_TYPE = "run"
TEST_FTP_URL = (
    "ftp.sra.ebi.ac.uk/vol1/fastq/SRR250/003/SRR25042885/SRR25042885.fastq.gz"
)
TEST_STUDY_ACCESSION = "PRJNA797994"
TEST_RUN_ACCESSION = "SAMN25043055"
TEST_MD5 = "0b512d2dc31685983456bd56fd836544"


@pytest.fixture
def run_accessions():
    accessions = ["SRR25042885", "ERR25042885", "DRR25042885"]
    yield accessions


@pytest.fixture
def sample_accessions():
    accessions = ["ERS25042885", "DRS25042885", "SRS25042885", "SAM25042885"]
    yield accessions


@pytest.fixture
def study_accessions():
    accessions = ["SRP25042885", "ERP25042885", "DRP25042885", "PRJ25042885"]
    yield accessions


@pytest.fixture
def illegal_accessions():
    accessions = ["illegal1", "illegal2", "illegal3", "illegal4"]
    yield accessions


@pytest.fixture
def enaftpcontainer():
    obj = ENAFTPContainer(
        TEST_RUN_ACCESSION,
        TEST_STUDY_ACCESSION,
        TEST_FTP_URL,
        TEST_MD5,
    )
    yield obj


def test_strtobool():
    """Test the strtobool method"""
    assert strtobool("y")
    assert not strtobool("n")
    assert strtobool("yes")
    assert not strtobool("no")
    assert strtobool("true")
    assert not strtobool("false")
    assert strtobool("on")
    assert not strtobool("off")
    assert strtobool("1")
    assert not strtobool("0")
    with pytest.raises(ValueError):
        strtobool("foobar")


def test_enaftpcontainer_to_string(enaftpcontainer):
    """Test conversion of an ENAFTPContainer to a string"""
    under_test = enaftpcontainer
    assert str(under_test) == ",".join(
        [TEST_RUN_ACCESSION, TEST_STUDY_ACCESSION, TEST_FTP_URL, TEST_MD5, str(False)]
    )
    under_test.md5_passed = True
    assert str(under_test) == ",".join(
        [TEST_RUN_ACCESSION, TEST_STUDY_ACCESSION, TEST_FTP_URL, TEST_MD5, str(True)]
    )


def test_enaftpcontainer_run_accession(enaftpcontainer):
    """Test ENAFTPContainer run accession property"""
    under_test = enaftpcontainer
    assert under_test.run_accession == TEST_RUN_ACCESSION
    with pytest.raises(ValueError) as e:
        under_test.run_accession = None
    with pytest.raises(ValueError) as e:
        under_test.run_accession = 5
    with pytest.raises(ValueError) as e:
        under_test.run_accession = ""


def test_enaftpcontainer_study_accession(enaftpcontainer):
    """Test ENAFTPContainer study accession property"""
    under_test = enaftpcontainer
    assert under_test.study_accession == TEST_STUDY_ACCESSION
    with pytest.raises(ValueError) as e:
        under_test.study_accession = None
    with pytest.raises(ValueError) as e:
        under_test.study_accession = 5
    with pytest.raises(ValueError) as e:
        under_test.study_accession = ""


def test_enaftpcontainer_ftp(enaftpcontainer):
    """Test ENAFTPContainer ftp property"""
    under_test = enaftpcontainer
    assert under_test.ftp == TEST_FTP_URL
    with pytest.raises(ValueError) as e:
        under_test.ftp = None
    with pytest.raises(ValueError) as e:
        under_test.ftp = 5
    with pytest.raises(ValueError) as e:
        under_test.ftp = ""


def test_enaftpcontainer_md5(enaftpcontainer):
    """Test ENAFTPContainer md5 property"""
    under_test = enaftpcontainer
    assert under_test.md5 == TEST_MD5
    with pytest.raises(ValueError) as e:
        under_test.md5 = None
    with pytest.raises(ValueError) as e:
        under_test.md5 = 5
    with pytest.raises(ValueError) as e:
        under_test.md5 = ""


def test_enaftpcontainer_md5_passed(enaftpcontainer):
    """Test ENAFTPContainer md5_passed property"""
    under_test = enaftpcontainer
    assert not under_test.md5_passed
    under_test.md5_passed = "TRUE"
    assert under_test.md5_passed
    under_test.md5_passed = "FALSE"
    assert not under_test.md5_passed
    under_test.md5_passed = "true"
    assert under_test.md5_passed
    under_test.md5_passed = "false"
    assert not under_test.md5_passed
    under_test.md5_passed = True
    assert under_test.md5_passed
    under_test.md5_passed = False
    assert not under_test.md5_passed


def test_enaftpcontainer_repr(enaftpcontainer):
    """Test ENAFTPContainer repr"""
    assert (
        repr(enaftpcontainer)
        == f"{enaftpcontainer.__class__.__name__}: {str(enaftpcontainer)}"
    )


def test_enaftpcontainer_hash(enaftpcontainer):
    """Test ENAFTPContainer hash"""
    # Test equal
    other_obj = copy.copy(enaftpcontainer)
    assert hash(enaftpcontainer) == hash(other_obj)
    # Test not equal
    other_obj.ftp = "ftp.sra.ebi.ac.uk/test/file.fastq.gz"
    assert hash(enaftpcontainer) != hash(other_obj)


def test_enaftpcontainer_equals(enaftpcontainer):
    """Test ENAFTPContainer equals"""
    assert hash(enaftpcontainer) == hash(enaftpcontainer.ftp)
    # Test equal
    other_obj = copy.copy(enaftpcontainer)
    assert enaftpcontainer == other_obj
    # Test not equal
    other_obj.ftp = "ftp.sra.ebi.ac.uk/test/file.fastq.gz"
    assert enaftpcontainer != other_obj


def test_validate_accession_for_valid_accessions(
    run_accessions, sample_accessions, study_accessions
):
    """Test the AccessionValidator validate_accession method with valid accessions"""
    try:
        for accession in run_accessions:
            AccessionValidator.validate_accession(accession, RUN_TYPE)
        for accession in sample_accessions:
            AccessionValidator.validate_accession(accession, SAMPLE_TYPE)
        for accession in study_accessions:
            AccessionValidator.validate_accession(accession, STUDY_TYPE)
    except ValueError as e:
        pytest.fail("Unexpected error raised: " + str(e))


def test_validate_accession_for_invalid_accessions():
    """Test the AccessionValidator validate_accession method with invalid accessions"""
    with pytest.raises(ValueError) as e:
        AccessionValidator.validate_accession("foobar", RUN_TYPE)
    assert RUN_TYPE in str(e)
    with pytest.raises(ValueError) as e:
        AccessionValidator.validate_accession("foobar", SAMPLE_TYPE)
    assert SAMPLE_TYPE in str(e)
    with pytest.raises(ValueError) as e:
        AccessionValidator.validate_accession("foobar", STUDY_TYPE)
    assert STUDY_TYPE in str(e)
    with pytest.raises(ValueError) as e:
        AccessionValidator.validate_accession("PRJ25042885", "illegal")
    assert "accession_type" in str(e)


def test_parse_accessions_for_valid_accessions(
    run_accessions, sample_accessions, study_accessions
):
    """Test the AccessionValidator parse_accessions method with valid accessions"""
    try:
        assert (
            AccessionValidator.parse_accessions(run_accessions, RUN_TYPE)
            == run_accessions
        )
        assert (
            AccessionValidator.parse_accessions(sample_accessions, SAMPLE_TYPE)
            == sample_accessions
        )
        assert (
            AccessionValidator.parse_accessions(study_accessions, STUDY_TYPE)
            == study_accessions
        )
    except ValueError as e:
        pytest.fail("Unexpected error raised: " + str(e))


def test_parse_accessions_for_invalid_accessions(run_accessions, illegal_accessions):
    """Test the AccessionValidator parse_accessions method with invalid accessions"""
    assert len(AccessionValidator.parse_accessions(illegal_accessions, RUN_TYPE)) == 0
    assert (
        len(AccessionValidator.parse_accessions(illegal_accessions, SAMPLE_TYPE)) == 0
    )
    assert len(AccessionValidator.parse_accessions(illegal_accessions, STUDY_TYPE)) == 0
    assert len(AccessionValidator.parse_accessions(run_accessions, "illegal")) == 0
