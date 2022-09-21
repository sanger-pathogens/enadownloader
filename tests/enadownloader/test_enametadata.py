import pytest
import requests
import io
import collections
import enadownloader.excel
from enadownloader.enametadata import ENAMetadata

""" Unit tests for the enametadata module """

RUN_TYPE = "run"
SAMPLE_TYPE = "sample"
TEST_TAXON_ID = "123"
TEST_JSON_FIELD_SET = [
    {"columnId": "study_accession", "description": "study accession number"},
    {
        "columnId": "secondary_study_accession",
        "description": "secondary study accession number",
    },
    {"columnId": "sample_accession", "description": "sample accession number"},
    {
        "columnId": "secondary_sample_accession",
        "description": "secondary sample accession number",
    },
    {"columnId": "run_accession", "description": "run accession number"},
]

TEST_XML_TAXONOMY_FIELDS = """
<?xml version="1.0" encoding="UTF-8"?>
<TAXON_SET>
    <taxon scientificName="Pirellula" taxId="123" parentTaxId="2691357" rank="genus" hidden="false" taxonomicDivision="PRO" geneticCode="11">
        <lineage>
            <taxon scientificName="Pirellulaceae" taxId="2691357" rank="family" hidden="false"></taxon>
            <taxon scientificName="Pirellulales" taxId="2691354" rank="order" hidden="false"></taxon>
            <taxon scientificName="Planctomycetia" taxId="203683" rank="class" hidden="false"></taxon>
            <taxon scientificName="Planctomycetes" taxId="203682" rank="phylum" hidden="false"></taxon>
            <taxon scientificName="PVC group" taxId="1783257" hidden="true"></taxon>
            <taxon scientificName="Bacteria" commonName="eubacteria" taxId="2" rank="superkingdom" hidden="false"></taxon>
            <taxon scientificName="cellular organisms" taxId="131567" hidden="true"></taxon>
            <taxon scientificName="root" taxId="1" hidden="true"></taxon>
        </lineage>
        <children>
            <taxon scientificName="unclassified Pirellula" taxId="2639138"></taxon>
            <taxon scientificName="environmental samples" taxId="70745"></taxon>
            <taxon scientificName="Pirellula staleyi" taxId="125" rank="species"></taxon>
        </children>
    </taxon>
</TAXON_SET>
"""

TEST_SEARCH_FIELDS = (
    "study_accession\trun_accession\n"
    "PRJEB11419\tERR1160846\n"
    "PRJNA682076\tSRR13191702\n"
    "PRJNA560329\tSRR9984183\n"
)

EXPECTED_FIELD_LIST = [
    "study_accession",
    "secondary_study_accession",
    "sample_accession",
    "secondary_sample_accession",
    "run_accession",
]
EXPECTED_FIELDS_URL = "https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=ena&format=json&result="
EXPECTED_SEARCH_URL = "https://www.ebi.ac.uk/ena/portal/api/search"
EXPECTED_TAXONOMY_URL = "https://www.ebi.ac.uk/ena/browser/api/xml/"


@pytest.fixture
def mock_fields_request(mocker):
    request = mocker.patch.object(requests, "get")
    request.return_value.json.return_value = TEST_JSON_FIELD_SET
    yield request


@pytest.fixture
def mock_fields_request_error(mocker):
    request = mocker.patch.object(requests, "get")
    request.return_value.raise_for_status.side_effect = requests.HTTPError(
        "Major malfunction"
    )
    yield request


@pytest.fixture
def mock_search_request(mocker):
    request = mocker.patch.object(requests, "post")
    request.return_value.text = TEST_SEARCH_FIELDS
    yield request


@pytest.fixture
def mock_search_request_error(mocker):
    request = mocker.patch.object(requests, "post")
    request.return_value.raise_for_status.side_effect = requests.HTTPError(
        "Major malfunction"
    )
    yield request


@pytest.fixture
def mock_taxonomy_request(mocker):
    request = mocker.patch.object(requests, "get")
    request.return_value.content = TEST_XML_TAXONOMY_FIELDS
    yield request


@pytest.fixture
def mock_taxonomy_request_error(mocker):
    request = mocker.patch.object(requests, "get")
    request.return_value.raise_for_status.side_effect = requests.HTTPError(
        "Major malfunction"
    )
    yield request


@pytest.fixture
def run_accessions():
    accessions = ["SRR9984183", "SRR13191702", "ERR1160846"]
    yield accessions


@pytest.fixture
def sample_accessions():
    accessions = [
        "SAMD00002711",
        "SRS7053897",
        "SAMN15546073",
        "SRS7053865",
        "SAMD00013986",
        "DRS000237",
    ]
    yield accessions


@pytest.fixture
def search_params(run_accessions):
    yield {
        "result": "read_run",
        "fields": ",".join(EXPECTED_FIELD_LIST),
        "includeAccessionType": RUN_TYPE,
        "includeAccessions": ",".join(run_accessions),
        "limit": 0,
        "format": "tsv",
    }


@pytest.fixture
def mock_get_metadata_return_data():
    """Dummy return data from get_metadata()"""
    yield {
        "ERR1160846": {"run_accession": "ERR1160846", "study_accession": "PRJEB11419"},
        "SRR13191702": {
            "run_accession": "SRR13191702",
            "study_accession": "PRJNA682076",
        },
        "SRR9984183": {"run_accession": "SRR9984183", "study_accession": "PRJNA560329"},
    }


@pytest.fixture
def test_path(tmp_path):
    yield tmp_path


def test_get_available_fields_success(mock_fields_request):
    """Test the get_available_fields method"""
    assert ENAMetadata.get_available_fields("read_run") == EXPECTED_FIELD_LIST
    mock_fields_request.assert_called_with(f"{EXPECTED_FIELDS_URL}read_run")


def test_get_available_fields_fail(mock_fields_request_error):
    """Test the get_available_fields method when an HTTP error is encountered"""
    with pytest.raises(SystemExit) as e:
        ENAMetadata.get_available_fields("read_run")


def test_get_metadata(
    mocker, run_accessions, mock_search_request, mock_get_metadata_return_data
):
    """Test get_metadata method"""
    # Given
    get_metadata_response_mock = mocker.patch.object(
        ENAMetadata, "_get_metadata_response"
    )
    get_metadata_response_mock.return_value.text = TEST_SEARCH_FIELDS
    # When
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE, 2)
    metadata_obj.get_metadata()
    # Then
    get_metadata_response_mock.assert_called_once_with(run_accessions, RUN_TYPE)
    assert metadata_obj.metadata == mock_get_metadata_return_data
    # Additionally, test caching
    metadata_obj.get_metadata()
    get_metadata_response_mock.assert_called_once()


def test_get_metadata_response(
    mocker, mock_fields_request, mock_search_request, search_params, run_accessions
):
    """Test _get_metadata_response method"""
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE)
    response = metadata_obj._get_metadata_response(run_accessions, RUN_TYPE)
    mock_search_request.assert_called_once_with(EXPECTED_SEARCH_URL, data=search_params)
    assert response.text == TEST_SEARCH_FIELDS


def test_get_metadata_response_with_retries(
    mocker,
    mock_fields_request,
    mock_search_request_error,
    search_params,
    run_accessions,
):
    """Test _get_metadata_response method for error handling and retries"""
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE, 2)
    with pytest.raises(SystemExit) as e:
        metadata_obj._get_metadata_response(run_accessions, RUN_TYPE)
    mock_search_request_error.assert_called_with(
        EXPECTED_SEARCH_URL, data=search_params
    )
    # one failed call followed by 2 failed retries = 3 total......
    assert mock_search_request_error.call_count == 3


def test_build_post_data(run_accessions):
    """Test _build_post_data method"""
    fields = EXPECTED_FIELD_LIST
    post_data = ENAMetadata._build_post_data(fields, RUN_TYPE, run_accessions)

    assert post_data["fields"] == ",".join(fields)
    assert post_data["includeAccessionType"] == RUN_TYPE
    assert post_data["includeAccessions"] == "SRR9984183,SRR13191702,ERR1160846"
    assert "query" not in post_data or not post_data["query"]


def test_build_post_data_with_mixed_accessions(sample_accessions):
    """Test _build_post_data method when using a mix of secondary and non-secondary accessions"""
    fields = EXPECTED_FIELD_LIST
    post_data = ENAMetadata._build_post_data(fields, SAMPLE_TYPE, sample_accessions)

    assert post_data["fields"] == ",".join(fields)
    assert post_data["includeAccessionType"] == SAMPLE_TYPE
    assert post_data["includeAccessions"] == "SAMD00002711,SAMN15546073,SAMD00013986"
    assert (
        post_data["query"] == 'secondary_sample_accession="SRS7053897" '
        'OR secondary_sample_accession="SRS7053865" '
        'OR secondary_sample_accession="DRS000237"'
    )


def test_parse_metadata(run_accessions):
    """Test _parse_metadata method"""
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE)
    input_data = (
        "run_accession	study_accession\nDRR028935\tPRJDB3420\nSRR9983610\tPRJNA560329"
    )
    assert metadata_obj._parse_metadata(io.StringIO(input_data)) == {
        "DRR028935": {"run_accession": "DRR028935", "study_accession": "PRJDB3420"},
        "SRR9983610": {"run_accession": "SRR9983610", "study_accession": "PRJNA560329"},
    }


def test_write_metadata_file(
    mocker, test_path, mock_fields_request, mock_get_metadata_return_data
):
    """Test write_metadata_file method"""
    # Given
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE)
    mock_get_metadata = mocker.patch.object(ENAMetadata, "get_metadata")
    metadata_obj.metadata = mock_get_metadata_return_data
    mock_get_metadata.return_value = mock_get_metadata_return_data
    # When
    metadata_obj.write_metadata_file(test_path)
    # Then
    file_path = test_path / "metadata.tsv"
    assert file_path.exists() and file_path.is_file()
    with open(file_path) as f:
        lines = f.readlines()
    assert len(lines) == 4
    assert lines[0] == "run_accession\tstudy_accession\n"
    assert lines[1] == "ERR1160846\tPRJEB11419\n"
    assert lines[2] == "SRR13191702\tPRJNA682076\n"
    assert lines[3] == "SRR9984183\tPRJNA560329\n"


def test_get_taxonomy(mock_taxonomy_request):
    """Test _get_taxonomy method"""
    result = ENAMetadata._get_taxonomy(TEST_TAXON_ID)
    assert isinstance(result, collections.OrderedDict)
    # Pick a few values to check...
    assert result["taxon"]["@scientificName"] == "Pirellula"
    assert result["taxon"]["@taxId"] == TEST_TAXON_ID
    assert result["taxon"]["@hidden"] == "false"
    assert len(result["taxon"]["lineage"]["taxon"]) == 8
    assert len(result["taxon"]["children"]["taxon"]) == 3


def test_get_taxonomy_failed(mock_taxonomy_request_error):
    """Test _get_taxonomy method failed ENA connection"""
    with pytest.raises(SystemExit) as e:
        ENAMetadata._get_taxonomy(TEST_TAXON_ID)


def test_get_scientific_name(mock_taxonomy_request):
    """Test get_scientific_name method"""
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE)
    assert metadata_obj.get_scientific_name(TEST_TAXON_ID) == "Pirellula"


def test_group_by_project(mocker, run_accessions, mock_get_metadata_return_data):
    """Test group_by_project method"""
    # Given
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE)
    mock_get_metadata = mocker.patch.object(ENAMetadata, "get_metadata")
    metadata_obj.metadata = mock_get_metadata_return_data
    mock_get_metadata.return_value = mock_get_metadata_return_data
    # When
    result = metadata_obj.group_by_project()
    # Then
    assert result == collections.defaultdict(
        list,
        {
            "PRJEB11419": [
                {"run_accession": "ERR1160846", "study_accession": "PRJEB11419"}
            ],
            "PRJNA682076": [
                {"run_accession": "SRR13191702", "study_accession": "PRJNA682076"}
            ],
            "PRJNA560329": [
                {"run_accession": "SRR9984183", "study_accession": "PRJNA560329"}
            ],
        },
    )


# TODO Add checks to to ensure that the ExcelWriter input params are correct in this test
def test_to_excel(mocker, run_accessions, test_path):
    """Test to_excel method"""
    # Given
    metadata_obj = ENAMetadata(run_accessions, RUN_TYPE)
    mocker_writer = mocker.patch.object(
        enadownloader.excel.ExcelWriter, "__init__", return_value=None
    )
    mock_writer_write = mocker.patch.object(enadownloader.excel.ExcelWriter, "write")
    input_test_metadata = [
        {
            "study_accession": "PRJEB11633",
            "secondary_study_accession": "ERP013030",
            "sample_accession": "SAMEA3643867",
            "instrument_platform": "Illumina",
            "tax_id": "63433",
            "study_title": "test study title",
            "fastq_ftp": "/random/path/file_1.fastq;/random/path/file_2.fastq",
        }
    ]
    # When
    metadata_obj.to_excel(test_path, input_test_metadata)
    # Then
    mocker_writer.assert_called_once()
    excel_path = test_path / "PRJEB11633.xls"
    mock_writer_write.assert_called_once_with(str(excel_path))
