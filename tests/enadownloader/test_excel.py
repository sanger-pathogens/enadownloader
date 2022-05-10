import datetime
import pytest
import xlrd

from enadownloader.excel import Data, ExcelWriter, FileHeader, Workbook

""" Unit tests for the excel module """


@pytest.fixture
def excel_path(tmp_path):
    yield tmp_path / "test.xls"


@pytest.fixture
def workbook():
    yield Workbook()


@pytest.fixture
def worksheet(workbook):
    yield workbook.add_sheet("Sheet1")


@pytest.fixture
def fileheader():
    header = FileHeader(
        supplier_name="Test User",
        supplier_organisation="Sanger",
        contact_name="Test User",
        sequencing_technology="Illumina",
        study_name="Test_Excel",
        size_in_gb=1.5,
        date_to_keep_until="01/12/2024",
        study_accession_number="12345",
    )

    yield header


@pytest.fixture
def data():
    yield Data(
        filename="file_1.fastq.gz",
        sample_name="Test Sample",
        taxon=123456,
        mate_file="file_2.fastq.gz",
        sample_accession="SAM12345",
        library="lib12345",
        fragment="2345",
        read_count="1234",
        base_count="12345",
        comments="None",
    )


@pytest.fixture
def excelwriter(fileheader, data):
    yield ExcelWriter(fileheader, [data])


def test_excelwriter(excel_path, excelwriter):
    assert not excel_path.exists()

    excelwriter.write(excel_path)

    assert excel_path.exists() and excel_path.is_file()

    test_book = xlrd.open_workbook(excel_path)
    test_sheet = test_book.sheet_by_index(0)

    # Test file header
    assert test_sheet.cell_value(0, 0) == "Supplier Name"
    assert test_sheet.cell_value(0, 1) == "Test User"
    assert test_sheet.cell_value(1, 0) == "Supplier Organisation"
    assert test_sheet.cell_value(1, 1) == "Sanger"
    assert test_sheet.cell_value(2, 0) == "Sanger Contact Name"
    assert test_sheet.cell_value(2, 1) == "Test User"
    assert test_sheet.cell_value(3, 0) == "Sequencing Technology"
    assert test_sheet.cell_value(3, 1) == "Illumina"
    assert test_sheet.cell_value(4, 0) == "Study Name"
    assert test_sheet.cell_value(4, 1) == "Test_Excel"
    assert test_sheet.cell_value(5, 0) == "Study Accession number"
    assert test_sheet.cell_value(5, 1) == "12345"
    assert test_sheet.cell_value(6, 0) == "Total size of files in GBytes"
    assert test_sheet.cell_value(6, 1) == 1.5
    assert type(test_sheet.cell_value(6, 1)) == float
    assert test_sheet.cell_value(7, 0) == "Data to be kept until"
    # excel datetime object converts to float, need to convert back to datetime for test
    excel_date_object = test_sheet.cell_value(7, 1)
    seconds = (excel_date_object - 25569) * 86400.0  # 25569 is an Excel defined offset
    date = datetime.datetime.utcfromtimestamp(seconds)
    assert date == datetime.datetime(2024, 12, 1, 0, 0)

    # Test empty newline
    assert test_sheet.cell_value(8, 0) == ""

    # Test data header
    assert test_sheet.cell_value(9, 0) == "Filename"
    assert test_sheet.cell_value(9, 1) == "Mate File"
    assert test_sheet.cell_value(9, 2) == "Sample Name"
    assert test_sheet.cell_value(9, 3) == "Sample Accession number"
    assert test_sheet.cell_value(9, 4) == "Taxon ID"
    assert test_sheet.cell_value(9, 5) == "Library Name"
    assert test_sheet.cell_value(9, 6) == "Fragment Size"
    assert test_sheet.cell_value(9, 7) == "Read Count"
    assert test_sheet.cell_value(9, 8) == "Base Count"
    assert test_sheet.cell_value(9, 9) == "Comments"

    # Test data values
    assert test_sheet.cell_value(10, 0) == "file_1.fastq.gz"
    assert test_sheet.cell_value(10, 1) == "file_2.fastq.gz"
    assert test_sheet.cell_value(10, 2) == "Test Sample"
    assert test_sheet.cell_value(10, 3) == "SAM12345"
    assert test_sheet.cell_value(10, 4) == 123456
    assert test_sheet.cell_value(10, 5) == "lib12345"
    assert test_sheet.cell_value(10, 6) == "2345"
    assert test_sheet.cell_value(10, 7) == "1234"
    assert test_sheet.cell_value(10, 8) == "12345"
    assert test_sheet.cell_value(10, 9) == "None"
