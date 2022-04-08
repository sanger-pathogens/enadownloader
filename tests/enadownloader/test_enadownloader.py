from urllib.error import URLError

import pytest
from pytest_mock import MockerFixture

from enadownloader.enadownloader import ENADownloader
from enadownloader.utils import ENAFTPContainer


def test_get_ftp_paths_without_previously_md5passed_downloads(
    downloader,
    mocker: MockerFixture,
    ENAFTPContainers,
):
    # Mock method
    mocker.patch.object(downloader, "load_progress", return_value=set())

    result = downloader.get_ftp_paths()
    assert result == {container.key: container for container in ENAFTPContainers}


def test_get_ftp_paths_with_previously_md5passed_downloads(
    downloader,
    mocker: MockerFixture,
    ENAFTPContainers,
):
    # Mock method
    mocker.patch.object(
        downloader, "load_progress", return_value=set(ENAFTPContainers[:1])
    )

    result = downloader.get_ftp_paths()
    assert result == {container.key: container for container in ENAFTPContainers[1:]}


def test_get_ftp_paths_with_invalid_metadata(
    downloader, mocker: MockerFixture, ENAFTPContainers, caplog
):
    downloader.metadata_obj.metadata = {
        "sample": {
            "fastq_ftp": "",
            "fastq_md5": "akekekdie",
            "run_accession": "SRR1235",
            "study_accession": "PRJN123545",
        }
    }

    # Mock method
    mocker.patch.object(downloader, "load_progress", return_value=set())

    result = downloader.get_ftp_paths()
    assert "Found invalid metadata for run accession SRR1235" in caplog.text
    assert result == {}


def test_get_ftp_paths_with_missing_metadata(
    downloader, mocker: MockerFixture, ENAFTPContainers, caplog
):
    downloader.metadata_obj.metadata = {"sample": {}}

    # Mock method
    mocker.patch.object(downloader, "load_progress", return_value=set())

    e = pytest.raises(ValueError, downloader.get_ftp_paths)
    assert "Missing field in given fields" in str(e)


def test_get_ftp_paths_with_multiple_ftps_but_not_md5(
    downloader, mocker: MockerFixture, ENAFTPContainers, caplog
):
    downloader.metadata_obj.metadata = {
        "sample": {
            "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR250/001/ERR25042885/ERR25042885_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR250/001/ERR25042885/ERR25042885_2.fastq.gz",
            "fastq_md5": "akekekdie",
            "run_accession": "SRR1235",
            "study_accession": "PRJN123545",
        }
    }

    # Mock method
    mocker.patch.object(downloader, "load_progress", return_value=set())

    result = downloader.get_ftp_paths()
    assert "Found invalid metadata for run accession SRR1235" in caplog.text
    assert result == {}


def test_wget(downloader, mock_urlopen, output_path):
    test_file = output_path / "test123.fastq.gz"
    assert not test_file.exists()

    downloader.wget("ftp://iamafastqurl", test_file)

    assert test_file.exists()
    assert test_file.read_text() == "I am a fastq file"


def test_wget_retries(downloader, mock_urlopen, output_path, caplog):
    mock_urlopen.side_effect = URLError("I fail")

    test_file = output_path / "test123.fastq.gz"
    assert not test_file.exists()

    downloader.retries = 0
    downloader.wget("ftp://iamafastqurl", test_file)

    assert not test_file.exists()

    assert "Download of test123.fastq.gz failed. Reason:" in caplog.text
    assert "failed entirely!" in caplog.text


def test_load_progress_when_file_doesnt_exist(downloader):
    assert not downloader.progress_file.exists()
    result = downloader.load_progress()
    assert len(result) == 0


def test_load_progress_when_file_is_empty(downloader):
    downloader.progress_file.write_text(f"{ENAFTPContainer.header}\n")
    assert downloader.progress_file.exists()
    result = downloader.load_progress()
    assert len(result) == 0


def test_load_progress_when_file_is_downloaded(downloader):
    downloader.progress_file.write_text(
        f"{ENAFTPContainer.header}\n"
        "SRR123456,SRR123456,/path/to/fastq.gz,d98430af7e7469da9e70385b9c681681,True\n"
    )
    assert downloader.progress_file.exists()
    result = downloader.load_progress()
    assert result == {
        ENAFTPContainer(
            "SRR123456",
            "SRR123456",
            "/path/to/fastq.gz",
            "d98430af7e7469da9e70385b9c681681",
            True,
        )
    }


def test_load_progress_when_file_is_downloaded_but_not_md5passed(downloader):
    downloader.progress_file.write_text(
        f"{ENAFTPContainer.header}\n"
        "SRR123456,SRR123456,/path/to/fastq.gz,d98430af7e7469da9e70385b9c681681,False\n"
    )
    assert downloader.progress_file.exists()
    result = downloader.load_progress()
    assert len(result) == 0


def test_write_progress_file_creates_file_when_it_doesnt_exist_yet(downloader):
    assert not downloader.progress_file.exists()
    downloader.write_progress_file()
    assert downloader.progress_file.exists()
    assert downloader.progress_file.read_text() == ENAFTPContainer.header + "\n"


def test_write_progress_file_writes_message_to_existing_file(downloader):
    assert not downloader.progress_file.exists()

    downloader.write_progress_file()
    assert downloader.progress_file.exists()

    assert downloader.progress_file.read_text() == ENAFTPContainer.header + "\n"

    downloader.write_progress_file("this,is,a,test")
    assert (
        downloader.progress_file.read_text()
        == f"{ENAFTPContainer.header}\nthis,is,a,test\n"
    )


def test_md5_check(output_path):
    test_file = output_path / "file.txt"
    test_file.write_text("This contains data\n")
    calc_md5 = ENADownloader.md5_check(test_file)
    assert calc_md5 == "e46c7039ed61c809401c378b1d4f604a"


def test_md5_check_throws_exception_when_file_doesnt_exist():
    pytest.raises(FileNotFoundError, ENADownloader.md5_check, "nonexistent_file.txt")


def test_download_from_ftp(downloader, mock_urlopen, output_path, ENAFTPContainers):
    container = ENAFTPContainers[0]

    downloader.download_from_ftp(container)

    result_file = downloader.output_dir / "SRR25042885.fastq.gz"
    assert result_file.exists()
    assert result_file.read_text() == "I am a fastq file"
    assert downloader.progress_file.exists()
    assert (
        downloader.progress_file.read_text().strip()
        == "run_accession,study_accession,fastq_ftp,fastq_md5,md5_passed\n"
        "SRR25042885,"
        "PRJNA123456,"
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR250/001/SRR25042885/SRR25042885.fastq.gz,"
        "5fc34f3bd5a7f2696902d661d8b21981,"
        "False"
    )


def test_download_all_fastqs(
    downloader, mocker, mock_urlopen, output_path, ENAFTPContainers
):
    import asyncio

    ftp_paths = {container.key: container for container in ENAFTPContainers[:1]}
    mocker.patch.object(downloader, "get_ftp_paths", return_value=ftp_paths)

    asyncio.run(downloader.download_all_fastqs())

    result_file = downloader.output_dir / "SRR25042885.fastq.gz"
    assert result_file.exists()
    assert result_file.read_text() == "I am a fastq file"

    assert downloader.progress_file.exists()
    assert (
        downloader.progress_file.read_text().strip()
        == "run_accession,study_accession,fastq_ftp,fastq_md5,md5_passed\n"
        "SRR25042885,"
        "PRJNA123456,"
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR250/001/SRR25042885/SRR25042885.fastq.gz,"
        "5fc34f3bd5a7f2696902d661d8b21981,"
        "False"
    )
