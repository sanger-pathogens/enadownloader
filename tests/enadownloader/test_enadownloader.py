from urllib.error import URLError

import asyncio
import pytest
from pytest_mock import MockerFixture

from enadownloader.enadownloader import ENADownloader
from enadownloader.utils import ENAFTPContainer


def test_get_fastq_ftp_paths_without_previously_md5passed_downloads(
    fastq_downloader,
    mocker: MockerFixture,
    ENAFastqFTPContainers,
):
    # Mock method
    mocker.patch.object(fastq_downloader, "load_progress", return_value=set())

    result = fastq_downloader.get_ftp_paths("fastq")
    assert result == {container.key: container for container in ENAFastqFTPContainers}


def test_get_submitted_ftp_paths_without_previously_md5passed_downloads(
    submitted_downloader,
    mocker: MockerFixture,
    ENASubmittedFTPContainers,
):
    # Mock method
    mocker.patch.object(submitted_downloader, "load_progress", return_value=set())

    result = submitted_downloader.get_ftp_paths("submitted")
    assert result == {
        container.key: container for container in ENASubmittedFTPContainers
    }


def test_get_fastq_ftp_paths_with_previously_md5passed_downloads(
    fastq_downloader,
    mocker: MockerFixture,
    ENAFastqFTPContainers,
):
    # Mock method
    mocker.patch.object(
        fastq_downloader, "load_progress", return_value=set(ENAFastqFTPContainers[:1])
    )

    result = fastq_downloader.get_ftp_paths("fastq")
    assert result == {
        container.key: container for container in ENAFastqFTPContainers[1:]
    }


def test_get_submitted_ftp_paths_with_previously_md5passed_downloads(
    submitted_downloader,
    mocker: MockerFixture,
    ENASubmittedFTPContainers,
):
    # Mock method
    mocker.patch.object(
        submitted_downloader,
        "load_progress",
        return_value=set(ENASubmittedFTPContainers[:1]),
    )

    result = submitted_downloader.get_ftp_paths("submitted")
    assert result == {
        container.key: container for container in ENASubmittedFTPContainers[1:]
    }


def test_get_ftp_paths_with_invalid_metadata(
    fastq_downloader, mocker: MockerFixture, ENAFastqFTPContainers, caplog
):
    fastq_downloader.metadata_obj.metadata = {
        "sample": {
            "fastq_ftp": "",
            "fastq_md5": "akekekdie",
            "run_accession": "SRR1235",
            "study_accession": "PRJN123545",
        }
    }

    # Mock method
    mocker.patch.object(fastq_downloader, "load_progress", return_value=set())

    result = fastq_downloader.get_ftp_paths("fastq")
    assert "Found invalid metadata for run accession SRR1235" in caplog.text
    assert result == {}


def test_get_ftp_paths_with_missing_metadata(
    fastq_downloader, mocker: MockerFixture, ENAFastqFTPContainers, caplog
):
    fastq_downloader.metadata_obj.metadata = {"sample": {}}

    # Mock method
    mocker.patch.object(fastq_downloader, "load_progress", return_value=set())

    with pytest.raises(ValueError) as e:
        fastq_downloader.get_ftp_paths("fastq")

        assert "Missing metadata for run accession SRR1235" in caplog.text


def test_get_ftp_paths_with_multiple_ftps_but_not_md5(
    fastq_downloader, mocker: MockerFixture, ENAFastqFTPContainers, caplog
):
    fastq_downloader.metadata_obj.metadata = {
        "sample": {
            "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR250/001/ERR25042885/ERR25042885_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR250/001/ERR25042885/ERR25042885_2.fastq.gz",
            "fastq_md5": "akekekdie",
            "run_accession": "SRR1235",
            "study_accession": "PRJN123545",
        }
    }

    # Mock method
    mocker.patch.object(fastq_downloader, "load_progress", return_value=set())

    result = fastq_downloader.get_ftp_paths("fastq")
    assert "Found invalid metadata for run accession SRR1235" in caplog.text
    assert result == {}


def test_wget(fastq_downloader, fastq_mock_urlopen, output_path):
    test_file = output_path / "test123.fastq.gz"
    assert not test_file.exists()

    fastq_downloader.wget("ftp://iamafastqurl", test_file)

    assert test_file.exists()
    assert test_file.read_text() == "I am a fastq file"


def test_wget_retries(fastq_downloader, fastq_mock_urlopen, output_path, caplog):
    fastq_mock_urlopen.side_effect = URLError("I fail")

    test_file = output_path / "test123.fastq.gz"
    assert not test_file.exists()

    fastq_downloader.retries = 0
    fastq_downloader.wget("ftp://iamafastqurl", test_file)

    assert not test_file.exists()

    assert "Download of test123.fastq.gz failed. Reason:" in caplog.text
    assert "failed entirely!" in caplog.text


def test_load_progress_when_file_doesnt_exist(fastq_downloader):
    assert not fastq_downloader.progress_file.exists()
    result = fastq_downloader.load_progress()
    assert len(result) == 0


def test_load_progress_when_file_is_empty(fastq_downloader):
    fastq_downloader.progress_file.write_text(f"{ENAFTPContainer.header}\n")
    assert fastq_downloader.progress_file.exists()
    result = fastq_downloader.load_progress()
    assert len(result) == 0


def test_load_progress_when_file_is_downloaded(fastq_downloader):
    fastq_downloader.progress_file.write_text(
        f"{ENAFTPContainer.header}\n"
        "SRR123456,SRR123456,/path/to/fastq.gz,d98430af7e7469da9e70385b9c681681,True\n"
    )
    assert fastq_downloader.progress_file.exists()
    result = fastq_downloader.load_progress()
    assert result == {
        ENAFTPContainer(
            "SRR123456",
            "SRR123456",
            "/path/to/fastq.gz",
            "d98430af7e7469da9e70385b9c681681",
            True,
        )
    }


def test_load_progress_when_file_is_downloaded_but_not_md5passed(fastq_downloader):
    fastq_downloader.progress_file.write_text(
        f"{ENAFTPContainer.header}\n"
        "SRR123456,SRR123456,/path/to/fastq.gz,d98430af7e7469da9e70385b9c681681,False\n"
    )
    assert fastq_downloader.progress_file.exists()
    result = fastq_downloader.load_progress()
    assert len(result) == 0


def test_load_progress_without_cache_returns_empty_set(fastq_downloader):
    fastq_downloader.cache = False
    result = fastq_downloader.load_progress()
    assert len(result) == 0


def test_write_progress_file_creates_file_when_it_doesnt_exist_yet(fastq_downloader):
    assert not fastq_downloader.progress_file.exists()
    fastq_downloader.write_progress_file()
    assert fastq_downloader.progress_file.exists()
    assert fastq_downloader.progress_file.read_text() == ENAFTPContainer.header + "\n"


def test_write_progress_file_writes_message_to_existing_file(fastq_downloader):
    assert not fastq_downloader.progress_file.exists()

    fastq_downloader.write_progress_file()
    assert fastq_downloader.progress_file.exists()

    assert fastq_downloader.progress_file.read_text() == ENAFTPContainer.header + "\n"

    fastq_downloader.write_progress_file("this,is,a,test")
    assert (
        fastq_downloader.progress_file.read_text()
        == f"{ENAFTPContainer.header}\nthis,is,a,test\n"
    )


def test_md5_check(output_path):
    test_file = output_path / "file.txt"
    test_file.write_text("This contains data\n")
    calc_md5 = ENADownloader.md5_check(test_file)
    assert calc_md5 == "e46c7039ed61c809401c378b1d4f604a"


def test_md5_check_throws_exception_when_file_doesnt_exist():
    pytest.raises(FileNotFoundError, ENADownloader.md5_check, "nonexistent_file.txt")


def test_download_from_ftp(
    fastq_downloader, fastq_mock_urlopen, output_path, ENAFastqFTPContainers
):
    container = ENAFastqFTPContainers[0]

    fastq_downloader.download_from_ftp(container)

    result_file = fastq_downloader.output_dir / "SRR25042885.fastq.gz"
    assert result_file.exists()
    assert result_file.read_text() == "I am a fastq file"
    assert fastq_downloader.progress_file.exists()
    assert (
        fastq_downloader.progress_file.read_text().strip()
        == "run_accession,study_accession,ftp,md5,md5_passed\n"
        "SRR25042885,"
        "PRJNA123456,"
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR250/001/SRR25042885/SRR25042885.fastq.gz,"
        "5fc34f3bd5a7f2696902d661d8b21981,"
        "False"
    )


def test_download_all_fastqs(
    fastq_downloader, mocker, fastq_mock_urlopen, output_path, ENAFastqFTPContainers
):
    ftp_paths = {container.key: container for container in ENAFastqFTPContainers[:1]}
    mocker.patch.object(fastq_downloader, "get_ftp_paths", return_value=ftp_paths)

    asyncio.run(fastq_downloader.download_all_files("fastq"))

    result_file = fastq_downloader.output_dir / "SRR25042885.fastq.gz"
    assert result_file.exists()
    assert result_file.read_text() == "I am a fastq file"

    assert fastq_downloader.progress_file.exists()
    assert (
        fastq_downloader.progress_file.read_text().strip()
        == "run_accession,study_accession,ftp,md5,md5_passed\n"
        "SRR25042885,"
        "PRJNA123456,"
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR250/001/SRR25042885/SRR25042885.fastq.gz,"
        "5fc34f3bd5a7f2696902d661d8b21981,"
        "False"
    )


def test_download_all_submitted(
    submitted_downloader,
    mocker,
    submitted_mock_urlopen,
    output_path,
    ENASubmittedFTPContainers,
):
    ftp_paths = {
        container.key: container for container in ENASubmittedFTPContainers[:1]
    }
    mocker.patch.object(submitted_downloader, "get_ftp_paths", return_value=ftp_paths)

    asyncio.run(submitted_downloader.download_all_files("submitted"))

    result_file = submitted_downloader.output_dir / "A29254.bam"
    assert result_file.exists()
    assert result_file.read_text() == "I am a submitted file"

    assert submitted_downloader.progress_file.exists()
    assert (
        submitted_downloader.progress_file.read_text().strip()
        == "run_accession,study_accession,ftp,md5,md5_passed\n"
        "ERR4303146,"
        "PRJEB39136,"
        "ftp.sra.ebi.ac.uk/vol1/run/ERR430/ERR4303146/A29254.bam,"
        "e35f1046ef3d4addd3e5407aa5963fb1,"
        "False"
    )


def test_download_all_fastqs_when_all_downloads_fail(
    fastq_downloader, mocker, fastq_mock_urlopen, output_path, ENAFastqFTPContainers
):
    ftp_paths = {container.key: container for container in ENAFastqFTPContainers[:1]}
    mocker.patch.object(fastq_downloader, "get_ftp_paths", return_value=ftp_paths)

    # Mock wget so downloads always appear to fail
    mocker.patch.object(fastq_downloader, "wget", return_value=False)

    with pytest.raises(fastq_downloader.NoSuccessfulDownloads):
        asyncio.run(fastq_downloader.download_all_files("fastq"))

    result_file = fastq_downloader.output_dir / "SRR25042885.fastq.gz"
    assert not result_file.exists()

    # The progress file should be empty except for the header
    assert fastq_downloader.progress_file.exists()
    assert (
        fastq_downloader.progress_file.read_text().strip()
        == "run_accession,study_accession,ftp,md5,md5_passed"
    )
