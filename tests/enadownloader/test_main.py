# noinspection PyUnresolvedReferences
import os

from enadownloader import main


def test_main_with_run_accession(tmp_path):
    _test_main(
        tmp_path,
        accessions=["SRR9984183"],
        accession_type="run",
        expected={
            "PRJNA560329": [
                ".progress.csv",
                "PRJNA560329.xls",
                "SRR9984183.fastq.gz",
            ]
        },
    )


def test_main_with_sample_accessions(tmp_path):
    _test_main(
        tmp_path,
        accessions=["SAMD00001129", "DRS007307"],
        accession_type="sample",
        expected={
            "PRJDB1817": [
                ".progress.csv",
                "PRJDB1817.xls",
                "DRR005312.fastq.gz",
            ],
            "PRJDB2727": [
                ".progress.csv",
                "PRJDB2727.xls",
                "DRR008199.fastq.gz",
            ],
        },
    )


def test_main_with_study_accessions(tmp_path):
    _test_main(
        tmp_path,
        accessions=["PRJDB13556", "DRP008715"],
        accession_type="study",
        expected={
            "PRJDB13556": [
                ".progress.csv",
                "PRJDB13556.xls",
                "DRR377379.fastq.gz",
                "DRR377381.fastq.gz",
                "DRR377435.fastq.gz",
                "DRR377502.fastq.gz",
                "DRR377503.fastq.gz",
            ],
            "PRJDB13464": [
                ".progress.csv",
                "PRJDB13464.xls",
                "DRR376594.fastq.gz",
            ],
        },
    )


def _test_main(tmp_path, accessions, accession_type, expected):
    input_file = tmp_path / "input.txt"
    input_file.write_text("\n".join(accessions) + "\n")

    output_folder = tmp_path / "results"

    main(
        args=[
            "-i",
            str(input_file),
            "-t",
            accession_type,
            "-o",
            str(output_folder),
            "-cmdev",
        ]
    )

    assert output_folder.exists() and output_folder.is_dir()
    contents = sorted(output_folder.iterdir(), reverse=True)
    assert contents[0].name == "metadata.tsv"

    files = {
        folder.name: [file.name for file in sorted(folder.iterdir())]
        for folder in contents[1:]
    }
    expected = {folder_name: sorted(expected[folder_name]) for folder_name in expected}
    assert files == expected
