# noinspection PyUnresolvedReferences
import os
from pathlib import Path

from enadownloader import main


def test_main(tmp_path):
    input_file = tmp_path / "input.txt"
    input_file.write_text("SRR9984183\n")

    output_folder = tmp_path / "results"

    main(args=["-i", str(input_file), "-t", "run", "-o", str(output_folder), "-cmdev"])

    assert output_folder.exists() and output_folder.is_dir()
    metadata_file, project_folder = list(output_folder.iterdir())
    assert Path(metadata_file).name == "metadata.tsv"

    files = os.listdir(project_folder)
    assert files == [".progress.csv", "PRJNA560329.xls", "SRR9984183.fastq.gz"]
