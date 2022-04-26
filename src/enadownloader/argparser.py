import argparse
import logging
import os
from pathlib import Path


class Parser:
    @classmethod
    def arg_parser(cls, vargs=None):
        parser = argparse.ArgumentParser(
            prog="enadownloader",
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "-i",
            "--input",
            required=True,
            type=cls.validate_input,
            help="Path to file containing ENA accessions",
        )
        parser.add_argument(
            "-t",
            "--type",
            required=True,
            choices=["run", "sample", "study"],
            help="Type of ENA accessions",
        )
        parser.add_argument(
            "-o",
            "--output_dir",
            default=os.getcwd(),
            type=cls.validate_dir,
            help="Directory in which to save downloaded files",
        )
        parser.add_argument(
            "-c",
            "--create-study-folders",
            action="store_true",
            help="Organise the downloaded files by study",
        )
        parser.add_argument(
            "-r",
            "--retries",
            default=5,
            type=cls.validate_retries,
            help="Amount to retry each fastq file upon download interruption",
        )
        parser.add_argument(
            "-v",
            "--verbosity",
            action="count",
            default=1,
            help="Use the option multiple times to increase output verbosity",
        )
        parser.add_argument(
            "-m",
            "--write-metadata",
            action="store_true",
            help="Output a metadata tsv for the given ENA accessions",
        )
        parser.add_argument(
            "-d",
            "--download-files",
            action="store_true",
            help="Download fastq files for the given ENA accessions",
        )
        parser.add_argument(
            "-e",
            "--write-excel",
            action="store_true",
            help="Create an External Import-compatible Excel file for legacy pipelines for the given ENA accessions, stored by project",
        )

        args = parser.parse_args(vargs)

        # Set log_level arg
        if args.verbosity >= 2:
            args.log_level = logging.DEBUG
        elif args.verbosity >= 1:
            args.log_level = logging.INFO
        else:
            args.log_level = logging.WARN

        return args

    @staticmethod
    def validate_input(filepath: str):
        filepath = Path(filepath)
        if not filepath.is_file():
            raise argparse.ArgumentTypeError(
                f"input file of accessions does not exist or is not a file: {filepath}"
            )
        return filepath.resolve()

    @staticmethod
    def validate_dir(path: str):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as err:
            raise argparse.ArgumentTypeError(f"cannot create dir: {str(err)}")
        return Path(path).resolve()

    @staticmethod
    def validate_retries(retries: str):
        try:
            retries = int(retries)
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid int value: {retries!r}")
        if retries >= 0:
            return retries
        else:
            raise argparse.ArgumentTypeError(
                f"invalid int value (must be nonnegative): {retries!r}"
            )
