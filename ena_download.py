#!/usr/bin/env python3
"""
Robust tool to download fastq.gz files and metadata from ENA
"""

import argparse
import hashlib
import logging
import multiprocessing as mp
import os
import shutil
import urllib.request as urlrequest
from distutils.util import strtobool
from os.path import basename, exists, join, splitext
from pathlib import Path
from time import sleep
from urllib.error import URLError

import requests


class ENAObject:
    header = "run_accession,fastq_ftp,fastq_md5,md5_passed"

    def __init__(
        self, run_accession: str, ftp: str, md5: str, md5_passed: bool = False
    ):
        self.run_accession = run_accession
        self.ftp = ftp
        self.md5 = md5
        self.md5_passed = md5_passed
        self.key = splitext(basename(ftp))[0]

    @property
    def md5_passed(self):
        pass

    @md5_passed.setter
    def md5_passed(self, value):
        self._md5_passed = (
            bool(strtobool(value)) if not isinstance(value, bool) else value
        )

    @md5_passed.getter
    def md5_passed(self):
        return self._md5_passed

    def __str__(self):
        return ",".join([self.run_accession, self.ftp, self.md5, str(self.md5_passed)])

    def __repr__(self):
        return f"{self.__class__.__name__}: {str(self)}"


class ENADownloader:
    def __init__(
        self, accession: str, threads: int, output_dir: Path, retries: int = 5
    ):
        self.accession = accession
        self.threads = threads
        self.output_dir = output_dir
        self.retries = retries

        self.response_file = join(output_dir, f".{accession}.csv")
        self.progress_file = join(output_dir, f".{accession}.progress.csv")

    def wget(self, url, filename, tries=0):
        print(f"Downloading {filename}")

        try:
            with urlrequest.urlopen(url) as response, open(filename, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
        except URLError as err:
            if tries <= self.retries:
                sleeptime = 2**tries
                print(
                    f"Download failed, retrying after {sleeptime} seconds... Reason: {err.reason}"
                )
                sleep(sleeptime)
                self.wget(url, filename, tries + 1)
            else:
                raise

    @staticmethod
    def parse_file_report(response: requests.Response):
        response_files = {}
        keys = None

        for line in response.text.split("\n"):
            if keys is None:
                keys = line
                continue
            try:
                run_accession, fastq_ftp, fastq_md5 = line.strip().split()
            except ValueError:
                continue

            file_links = fastq_ftp.split(";")
            md5s = fastq_md5.split(";")

            assert len(file_links) == len(md5s)

            for f, m in zip(file_links, md5s):
                obj = ENAObject(run_accession, f, m)
                response_files[obj.key] = obj

        logging.info("fastq files parsed")
        return response_files

    def load_response(self):
        response_parsed = {}
        with open(self.response_file) as fp:
            # skip header
            fp.readline()

            for line in fp:
                line = line.strip().split(",")
                obj = ENAObject(*line)
                response_parsed[obj.key] = obj

        if exists(self.progress_file):
            with open(self.progress_file) as prf:
                # skip header
                prf.readline()

                for line in prf:
                    line = line.strip().split(",")
                    obj = ENAObject(*line)
                    response_parsed[obj.key].md5_passed = line[-1]

        return response_parsed

    def write_response_file(self, response_parsed):
        with open(self.response_file, "w") as fp:
            fp.write(f"{ENAObject.header}\n")
            for data in response_parsed.values():
                fp.write(str(data) + "\n")

    def listener(self, queue: mp.Queue):
        if not exists(self.progress_file):
            with open(self.progress_file, "w") as f:
                f.write(f"{ENAObject.header}\n")

        with open(self.progress_file, "a") as f:
            while True:
                m = queue.get()
                assert isinstance(
                    m, (str, ENAObject)
                ), f"Unrecognised type sent in queue: {m} of type {m.__class__.__name__}"
                if m == "kill":
                    queue.task_done()
                    break

                else:
                    f.write(str(m) + "\n")
                    f.flush()
                    queue.task_done()

    def get_ftp_paths(self, accession):
        if exists(self.response_file):
            response_parsed = self.load_response()

        else:
            url = (
                f"https://www.ebi.ac.uk/ena/portal/api/filereport?"
                f"accession={accession}&result=read_run&fields=fastq_ftp,fastq_md5&limit=0"
            )
            rp = 400
            while rp >= 300:
                response = requests.get(url)
                rp = response.status_code
                if rp >= 300:
                    logging.warning(response.text)
                    sleep(5)

            response_parsed = self.parse_file_report(response)
            self.write_response_file(response_parsed)

        return response_parsed

    @staticmethod
    def md5_check(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_fastqs(self, ena: ENAObject, queue: mp.Queue, output_dir: Path):
        url = "ftp://" + ena.ftp
        outfile = output_dir / basename(ena.ftp)
        self.wget(url, outfile)
        md5_f = self.md5_check(outfile)

        ena.md5_passed = md5_f == ena.md5
        queue.put(ena)

    def download_project_fastqs(self):
        response = self.get_ftp_paths(self.accession)

        number_of_threads = self.threads
        manager = mp.Manager()
        queue = manager.Queue()
        with mp.Pool(processes=number_of_threads) as pool:
            pool.apply_async(self.listener, (queue,))

            res = pool.starmap_async(
                self.download_fastqs,
                [
                    (item, queue, args.output_dir)
                    for item in response.values()
                    if not item.md5_passed
                ],
            )
            res.get()

            queue.put("kill")
            queue.join()


class Parser:
    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "-p",
            "--project",
            required=True,
            help="ENA project identifier",
        )
        parser.add_argument(
            "-o",
            "--output_dir",
            default=os.getcwd(),
            type=cls.validate_dir,
            help="directory in which to save downloaded files",
        )
        parser.add_argument(
            "-t",
            "--threads",
            default=1,
            type=cls.validate_threads,
            help="Number of threads to use for download",
        )
        parser.add_argument(
            "-v",
            "--verbosity",
            action="count",
            default=0,
            help="Use the option multiple times to increase output verbosity",
        )
        args = parser.parse_args()

        # Set log_level arg
        if args.verbosity >= 2:
            args.log_level = logging.DEBUG
        elif args.verbosity >= 1:
            args.log_level = logging.INFO
        else:
            args.log_level = logging.WARN

        return args

    @staticmethod
    def validate_dir(path: str):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as err:
            raise argparse.ArgumentTypeError(f"cannot create dir: {str(err)}")
        return Path(path).resolve()

    @staticmethod
    def validate_threads(threads: str):
        try:
            threads = int(threads)
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid int value: {threads!r}")
        if 1 < threads <= 100:
            return threads
        else:
            raise argparse.ArgumentTypeError(
                f"invalid int value (must be between 2 and 100): {threads!r}"
            )


if __name__ == "__main__":
    args = Parser.arg_parser()
    this_file = join(args.output_dir, basename(splitext(__file__)[0]))

    # Set up logging
    fh = logging.FileHandler(f"{this_file}.log", mode="w")
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()

    # noinspection PyArgumentList
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[fh, sh],
    )

    enadownloader = ENADownloader(
        accession=args.project, threads=args.threads, output_dir=args.output_dir
    )
    enadownloader.download_project_fastqs()
