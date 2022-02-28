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
from os.path import basename, exists, splitext
from time import sleep
from urllib.error import URLError
from pathlib import Path

import requests

INITIAL_RETRIES = 5


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


# def download_metadata(accession_number):
#     result = requests.get(
#         f"https://www.ebi.ac.uk/ena/browser/api/xml/{accession_number}"
#     )
#
#     root = xmltodict.parse(result.content.strip())
#     return json.dumps(root["ASSEMBLY_SET"])
#
#
# def download_fasta(accession_number):
#     filename = f"{accession_number}.fasta.gz"
#     if exists(filename):
#         print("File exists already, not downloading again")
#         return
#
#     url = f"https://www.ebi.ac.uk/ena/browser/api/fasta/{accession_number}?download=true&gzip=true"
#     wget(url, filename)
#
#
# def download_multi_fasta(*accessions):
#     for a in accessions:
#         p = mp.Process(target=download_fasta, args=(a,))
#         p.start()


def wget(url, filename, tries=0):
    # TODO This doesn't work
    logging.info(f"Downloading {filename}")

    try:
        with urlrequest.urlopen(url) as response, open(filename, "wb") as out_file:
            shutil.copyfileobj(response, out_file)
    except URLError as err:
        if tries <= INITIAL_RETRIES:
            sleeptime = 2 ** tries
            # TODO This doesn't work
            logging.warning(
                f"{err.errno}: {str(err)} - Download failed, retrying after {sleeptime} seconds..."
            )
            sleep(sleeptime)
            wget(url, filename, tries + 1)
        else:
            raise


def parse_file_report(response):
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


def read_response_file(response_file):
    progress_file = f"{splitext(basename(response_file))[0]}.progress.csv"
    response_parsed = {}
    with open(response_file) as fp:
        # skip header
        fp.readline()

        for line in fp:
            line = line.strip().split(",")
            obj = ENAObject(*line)
            response_parsed[obj.key] = obj

    if exists(progress_file):
        with open(progress_file) as prf:
            prf.readline()

            for line in prf:
                line = line.strip().split(",")
                obj = ENAObject(*line)
                try:
                    response_parsed[obj.key].md5_passed = line[-1]
                except KeyError:
                    logging.warning(
                        f"{obj.key} key has gone missing from {progress_file}!"
                    )

    return response_parsed


def write_response_file(response_parsed, response_file):
    with open(response_file, "w") as fp:
        fp.write(f"{ENAObject.header}\n")
        for data in response_parsed.values():
            fp.write(str(data) + "\n")


def listener(accession, queue: mp.Queue):
    response_file = f"{accession}.progress.csv"
    if not exists(response_file):
        with open(response_file, "w") as f:
            f.write(f"{ENAObject.header}\n")

    with open(response_file, "a") as f:
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


def get_files(accession):
    response_file = f"{accession}.csv"

    if exists(response_file):
        response_parsed = read_response_file(response_file)

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

        response_parsed = parse_file_report(response)
        write_response_file(response_parsed, response_file)

    return response_parsed


def download_project_fastqs(accession, threads, output_dir):
    response = get_files(accession)

    number_of_threads = int(threads)
    manager = mp.Manager()
    queue = manager.Queue()
    with mp.Pool(processes=number_of_threads) as pool:
        pool.apply_async(listener, (accession, queue))

        res = pool.starmap_async(
            download_fastqs,
            [
                (item, queue, output_dir)
                for item in response.values()
                if not item.md5_passed
            ],
        )
        res.get()

        queue.put("kill")
        queue.join()


def md5_check(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def download_fastqs(ena: ENAObject, q, output_dir: Path):
    url = "ftp://" + ena.ftp
    outfile = output_dir / basename(ena.ftp)
    wget(url, outfile)
    md5_f = md5_check(outfile)

    ena.md5_passed = md5_f == ena.md5
    q.put(ena)


def arg_parser():
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
        type=validate_dir,
        help="directory in which to save downloaded files",
    )
    parser.add_argument(
        "-t",
        "--threads",
        default=1,
        type=validate_threads,
        help="Number of threads to use for download",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Use the option multiple times to increase output verbosity"
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


def validate_dir(path: str):
    path = Path(path)
    if path.is_dir():
        return path
    else:
        raise argparse.ArgumentTypeError(
            f"invalid path to dir (path does not exist or is not a directory): {path}"
        )


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
    args = arg_parser()
    # Set up logging
    fh = logging.FileHandler(f"{splitext(__file__)[0]}.log", mode="w")
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    # noinspection PyArgumentList
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[fh, sh],
    )

    download_project_fastqs(
        accession=args.project, threads=args.threads, output_dir=args.output_dir
    )
