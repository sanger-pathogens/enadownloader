# enadownloader

A robust tool for downloading fastq.gz and metadata from ENA.

The tool has three main functionalities.
Given a file of project/study, sample or run accessions, it can:
1) Download read data in fastq.gz format from the ENA FTP for multiple, associated runs concurrently.
2) Download metadata for associated runs in tsv format
3) Construct excel files that can be fed into the [external_import.py tool](https://github.com/sanger-pathogens/external-import/).

## Usage
```
usage: enadownloader [-h] -i INPUT -t {run,sample,study} [-o OUTPUT_DIR] [-c] [-r RETRIES] [-v] [-m] [-d] [-e]

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Path to file containing ENA accessions (default: None)
  -t {run,sample,study}, --type {run,sample,study}
                        Type of ENA accessions (default: None)
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                        Directory in which to save downloaded files (default: /opt)
  -c, --create-study-folders
                        Organise the downloaded files by study (default: False)
  -r RETRIES, --retries RETRIES
                        Amount to retry each fastq file upon download interruption (default: 5)
  -v, --verbosity       Use the option multiple times to increase output verbosity (default: 1)
  -m, --write-metadata  Output a metadata tsv for the given ENA accessions (default: False)
  -d, --download-files  Download fastq files for the given ENA accessions (default: False)
  -e, --write-excel     Create an External Import-compatible Excel file for legacy pipelines for the given ENA accessions, stored by project (default:
                        False)
```

## Downloading read data

Downloads may fail, but download progress is saved. 
Each downloaded file is checked for integrity by comparing md5 checksums.
Re-running the script will only download files that have not yet been downloaded successfully.
The script downloads files at a limited rate to avoid overloading the ENA FTP server.

## Install
### Development
```bash
python -m venv venv
source venv/bin/activate
pip install -e ".[test]"
```

### Docker
To run the `enadownloader` script from the production docker image (tagged `enadownload:latest` below):
```bash
docker build -t enadownload:latest --target=runner .
docker run -it --rm enadownload:latest enadownloader --help
```

To access a shell inside the image:
```bash
docker run -it --rm enadownload:latest bash
```

## Testing
After installing packages required for development, run:
```bash
pytest --cov src --cov-branch --cov-report term-missing --cov-fail-under 80
```

Alternatively, run within the test docker image:
```bash
docker build -t enadownload:test --target=test .
docker run -it --rm enadownload:test pytest --cov src --cov-branch --cov-report term-missing --cov-fail-under 80
```

## Deployment on Sanger HPC (farm)

See [here](./docs/gitlab_ci.md)