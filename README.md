# ENA Download
Robust tool for downloading fastq.gz and metadata from ENA.

When started it fires off several jobs to ENA to try and download data in tandem, 
repeating a failed job up to 5 times, and saving progress of each md5 passed download 
to a file. When restarted it will read this file and skip any downloads that were already successful.

## Install
### Development
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Docker
```
docker build . -t ENADownload
docker run -it --entrypoint /bin/ash ENADownload
```

## Distribution
Gitlab CI automatically builds and pushes the Docker image.
By default it will build the image as `<branch>-<commit SHA>`.
These are development images and should only be used for development and testing.
After a Merge Request has been approved and merged, create a new `tag` on `master`: https://gitlab.internal.sanger.ac.uk/sanger-pathogens/enadownload/-/tags.

This will start a new job on the CI building the image as `<tag>-<commit SHA>` which can then be fed into [farm5-etc](https://gitlab.internal.sanger.ac.uk/sanger-pathogens/farm5-etc/-/blob/master/software/current/enadownload.yml#L1) 
