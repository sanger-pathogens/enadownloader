## Deployment on Sanger HPC (farm) using Gitlab CI

Gitlab CI automatically builds and pushes the Docker image.
By default, it will build the image as `<branch>-<commit SHA>`.
These are development images and should only be used for development and testing.
After a Merge Request has been approved and merged, create a new `tag` on `master`: https://gitlab.internal.sanger.ac.uk/sanger-pathogens/enadownload/-/tags.

This will start a new job on the CI building the image as `<tag>-<commit SHA>` which can then be fed into [farm5-etc](https://gitlab.internal.sanger.ac.uk/sanger-pathogens/farm5-etc/-/blob/master/software/current/enadownload.yml#L1) 
