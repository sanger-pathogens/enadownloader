import asyncio
import logging
import os
from os.path import join

from enadownloader.argparser import Parser
from enadownloader.enadownloader import ENADownloader
from enadownloader.enametadata import ENAMetadata

logfile = join(os.getcwd(), "enadownloader.log")

# Set up logging
fh = logging.FileHandler(logfile, mode="w")
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler()

args = Parser.arg_parser()

# noinspection PyArgumentList
logging.basicConfig(
    level=args.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[fh, sh],
)

with open(args.input) as f:
    accessions = set()
    for line in f:
        accession = line.strip()
        accessions.add(accession)

enametadata = ENAMetadata(accessions=accessions, accession_type=args.type)

if args.write_metadata:
    enametadata.write_metadata_file(args.output_dir / "metadata.tsv", overwrite=True)

if args.write_excel:
    enametadata.to_excel(args.output_dir)

if args.download_files:
    for project, rows in enametadata.to_dict().items():
        run_accessions = [row["run_accession"] for row in rows]
        enametadata_obj = ENAMetadata(accessions=run_accessions, accession_type="run")
        enadownloader = ENADownloader(
            accessions=run_accessions,
            accession_type="run",
            output_dir=args.output_dir,
            create_study_folders=args.create_study_folders,
            metadata_obj=enametadata_obj,
            project_id=project,
            retries=args.retries,
        )
        asyncio.run(enadownloader.download_project_fastqs())
