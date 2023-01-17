import asyncio
import logging
import os
from os.path import join
from pathlib import Path

from enadownloader.argparser import Parser
from enadownloader.enadownloader import ENADownloader
from enadownloader.enametadata import ENAMetadata
from enadownloader.utils import AccessionValidator


def main(args=None):
    logfile = join(os.getcwd(), "enadownloader.log")

    # Set up logging
    fh = logging.FileHandler(logfile, mode="w")
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()

    args = Parser.arg_parser(args)

    # noinspection PyArgumentList
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[fh, sh],
    )

    logging.info(f"Absolute output folder path: {args.output_dir.resolve()}")

    with open(args.input) as f:
        accessions = {l.strip() for l in f}

    logging.debug(f"Checking accession validity...")
    accessions = AccessionValidator.parse_accessions(
        accessions=accessions, accession_type=args.type
    )
    if not accessions:
        logging.fatal("No valid accessions provided")
        exit(1)

    enametadata = ENAMetadata(accessions=accessions, accession_type=args.type)

    if args.write_metadata:
        enametadata.write_metadata_file(args.output_dir)

    # They both need folder management, so I'm grouping them together
    if args.download_files or args.write_excel:
        output_files = set()
        for project, rows in enametadata.group_by_project().items():
            run_accessions = {row["run_accession"] for row in rows}
            enametadata_obj = ENAMetadata(
                accessions=run_accessions, accession_type="run"
            )
            # Prevents us from having to call metadata again
            enametadata_obj.metadata = {
                row["run_accession"]: row
                for row in enametadata.metadata.values()
                if row["run_accession"] in run_accessions
            }

            # Do generic stuff first
            if args.create_study_folders:
                output_dir: Path = args.output_dir / project
                logging.info(
                    f"Using output directory {output_dir.relative_to(args.output_dir.parent)}"
                )
            else:
                output_dir: Path = args.output_dir

            output_dir.mkdir(parents=True, exist_ok=True)

            # Specifics
            if args.write_excel:
                ENAMetadata.to_excel(output_dir, rows)

            if args.download_files:
                enadownloader = ENADownloader(
                    output_dir=output_dir,
                    metadata_obj=enametadata_obj,
                    retries=args.retries,
                    log_full_path=args.log_full_path,
                )
                try:
                    asyncio.run(enadownloader.download_all_fastqs())
                except enadownloader.NoSuccessfulDownloads as err:
                    logging.error(f"{err} for project {project}")
                    exit(1)

                output_files.update({ena.ftp for ena in enadownloader.load_progress()})

            if args.create_study_folders:
                logging.info("-" * 50)
