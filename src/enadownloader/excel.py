from datetime import datetime
import logging
import re
from os.path import basename
from typing import List, Union

from xlwt import Style, Workbook, Worksheet, easyxf

solid_green_style = easyxf("pattern: pattern solid;")
solid_green_style.pattern.pattern_fore_colour = 50
default_style = Style.default_style
date_style = easyxf(num_format_str="DD/MM/YYYY")
float_style = easyxf(num_format_str="0.00")


class ValueFormatClass:
    def __init__(self, value: Union[str, int], format: Style = default_style):
        self.value = value
        self.format = format

    def __repr__(self):
        return str(self.value) or ""


class HeaderValue:
    def __init__(self, header: ValueFormatClass, value: ValueFormatClass):
        self.header = header
        self.value = value

    def __repr__(self):
        return f"{self.header.value}: {self.value.value}"


def regex_clean(value: str):
    return re.sub(pattern=r"[^\w]+", repl=" ", string=value)


class FileHeader:
    def __init__(
        self,
        supplier_name: str,
        supplier_organisation: str,
        contact_name: str,
        sequencing_technology: str,
        study_name: str,
        size_in_gb: int,
        date_to_keep_until: str,
        study_accession_number: str = None,
    ):
        self.supplier_name = HeaderValue(
            ValueFormatClass("Supplier Name", solid_green_style),
            ValueFormatClass(regex_clean(supplier_name)),
        )
        self.supplier_organisation = HeaderValue(
            ValueFormatClass("Supplier Organisation", solid_green_style),
            ValueFormatClass(supplier_organisation),
        )
        self.contact_name = HeaderValue(
            ValueFormatClass("Sanger Contact Name", solid_green_style),
            ValueFormatClass(contact_name),
        )
        self.sequencing_technology = HeaderValue(
            ValueFormatClass("Sequencing Technology", solid_green_style),
            ValueFormatClass(sequencing_technology),
        )
        self.study_name = HeaderValue(
            ValueFormatClass("Study Name", solid_green_style),
            ValueFormatClass(regex_clean(study_name)),
        )
        self.study_accession_number = HeaderValue(
            ValueFormatClass("Study Accession number"),
            ValueFormatClass(study_accession_number or ""),
        )
        self.size = HeaderValue(
            ValueFormatClass("Total size of files in GBytes", solid_green_style),
            ValueFormatClass(size_in_gb, float_style),
        )
        self.date_to_keep_until = HeaderValue(
            ValueFormatClass("Data to be kept until", solid_green_style),
            ValueFormatClass(
                datetime.strptime(date_to_keep_until, "%d/%m/%Y"), date_style
            ),
        )

    def write(self, sheet: Worksheet):
        row_index = 0
        for row_index, data in enumerate(
            [
                self.supplier_name,
                self.supplier_organisation,
                self.contact_name,
                self.sequencing_technology,
                self.study_name,
                self.study_accession_number,
                self.size,
                self.date_to_keep_until,
            ]
        ):
            sheet.write(row_index, 0, data.header.value, data.header.format)
            sheet.write(row_index, 1, data.value.value, data.value.format)

        return row_index + 1


class Data:
    def __init__(
        self,
        filename: str,
        sample_name: str,
        taxon: int,
        mate_file: str = None,
        sample_accession: str = None,
        library: str = None,
        fragment: str = None,
        read_count: str = None,
        base_count: str = None,
        comments: str = None,
    ):
        self.filename = HeaderValue(
            ValueFormatClass("Filename", solid_green_style), ValueFormatClass(filename)
        )
        self.sample_name = HeaderValue(
            ValueFormatClass("Sample Name", solid_green_style),
            ValueFormatClass(sample_name),
        )
        self.taxon = HeaderValue(
            ValueFormatClass("Taxon ID", solid_green_style), ValueFormatClass(taxon)
        )
        self.mate_file = HeaderValue(
            ValueFormatClass("Mate File", default_style), ValueFormatClass(mate_file)
        )
        self.sample_accession = HeaderValue(
            ValueFormatClass("Sample Accession number", default_style),
            ValueFormatClass(sample_accession),
        )
        self.library = HeaderValue(
            ValueFormatClass("Library Name", default_style), ValueFormatClass(library)
        )
        self.fragment = HeaderValue(
            ValueFormatClass("Fragment Size", default_style), ValueFormatClass(fragment)
        )
        self.read_count = HeaderValue(
            ValueFormatClass("Read Count", default_style), ValueFormatClass(read_count)
        )
        self.base_count = HeaderValue(
            ValueFormatClass("Base Count", default_style), ValueFormatClass(base_count)
        )
        self.comments = HeaderValue(
            ValueFormatClass("Comments", default_style), ValueFormatClass(comments)
        )

        self.order = [
            self.filename,
            self.mate_file,
            self.sample_name,
            self.sample_accession,
            self.taxon,
            self.library,
            self.fragment,
            self.read_count,
            self.base_count,
            self.comments,
        ]

    def write_header(self, sheet: Worksheet, row):
        for column, value in enumerate(self.order):
            sheet.write(row, column, value.header.value, value.header.format)


class ExcelWriter:
    def __init__(self, header: FileHeader, data: List[Data]):
        """Raises a ValueError if not all required columns are present"""
        self.header = header
        self.data = data

        self.book = Workbook()
        self.sheet: Worksheet = self.book.add_sheet("Sheet1")

    def write(self, filename: str):
        if not self.data:
            logging.warning(
                f"{self.__class__.__name__} - Found no filepaths to write for {self.header.study_accession_number.value}. Skipping."
            )
            return

        row = self.header.write(self.sheet)
        row += 1
        self.data[0].write_header(self.sheet, row)
        row += 1

        for r in self.data:
            for c, value in enumerate(r.order):
                # This is poetry XD
                self.sheet.write(row, c, value.value.value, value.value.format)
            row += 1

        self.book.save(filename)
        logging.info(f"Wrote Excel file to {basename(filename)}")
