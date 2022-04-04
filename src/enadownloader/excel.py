import logging
from os.path import basename
from typing import List, Union

from xlwt import Style, Workbook, Worksheet, easyxf

st = easyxf("pattern: pattern solid;")
st.pattern.pattern_fore_colour = 50
default = Style.default_style


class ValueFormatClass:
    def __init__(self, name: str, value: Union[str, int], format: Style = st):
        self.name = name
        self.value = value
        self.format = format

    def __repr__(self):
        return str(self.value) or ""


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
        self.supplier_name = ValueFormatClass("Supplier Name", supplier_name)
        self.supplier_organisation = ValueFormatClass(
            "Supplier Organisation", supplier_organisation
        )
        self.contact_name = ValueFormatClass("Sanger Contact Name", contact_name)
        self.sequencing_technology = ValueFormatClass(
            "Sequencing Technology", sequencing_technology
        )
        self.study_name = ValueFormatClass("Study Name", study_name)
        self.study_accession_number = ValueFormatClass(
            "Study Accession number", study_accession_number or "", format=default
        )
        self.size = ValueFormatClass("Total size of files in GBytes", size_in_gb)
        self.date_to_keep_until = ValueFormatClass(
            "Data to be kept until", date_to_keep_until
        )

    def write(self, sheet: Worksheet):
        row = 0
        for row, header in enumerate(
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
            sheet.write(row, 0, header.name, header.format)
            sheet.write(row, 1, header.value)

        return row + 1


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
        self.filename = ValueFormatClass("Filename", value=filename)
        self.sample_name = ValueFormatClass("Sample Name", value=sample_name)
        self.taxon = ValueFormatClass("Taxon ID", value=taxon)
        self.mate_file = ValueFormatClass("Mate File", format=default, value=mate_file)
        self.sample_accession = ValueFormatClass(
            "Sample Accession number", format=default, value=sample_accession
        )
        self.library = ValueFormatClass("Library Name", format=default, value=library)
        self.fragment = ValueFormatClass(
            "Fragment Size", format=default, value=fragment
        )
        self.read_count = ValueFormatClass(
            "Read Count", format=default, value=read_count
        )
        self.base_count = ValueFormatClass(
            "Base Count", format=default, value=base_count
        )
        self.comments = ValueFormatClass("Comments", format=default, value=comments)

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
            sheet.write(row, column, value.name, value.format)


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
                self.sheet.write(row, c, value.value)
            row += 1

        self.book.save(filename)
        logging.info(f"Wrote Excel file to {basename(filename)}")
