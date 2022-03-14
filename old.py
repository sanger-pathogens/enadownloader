"""
Functions to implement later
"""

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

# def get_ftp_paths(self, accession):
#     if exists(self.response_file):
#         response_parsed = self.load_response()
#
#     else:
#         url = (
#             f"https://www.ebi.ac.uk/ena/portal/api/filereport?"
#             f"accession={accession}&result=read_run&fields=fastq_ftp,fastq_md5&limit=0"
#         )
#         rp = 400
#         while rp >= 300:
#             response = requests.get(url)
#             rp = response.status_code
#             if rp >= 300:
#                 logging.warning(response.text)
#                 sleep(5)
#
#         response_parsed = self.parse_file_report(response)
#         self.write_response_file(response_parsed)
#
#     return response_parsed