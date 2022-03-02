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
