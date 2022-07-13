# HL7 FHIR Zulip chat NLP analysis
Ad hoc NLP (Natural Language Processing) analysis of HL7 FHIR's online Zulip chat streams.

## Setting up and running
0. Prerequisites: Python3
1. Clone this repository.
2. From the repository directory, install libraries: `python3 -m pip install -r requirements.txt`
3. Get `.zuliprc`: Open up [chat.fhir.org](https://chat.fhir.org). Then follow the 
   [Zulip API key documentation](https://zulip.com/api/api-keys). It will instruct you to fetch your API Key, but if you
   follow these instructions, when you get to the point where it shows you your API Key, there will also be an option 
   that says _"Download .zuliprc"_. Click that, and it will download a file. It might save the file under a different
   filename. If so, you should rename that `.zuliprc`. Place this file in the root directory of this cloned repository.
4. From the repository directory, run: `python3 -m fhir_zulip_nlp`

## Results
After running, analysis reports will be generated and saved in the repository directory as CSV files with the name 
pattern `fhir-zulip-nlp*.csv`.

Releases from previous runs have been uploaded on [GitHub](https://github.com/jhu-bids/fhir-zulip-nlp-analysis/releases)
and [GoogleDrive](https://drive.google.com/drive/u/0/folders/16MFLnKoKA5gk4ELbSnVS2GCjgR_R0ETL).