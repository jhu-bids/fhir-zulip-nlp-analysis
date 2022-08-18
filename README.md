# HL7 FHIR Zulip chat NLP analysis
Ad hoc NLP (Natural Language Processing) analysis of HL7 FHIR's online Zulip chat streams.

## Management GoogleSheeet
The program reads directly [from this Google Sheet](https://docs.google.com/spreadsheets/d/1OB0CEAkOhVTN71uIhzCo_iNaiD1B6qLqL7uwil5O22Q/). Allows for the management of:
- Category keywords
- User roles

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

### Codebook
#### `zulip_raw_results.csv`
TODO

#### `zulip_raw_results_user_participation.csv`
TODO

#### `zulip_report_queries_with_no_results.csv`
TODO

#### `zulip_user_info.csv`
TODO

#### `zulip_report1_counts.csv`
TODO

#### `zulip_report2_thread_lengths.csv`
TODO

#### `zulip_report3_users.csv`
- `user.id` (`integer`): Zulip-assigned user ID. Can look up this ID in `zulip_user_info.csv` to find more info.
- `user.full_name` (`string`): Full name of user.
- `stream` (`string`): Zulip stream.
- `category` (`string`): Category as defined in [GoogleSheet](https://docs.google.com/spreadsheets/d/1OB0CEAkOhVTN71uIhzCo_iNaiD1B6qLqL7uwil5O22Q/).
- `keyword` (`string`): Keyword as defined in [GoogleSheet](https://docs.google.com/spreadsheets/d/1OB0CEAkOhVTN71uIhzCo_iNaiD1B6qLqL7uwil5O22Q/).
- `role` (`string`): Either 'author' or 'respondent'.
- `count` (`integer`): The sum total for of the combination of **`user` x `stream` x `category` x `keyword` x `role`** 
  for the given row. If any of these cells in the row is empty, it means that this row represents a higher-level 
  aggregation. E.g. if only `keyword` is empty, the `count` represents the combo of **`user` x `stream` x `category` x 
  `role`**. If `keyword` and `category` are empty, the `count` represents the combo of **`user` x `stream` x 
  `role`**.
