"""FHIR Zulip NLP Analysis
# TODO: turn into GH issue, link that doc there, and link GH issue here
# TODO: set up env and put instructions in readme
Resources:
  1. Project requirements: https://github.com/jhu-bids/fhir-zulip-nlp-analysis/issues/1
  2. Zulip API docs: https://zulip.com/api/rest
    - Links to specific endpoints/articles are included in functions below.
  3. The Zulip chat we're querying: https://chat.fhir.org/#

Possible areas of improvement
  1. Save to calling directory, not project directory.
"""
import os
from typing import Dict

import pandas as pd
import zulip


# Vars
PKG_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.join(PKG_DIR, '..')
CONFIG = {
    'outdir': PROJECT_DIR,
    'report1_filename': 'fhir-zulip-nlp - frequencies and date ranges.csv',
    'report2_filename': 'fhir-zulip-nlp - age and date analyses.csv',
    'zuliprc_path': os.path.join(PROJECT_DIR, '.zuliprc')  # rc = "runtime config"
}


# todo: maybe turn into a class or decorator to share common logic
# Functions
def analysis_frequencies(data, outpath: str = None) -> pd.DataFrame:
    """Analyze counts and date ranges
    Requirements 1-5
    """
    # @Rohan: I think that the data you get back from Zulip is likely to be a Python dictionary. There are several ways
    # to get your data into a dataframe. Personally, Here's the way that I find the easiest:
    # 1. Put all of your "rows" into a python list:
    # rows = [{'keyword': 'SNOMED', 'count': 100}, {'keyword': 'ICD10', 'count': 50}]
    # 2. then you can simply convert this kind of structure shown above into a dataframe like this:
    # df = pd.DataFrame(rows)
    print(data)
    df = pd.DataFrame()

    if outpath:
        df.to_csv(outpath, index=False)
    return df


def analysis_age_and_date(data, outpath: str = None) -> pd.DataFrame:
    """Analyze age and date
    Requirements 6
    """
    print(data)
    df = pd.DataFrame()

    if outpath:
        df.to_csv(outpath, index=False)
    return df


def normalize_data(data):
    """Normalize data
    @Rohan: I created this function as a separate step to take the data you got from Zulip, and maybe transform it
    into something easier to work with. Maybe this step is not necessary, though. Pehraps the data you get back from
    the Zulip API will be in an easy format to work with.
    """
    return data


# TODO: will probably want to pass (i) chat.fhir.org, (ii) list of streams
def get_data(zuliprc_path: str):
    """Fetch data from API
    Resources
      - Get all streams (probably not needed): https://zulip.com/api/get-streams
    """
    data = None  # temp; dk what format the data will be in yet

    # Initialize client
    client = zulip.Client(config_file=zuliprc_path)

    # Get the ID of a given stream
    # https://zulip.com/api/get-stream-id
    stream_name = 'terminology'
    stream_id_response = client.get_stream_id(stream_name)
    stream_id: str = stream_id_response['id']  # not sure if this is how to get the 'id'. would have to check docs

    # todo: then fetch stream by id
    # https://zulip.com/api/get-stream-by-id
    # I haven't checked docs yet, but I imagine we pass the ID here
    # It may also be possible that we can get messages back from this as well
    stream_data = client.get_stream_id(stream_id)

    # todo: might need to fetch topics before querying messages, not sure
    # https://zulip.com/api/get-stream-topics#get-topics-in-a-stream
    topics = client.get_stream_topics(stream_id)

    # todo: then fetch messages
    # Hopefully everything we need is available to request from this endpoint,
    # https://zulip.com/api/get-messages
    messages_request_data = {}  # todo: populate w/ stream ID (I think) and other params
    messages = client.get_messages(messages_request_data)

    return data


def run(outdir: str, report1_filename: str, report2_filename: str, zuliprc_path: str) -> Dict[str, pd.DataFrame]:
    """Run analysis"""
    # Collect and normalize data
    # TODO: (i) get data, (ii) what format to normalize into? dict? df?
    data = get_data(zuliprc_path)
    data = normalize_data(data)

    # Analyze
    df1 = analysis_frequencies(data, os.path.join(outdir, report1_filename))
    df2 = analysis_age_and_date(data, os.path.join(outdir, report2_filename))

    # how to save to csv, example
    df1.to_csv(os.path.join(PROJECT_DIR, 'my_csv.csv'), index=False)

    # Return
    report = {
        report1_filename: df1,
        report2_filename: df2
    }
    return report


# Execution
if __name__ == '__main__':
    run(**CONFIG)
