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
from typing import Dict, List

import pandas as pd
import zulip
from datetime import datetime

# Vars
PKG_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.join(PKG_DIR, '..')
CONFIG = {
    # 'outdir': PROJECT_DIR,
    # 'report1_filename': 'fhir-zulip-nlp - frequencies and date ranges.csv',
    # 'report2_filename': 'fhir-zulip-nlp - age and date analyses.csv',
    'zuliprc_path': os.path.join(PROJECT_DIR, 'zuliprc'),  # rc = "runtime config"
    'outpath': os.path.join(PROJECT_DIR, 'zulip_report.csv')
}


# Functions
'''
def data(key):
    client = zulip.Client(config_file=CONFIG['zuliprc_path'])
    vals = {}
    mess = get_messages_and_topics(key)

    vals['name'] = key
    vals['number of occurances']=len(mess)


    newest = {
        "anchor": "newest",
        "num_before": 1,
        "num_after": 0,
        "narrow": [
            {"operator": "stream", "operand": "terminology"},
            {"operator": "search", "operand": key},
            ],
        }
    result_new = client.get_messages(newest)
    datenew = result_new['messages'][0]['timestamp']
    datenew1 = datetime.utcfromtimestamp(datenew).strftime('%Y-%m-%d %H:%M:%S')
    vals['newest time']=datenew1

    oldest = {
        "anchor": "oldest",
        "num_before": 0,
        "num_after": 1,
        "narrow": [
            {"operator": "stream", "operand": "terminology"},
            {"operator": "search", "operand": key},
            ],
        }
    result_old = client.get_messages(oldest)
    dateold = result_old['messages'][0]['timestamp']
    dateold1 = datetime.utcfromtimestamp(dateold).strftime('%Y-%m-%d %H:%M:%S')
    vals['oldest time']=dateold1

    return vals
'''


def messagepull(anchor: str, before: int, after: int, keyword: str) -> Dict:
    """Get messages"""
    client = zulip.Client(config_file=CONFIG['zuliprc_path'])
    request = {
            "anchor": anchor,
            "num_before": before,
            "num_after": after,
            "narrow": [
                {"operator": "stream", "operand": "terminology"},
                {"operator": "search", "operand": keyword},
            ],
            }
    result = client.get_messages(request)
    return result 


def format_timestamp(i):
    """Format datetime string"""
    return datetime.utcfromtimestamp(i).strftime('%Y-%m-%d %H:%M:%S')


# TODO: will probably want to pass (i) chat.fhir.org, (ii) list of streams
def get_data(key):
    """Fetch data from API
    Resources
      - Get all streams (probably not needed): https://zulip.com/api/get-streams
    """
    bob = False
    i = 1
    allmess: List = []
    while True:
        if bob:
            break
        res = messagepull(str(0), 0, i, key)
        allmess = res['messages']
        i += 1
        bob = res['found_newest']

    newest = format_timestamp(messagepull('newest', 1, 0, key)['messages'][0]['timestamp'])
    oldest = format_timestamp(messagepull('oldest', 0, 1, key)['messages'][0]['timestamp'])

    vals = {'name': key, 'number of occurences': len(allmess), 'most recent message': newest, 'oldest message': oldest}
    return vals


def alldicts(lis):
    """Get all dictionaries"""
    message_objects = []
    for i in lis:
        data = get_data(i)
        message_objects.append(data)
    newmessages = sorted(message_objects, key=lambda d: d['number of occurances'])
    df = pd.DataFrame(newmessages)
    df.to_csv(CONFIG['outpath'], index=False)
    return newmessages


# Execution
if __name__ == '__main__':
    code_systems = ['DICOM', 'SNOMED', 'LOINC', 'ICD', 'NDC', 'RxNorm']
    # message_objects = []
    # for i in code_sys_names:
    #     x = data(i)
    #     message_objects.append(x)
    # newmessages = sorted(message_objects, key=lambda d: d['number of occurances'])
    # df = pd.DataFrame(newmessages)
    # df.to_csv(CONFIG['outpath'], index=False)
    all_results = alldicts(code_systems)
    print(all_results)
