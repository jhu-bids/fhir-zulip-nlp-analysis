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
from datetime import datetime

# Vars
PKG_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.join(PKG_DIR, '..')
ZULIP_CONFIG_PATH = os.path.join(PROJECT_DIR, 'zuliprc')
CONFIG = {
    'outdir': PROJECT_DIR,
    'report1_filename': 'fhir-zulip-nlp - frequencies and date ranges.csv',
    'report2_filename': 'fhir-zulip-nlp - age and date analyses.csv',
    'zuliprc_path': os.path.join(PROJECT_DIR, 'zuliprc')  # rc = "runtime config"
}


# todo: maybe turn into a class or decorator to share common logic
# Functions
def analysis_frequencies(data, outpath: str = None) -> pd.DataFrame:
    """Analyze counts and date ranges
    Requirements 1-5
    """
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
    """Normalize data"""
    return data


# TODO: will probably want to pass (i) chat.fhir.org, (ii) list of streams
def data(key):
    """Fetch data from API
    Resources
      - Get all streams (probably not needed): https://zulip.com/api/get-streams
    """    
    bob = False
    i = 1
    while True:
        if bob:
            break
        res = messagepull(0,0,i,key)
        allmess = res['messages']
        i+=1
        bob = res['found_newest']
    
    newest = timechange(messagepull('newest',1,0,key)['messages'][0]['timestamp'])
    oldest = timechange(messagepull('oldest',0,1,key)['messages'][0]['timestamp'])
    
    vals = {'name': key, 'number of occurences': len(allmess), 'most recent message': newest, 'oldest message' : oldest}
    return vals

def messagepull(anchor,before,after,keyword):
    client = zulip.Client(config_file=ZULIP_CONFIG_PATH)
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

def timechange(i): 
    return datetime.utcfromtimestamp(i).strftime('%Y-%m-%d %H:%M:%S')

def alldicts(lis):
    message_objects = []
    for i in lis:
        x = data(i)
        message_objects.append(x)
    newmessages = sorted(message_objects, key=lambda d: d['number of occurances'])
    df = pd.DataFrame(newmessages)
    df.to_csv('~/fhir-zulip-nlp-analysis-main/zulip_report.csv', index=False)
    return newmessages

'''
def data(key):
    client = zulip.Client(config_file="~/fhir-zulip-nlp-analysis-main/zuliprc")
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

def run(outdir: str, report1_filename: str, report2_filename: str, zuliprc_path: str) -> Dict[str, pd.DataFrame]:
    """Run analysis"""
    # Collect and normalize data
    # TODO: (i) get data, (ii) what format to normalize into? dict? df?
    #part 1 of project
    '''
    code_sys_names = ['DICOM','SNOMED','LOINC','ICD','NDC','RxNorm']
    message_objects = []
    data(code_sys_names[0])
    
    for i in code_sys_names:
        curr = get_messages_and_topics(i)
        message_objects.append(curr)
    '''
         
    

    # Analyze
    df1 = analysis_frequencies( os.path.join(outdir, report1_filename))
    df2 = analysis_age_and_date( os.path.join(outdir, report2_filename))

    # Return
    report = {
        report1_filename: df1,
        report2_filename: df2
    }
    return report


# Execution
if __name__ == '__main__':
    run(**CONFIG)
    codes = ['DICOM','SNOMED','LOINC','ICD','NDC','RxNorm']
    # message_objects = []
    # for i in code_sys_names:
    #     x = data(i)
    #     message_objects.append(x)
    # newmessages = sorted(message_objects, key=lambda d: d['number of occurances'])
    # df = pd.DataFrame(newmessages)
    # df.to_csv('~/fhir-zulip-nlp-analysis-main/zulip_report.csv', index=False)
    x = alldicts(codes)
    print(x)     
    
   