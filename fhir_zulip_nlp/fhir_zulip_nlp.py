"""FHIR Zulip NLP Analysis

TODO's
  1. turn into GH issue, link that doc there, and link GH issue here
  2. set up env and put instructions in readme
  3. Need to show a report of all queries where keyword returned no messages

Resources:
  1. Project requirements: https://github.com/jhu-bids/fhir-zulip-nlp-analysis/issues/1
  2. Zulip API docs: https://zulip.com/api/rest
    - Links to specific endpoints/articles are included in functions below.
    - Get all streams (probably not needed): https://zulip.com/api/get-streams
    - https://zulip.com/api/get-messages
  3. The Zulip chat we're querying: https://chat.fhir.org/#

Possible areas of improvement
  1. Save to calling directory, not project directory.
"""
import json
import os
import sys
from typing import Dict, List, Union

import pandas as pd
import time
import zulip
from datetime import datetime

# Vars
PKG_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.join(PKG_DIR, '..')
CACHE_DIR = os.path.join(PKG_DIR, 'cache')
RAW_RESULTS_FILENAME = 'zulip_raw_results.csv'
CONFIG = {
    # 'outdir': PROJECT_DIR,
    # 'report1_filename': 'fhir-zulip-nlp - frequencies and date ranges.csv',
    # 'report2_filename': 'fhir-zulip-nlp - age and date analyses.csv',
    'zuliprc_path': os.path.join(PROJECT_DIR, 'zuliprc'),  # rc = "runtime config"
    'chat_stream_name': 'terminology',
    'num_messages_per_query': 1000,
    'outpath_report1': os.path.join(PROJECT_DIR, 'zulip_report1.csv'),
    'outpath_raw_results': os.path.join(PROJECT_DIR, RAW_RESULTS_FILENAME),
    'cache_outpath': os.path.join(CACHE_DIR, RAW_RESULTS_FILENAME),
}
# TODO: need to account for spelling variations
# TODO: need to account for overlap. CDA is a subset of C-CDA, so need to prune results for these miscatches.
KEYWORDS = {
    'code_systems': ['DICOM', 'SNOMED', 'LOINC', 'ICD', 'NDC', 'RxNorm'],
    'product_families': ['V2', 'Version 2', 'CDA', 'C-CDA', 'V3', 'Version 3'],
    'terminology_resources': [
        'ConceptMap',
        'CodeSystem',
        'ValueSet',
        'Terminology Service',
        'TerminologyCapabilities',
        'NamingSystem',
        'Code',
        'Coding',
        'CodeableConcept',
    ],
    'terminology_operations': [
        '$lookup',
        '$validate-code',
        '$subsumes',
        '$find-matches',
        '$expand',
        '$validate-code',
        '$translate',
        '$closure',
    ],
}
CLIENT = zulip.Client(config_file=CONFIG['zuliprc_path'])


# Functions
def format_timestamp(timestamp: int):
    """Format datetime string"""
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def message_pull(anchor: Union[int, str], num_before: int, num_after: int, keyword: str) -> Dict:
    """Get messages: https://zulip.com/api/get-messages"""
    request = {
        "anchor": anchor if anchor != 0 else 'oldest',
        "num_before": num_before,
        "num_after": num_after,
        "narrow": [
            {"operator": "stream", "operand": CONFIG['chat_stream_name']},
            {"operator": "search", "operand": keyword}]}
    result = CLIENT.get_messages(request)
    return result


def query_keyword(keyword: str, anchor: int = 0, num_after: int = CONFIG['num_messages_per_query']):
    """Fetch data from API
    anchor: Integer message ID to anchor fetching of new messages. Supports special string values too; to learn more,
      see: https://zulip.com/api/get-messages
    num_after: The number of messages with IDs greater than the anchor to retrieve. Max: 5000. Recommended: <=1000."""
    err_msg = None
    messages: List[Dict] = []
    try:
        while True:
            res: Dict = message_pull(anchor=anchor, num_before=0, num_after=num_after, keyword=keyword)
            if 'retry-after' in res:  # rate limit, 200messages/user/minute
                wait_seconds_required: float = res['retry-after']
                time.sleep(wait_seconds_required * 1000)  # ms
                continue
            messages += res['messages']
            if not messages:
                print(f'No messages found for: {keyword}')
                break
            anchor = messages[-1]['id']  # returned messages are in chronological order; -1 is most recent in batch
            # if i == 3:  # TODO: temp debugging
            #     break
            if res['found_newest']:  # this assumes API will reliably always return this
                break

    # TODO: @Rohan: We may never need this, but the API could change and things could break. I'm not sure whether or not
    #  ...this is the best approach, continuing after error. Or maybe it should just exit? What do you think?
    except Exception as err:
        err_msg = str(err)
        print(f'Got error querying {keyword}. Original error: {err}', file=sys.stderr)
        print(f'Stopping for "{keyword}" and continuing on with the next keyword.', file=sys.stderr)

    # TODO: Check: If a message contains the keyword more than once, will it return more than 1 result? or
    #  ...simply 1 message result?
    kw_report = {
        'keyword': keyword,
        'number of occurrences': len(messages),
        # This is oldest/newest of all such messages. However, depending on caching / what was passed to this function,
        # the oldest in `allmess` may be newer than the absolute oldest, and newest in `allmess` may be older than the
        # absolute oldest.
        'newest_datetime_this_session': format_timestamp(messages[0]['timestamp']) if messages else None,
        'oldest_datetime_this_session': format_timestamp(messages[-1]['timestamp']) if messages else None,
        'newest_datetime_absolute':
            format_timestamp(message_pull('newest', 1, 0, keyword)['messages'][0]['timestamp']) if messages else None,
        'oldest_datetime_absolute':
            format_timestamp(message_pull('oldest', 0, 1, keyword)['messages'][0]['timestamp']) if messages else None,
    }
    if err_msg:
        kw_report['error'] = str(err_msg)
    return kw_report, messages


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format dataframe"""
    # Reorganize columns
    cols_head = ['category', 'keyword']
    cols_tail = [x for x in list(df.columns) if x not in cols_head]
    df = df[cols_head + cols_tail]
    # Sort values
    df = df.sort_values(cols_head + ['timestamp'] if 'timestamp' in cols_tail else [])
    return df


def query_categories(category_keywords: Dict[str, List[str]]) -> pd.DataFrame:
    """Get all dictionaries"""
    # Load cache
    last_msg_id = 0
    cache_df = pd.DataFrame()
    cache_json_path = CONFIG['cache_outpath'].replace('.csv', '.json')
    cache_json = {cat: {k: [] for k in keywords} for cat, keywords in category_keywords.items()}
    if os.path.exists(CONFIG['cache_outpath']):
        cache_df = pd.read_csv(CONFIG['cache_outpath'])
    reports: List[Dict] = []
    messages: List[Dict] = []
    for category, keywords in category_keywords.items():
        for k in keywords:
            if len(cache_df) > 0:
                kw_cache_df = cache_df[cache_df['keyword'] == k]
                if len(kw_cache_df) > 0:
                    last_msg_id = list(kw_cache_df['id'])[-1]
            kw_report, kw_messages = query_keyword(keyword=k, anchor=last_msg_id)
            kw_report = {**kw_report, **{'category': category}}
            kw_messages = [{**x, **{'category': category, 'keyword': k}} for x in kw_messages]
            cache_json[category][k] = kw_report
            reports.append(kw_report)
            messages += kw_messages

    # Save outputs
    # TODO: could probably put more of these lines into function as well, and rename it 'format and save'
    df_report1 = pd.DataFrame(reports)
    df_report1 = format_df(df_report1)
    df_report1.to_csv(CONFIG['outpath_report1'], index=False)
    df_raw = pd.DataFrame(messages)
    df_raw = format_df(df_raw)
    df_raw.to_csv(CONFIG['outpath_raw_results'], index=False)

    # Save cache
    if not os.path.exists(CACHE_DIR):
        os.mkdir(CACHE_DIR)
    cache_df_new = pd.concat([cache_df, df_raw])
    cache_df_new = format_df(cache_df_new)
    cache_df_new.to_csv(CONFIG['cache_outpath'], index=False)
    cache_json_old = {}
    if os.path.exists(cache_json_path):
        with open(cache_json_path, 'r') as file:  # just in case useful
            cache_json_old = json.loads(file.read())
    cache_json_new = {**cache_json_old, **cache_json}
    with open(cache_json_path, 'w') as file:  # just in case useful
        json.dump(cache_json_new, file)

    return df_raw


# Execution
if __name__ == '__main__':
    all_results = query_categories(KEYWORDS)
