"""FHIR Zulip NLP Analysis

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
import os
import sys
from typing import Dict, List, Optional, Union

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
    # TODO: these types of files should have a '.' in front of them, so will change.
    'zuliprc_path': os.path.join(PROJECT_DIR, 'zuliprc'),  # rc = "runtime config"
    'chat_stream_name': 'terminology',
    'num_messages_per_query': 1000,
    'outpath_report1': os.path.join(PROJECT_DIR, 'zulip_report1.csv'),
    'outpath_errors': os.path.join(PROJECT_DIR, 'zulip_errors.csv'),
    'outpath_no_results': os.path.join(PROJECT_DIR, 'zulip_report_keywords_with_no_results.csv'),
    'outpath_raw_results': os.path.join(PROJECT_DIR, RAW_RESULTS_FILENAME),
    'cache_outpath': os.path.join(CACHE_DIR, RAW_RESULTS_FILENAME),
}
# TODO: need to account for spelling variations
# TODO: need to account for overlap. CDA is a subset of C-CDA, so need to prune results for these miscatches.
# Download new CSVs from: https://docs.google.com/spreadsheets/d/1J_PRWi2arsWQ9IJlg1iDfCeRIAzEWRYrBPTPVfqna3Y/edit#gid=1023607044
KEYWORDS = {
    'code_systems': [
        'DICOM',
        'SNOMED',
        'LOINC',
        'ICD',
        'NDC',
        'RxNorm'
    ],
    'product_families': [
        'V2',
        'Version 2',
        'CDA',
        'C-CDA',
        'V3',
        'Version 3'
    ],
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


def query_keyword(
    keyword: str, anchor: int = 0, num_after: int = CONFIG['num_messages_per_query']
) -> (List[Dict], Optional[str]):
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
            if res['found_newest']:  # this assumes API will reliably always return this
                break
    # TODO: @Rohan: We may never need this, but the API could change and things could break. I'm not sure whether or not
    #  ...this is the best approach, continuing after error. Or maybe it should just exit? What do you think?
    except Exception as err:
        err_msg = str(err)
        print(f'Got error querying {keyword}. Original error: {err_msg}', file=sys.stderr)
        print(f'Stopping for "{keyword}" and continuing on with the next keyword.', file=sys.stderr)

    return messages, err_msg


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format dataframe"""
    # Reorganize columns
    cols_head = ['category', 'keyword']
    cols_tail = [x for x in list(df.columns) if x not in cols_head]
    df = df[cols_head + cols_tail]
    # Sort values
    df = df.sort_values(cols_head + ['timestamp'] if 'timestamp' in cols_tail else [])
    return df
'''
def time_format(seconds):
    hours = seconds // 3600
    minutes = seconds % 3600 // 60
    secs = seconds % 3600 % 60
    return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, secs)
'''

def create_report1(
    df: pd.DataFrame, category_keywords: Dict[str, List[str]] = KEYWORDS
) -> (pd.DataFrame, pd.DataFrame):
    """Report 1: counts and latest/oldest message timestamps"""
    # No-results report
    reports: List[Dict] = []
    no_result_keywords: List[str] = []
    for category, keywords in category_keywords.items():
        for k in keywords:
            df_kw = df[df['keyword'] == k]
            df_kw = df_kw.sort_values(['timestamp'])  # oldest first
            z = ((list(df_kw['timestamp'])[-1] - list(df_kw['timestamp'])[0])/86400)
            threadlen= f'{z:.1f}'
            kw_report = {
                'category': category,
                'keyword': k,
                # TODO: Check: If a message contains the keyword more than once, will it return more than 1 result? or
                #  ...simply 1 message result? @Davera
                'num_messages_with_keyword': len(df_kw),
                'newest_message_datetime': format_timestamp(list(df_kw['timestamp'])[-1]) if len(df_kw) > 0 else None,
                'oldest_message_datetime': format_timestamp(list(df_kw['timestamp'])[0]) if len(df_kw) > 0 else None,
                'thread_length': threadlen
            }
            if kw_report['num_messages_with_keyword'] == 0:
                no_result_keywords.append(k)
            reports.append(kw_report)

    # Report 1
    df_report1 = pd.DataFrame(reports)
    df_report1 = format_df(df_report1)

    # No results report
    df_no_results = pd.DataFrame()
    df_no_results['keywords_with_no_results'] = no_result_keywords

    return df_report1, df_no_results


# TODO: In order to account for the possibility that people could edit their prior messages, can add as a param to this
#  ...function "account_for_edits"=True, or "cache"=False
# TODO: Add "as_of" field with today's date at the time we ran this script.
# TODO: spelling variations: v2 and V2 should be the same count, "Terminology Service" and
#  "Terminology Svc", "$lookup" and "lookup operation(s)", etc.
# TODO: keyword_variations: consider adding column to spreadsheet and using e.g. V2 variations would be "V2, Version 2"
# TODO: how account for overlap? For example, CDA is different from C-CDA, but C-CDA counts probably include all
#  ...instances of CDA.
def query_categories(category_keywords: Dict[str, List[str]] = KEYWORDS) -> pd.DataFrame:
    """Get all dictionaries"""
    # Load cache
    cache_df = pd.DataFrame()
    if not os.path.exists(CACHE_DIR):
        os.mkdir(CACHE_DIR)
    if os.path.exists(CONFIG['cache_outpath']):
        cache_df = pd.read_csv(CONFIG['cache_outpath'])

    # Fetch data for all keywords for all categories
    last_msg_id = 0
    new_messages: List[Dict] = []
    errors: List[Dict[str, str]] = []
    for category, keywords in category_keywords.items():
        for k in keywords:
            # Get latest message ID from cache
            if len(cache_df) > 0:
                kw_cache_df = cache_df[cache_df['keyword'] == k]
                if len(kw_cache_df) > 0:
                    last_msg_id = list(kw_cache_df['id'])[-1]
            # Raw messages
            kw_messages, error = query_keyword(keyword=k, anchor=last_msg_id)
            kw_messages = [{**x, **{'category': category, 'keyword': k}} for x in kw_messages]
            new_messages += kw_messages
            # Error report
            errors += [{'keyword': k, 'error_message': error}] if error else []

    # Save outputs
    # - raw messages
    df_raw_new = pd.DataFrame(new_messages)
    df_raw_new = format_df(df_raw_new)  # todo: this may not be necessary; remove?
    df_raw = pd.concat([cache_df, df_raw_new])
    df_raw = format_df(df_raw)
    df_raw.to_csv(CONFIG['outpath_raw_results'], index=False)
    df_raw.to_csv(CONFIG['cache_outpath'], index=False)
    # - report 1: counts and latest/oldest message timestamps
    df_report1, df_no_results = create_report1(df=df_raw, category_keywords=category_keywords)
    df_report1.to_csv(CONFIG['outpath_report1'], index=False)
    # - keywords w/ no results
    if len(df_no_results) > 0:
        df_no_results.to_csv(CONFIG['outpath_no_results'], index=False)
    # - errors
    if errors:
        df_errors = pd.DataFrame(errors)
        df_errors.to_csv(CONFIG['outpath_errors'], index=False)

    return df_raw


# Execution
if __name__ == '__main__':
    all_results = query_categories(KEYWORDS)
