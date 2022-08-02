"""FHIR Zulip NLP Analysis

Resources:
  1. Project requirements: https://github.com/jhu-bids/fhir-zulip-nlp-analysis/issues/1
  2. Zulip API docs: https://zulip.com/api/rest
    - Links to specific endpoints/articles are included in functions below.
    - Get all streams (probably not needed): https://zulip.com/api/get-streams
    - https://zulip.com/api/get-messages
  3. The Zulip chat we're querying: https://chat.fhir.org/#
  4. Category keywords google sheet:
     https://docs.google.com/spreadsheets/d/1OB0CEAkOhVTN71uIhzCo_iNaiD1B6qLqL7uwil5O22Q/edit#gid=1136391153

Possible areas of improvement
  1. Save to calling directory, not project directory.

TODOs
TODO #1: Check: If a message contains the keyword more than once, will it return more than 1 result? or simply 1 message
 result? @Davera
"""
import math
import os
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, date
from typing import Dict, List, Optional, Union

import zulip
import pandas as pd

try:
    from fhir_zulip_nlp.google_sheets import get_sheets_data
except (ModuleNotFoundError, ImportError):
    from google_sheets import get_sheets_data


# Vars
TYPE_KEYWORDS_DICT = Dict[str, Dict[str, List[str]]]
INTRA_CELL_DELIMITER = ';'
PKG_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.join(PKG_DIR, '..')
ENV_DIR = os.path.join(PROJECT_DIR, 'env')
CACHE_DIR = os.path.join(PKG_DIR, 'cache')
RAW_RESULTS_FILENAME = 'zulip_raw_results.csv'
CONFIG = {
    'zuliprc_path': os.path.join(ENV_DIR, '.zuliprc'),  # rc = "runtime config"
    'chat_stream_name': 'terminology',
    'num_messages_per_query': 1000,
    'outpath_report1': os.path.join(PROJECT_DIR, 'zulip_report1_counts.csv'),
    'outpath_report2': os.path.join(PROJECT_DIR, 'zulip_report2_thread_lengths.csv'),
    'outpath_errors': os.path.join(PROJECT_DIR, 'zulip_errors.csv'),
    'outpath_no_results': os.path.join(PROJECT_DIR, 'zulip_report_keywords_with_no_results.csv'),
    'outpath_raw_results': os.path.join(PROJECT_DIR, RAW_RESULTS_FILENAME),
    'results_cache_path': os.path.join(CACHE_DIR, RAW_RESULTS_FILENAME),
    'keywords_cache_path': os.path.join(CACHE_DIR, 'keywords.csv'),
}
CLIENT = zulip.Client(config_file=CONFIG['zuliprc_path'])


# Functions
def format_timestamp(timestamp: int):
    """Format datetime string"""
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def _load_cached_messages(msg_cache_path=CONFIG['results_cache_path']):
    """Load cached messages"""
    cache_df = pd.DataFrame()
    if not os.path.exists(CACHE_DIR):
        os.mkdir(CACHE_DIR)
    if os.path.exists(msg_cache_path):
        cache_df = pd.read_csv(msg_cache_path)
    return cache_df


def _load_keywords_df(
    sheet_name='category_keywords', cache_path=CONFIG['keywords_cache_path'], use_cache_only=False
) -> pd.DataFrame:
    """Get keywords data from google sheets, else cache
        sheet_name: The name of the specific sheet within a GoogleSheet workbook."""
    if use_cache_only:
        df: pd.DataFrame = pd.read_csv(cache_path).fillna('')
    else:
        # PyBroadException: Broad because (a) don't now what failure returns now nor (b) might return in future, and
        # ...(c) basic usability is more improtant right now. The warning should be sufficient.
        # noinspection PyBroadException
        try:
            # todo: Pass URI too
            df: pd.DataFrame = get_sheets_data(sheet_name=sheet_name, env_dir=ENV_DIR)
            df.to_csv(cache_path, index=False)
        except BaseException as err:
            last_modified = str(datetime.utcfromtimestamp(os.path.getmtime(cache_path)))
            print(f'Warning: Reading from GoogleSheets failed. Using cache: '
                  f'\n- File name: {os.path.basename(cache_path)}'
                  f'\n- Last modified: {last_modified}'
                  f'\n- Error: {str(err)}', file=sys.stderr)
            df: pd.DataFrame = pd.read_csv(cache_path).fillna('')
    return df


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


def _handle_keyword_edge_cases(keyword: str, messages: List[Dict]):
    """Filter out / etc special edge cases in API responses"""
    new_list = []
    for x in messages:
        # - Filter out: Handle case where the text of one keyword is fully contained within another keyword
        # todo: minor: it is theoretically possible that a message could contain both (i) case where the
        #  text ofone keyword is fully contained within another keyword, as well as (ii) the actual keyword
        #  itself, e.g. it might contain 'C-CDA' and also ' CDA ' or ' CDA,' or ' CDA.' or some other
        #  variation. However this is really a minor edge case, and especially in the case of 'CDA' vs
        #  'C-CDA', they are very different things, so it is unlikely that they would both appear at the
        #  same time.
        if keyword == 'CDA' and 'C-CDA' in x['content']:
            continue  # don't include
        else:
            new_list.append(x)
    return new_list


# todo: this would be a good place to use `nltk`
def _get_messages_with_context(df: pd.DataFrame, context: str) -> pd.DataFrame:
    """Given a Zulip message dataframe, get all messages w/ subject or body containing context"""
    if not context:
        return df
    df2 = df[(df['content'].str.contains(context)) | (df['subject'].str.contains(context))]
    return df2


def _get_counts_from_kw_messages(
    df: pd.DataFrame, category: str, keyword: str, spelling: str, today: str, context: str = ''
) -> Dict:
    """Get counts from keyword messages"""
    # Get context info
    if context:
        df = _get_messages_with_context(df, context)
    # Calculate thread length
    df = df.sort_values(['timestamp'])  # oldest first
    z = ((list(df['timestamp'])[-1] - list(df['timestamp'])[0]) / 86400)
    threadlen = f'{z:.1f}'
    # Create report
    kw_report = {
        'category': category,
        'keyword': keyword,
        'keyword_spelling': spelling,
        'context': context,
        # TODO: #1
        'num_messages_with_kw_spelling': len(df),
        'newest_message_datetime': format_timestamp(list(df['timestamp'])[-1]) if len(df) > 0 else None,
        'oldest_message_datetime': format_timestamp(list(df['timestamp'])[0]) if len(df) > 0 else None,
        'days_between_first_and_last_mention': threadlen,
        'query_date': today
    }
    return kw_report


def create_report1(
    df: pd.DataFrame, category_keywords: TYPE_KEYWORDS_DICT, kw_contexts: Dict[str, List[str]]
) -> (pd.DataFrame, pd.DataFrame):
    """Report 1: counts and latest/oldest message timestamps"""
    reports: List[Dict] = []
    no_result_keywords: List[str] = []
    today = str(date.today())
    for c, keywords in category_keywords.items():
        for k, spellings in keywords.items():
            contexts = kw_contexts.get(k, [])
            # add null context: needed to capture messages where no context words appear
            contexts = contexts + [''] if '' not in contexts else contexts
            for s in spellings:
                df_i = df[df['keyword_spelling'] == s]
                for context in contexts:
                    kw_report: Dict = _get_counts_from_kw_messages(
                        df=df_i, category=c, keyword=k, spelling=s, today=today, context=context)
                    if kw_report['num_messages_with_kw_spelling'] == 0:
                        no_result_keywords.append(k)
                    reports.append(kw_report)

    # Report
    df_report = pd.DataFrame(reports)
    df_report = format_df(df_report)

    # No results report
    df_no_results = pd.DataFrame()
    df_no_results['keywords_with_no_results'] = no_result_keywords

    # Save & return
    df_report.to_csv(CONFIG['outpath_report1'], index=False)
    if len(df_no_results) > 0:
        df_no_results.to_csv(CONFIG['outpath_no_results'], index=False)

    return df_report, df_no_results


def create_report2(
    df: pd.DataFrame, category_keywords: TYPE_KEYWORDS_DICT, kw_contexts: Dict[str, List[str]]
) -> (pd.DataFrame, pd.DataFrame):
    """Report 2: thread lengths"""
    seconds_per_day = 86400
    reports: List[Dict] = []
    today = date.today()
    for category, keywords in category_keywords.items():
        for k, spellings in keywords.items():
            contexts = kw_contexts.get(k, [])
            # add null context: needed to capture messages where no context words appear
            contexts = contexts + [''] if '' not in contexts else contexts
            for s in spellings:
                df_i = df[df['keyword_spelling'] == s]
                for context in contexts:
                    # Get context info
                    if context:
                        df_i = _get_messages_with_context(df_i, context)
                    df_i = df_i.sort_values(['timestamp'])  # oldest first
                    threads: List[str] = list(df_i['subject'].unique())
                    # Average thread length
                    # TODO: Refactor to pandas?
                    tot_thread_len = 0
                    thread_data: Dict[str, pd.DataFrame] = {}
                    for thread in threads:
                        df_thread = df_i[df_i['subject'] == thread]
                        # TODO: Want to double check that timestamps are still/indeed sorted properly (i) here, and
                        #  (ii) everywhere else where we're doing timestamps like this
                        # TODO: better: rather than get the first & last timestamp. should be able to get max() & min()
                        thread_len = (list(df_thread['timestamp'])[-1] -
                                      list(df_thread['timestamp'])[0]) / seconds_per_day
                        tot_thread_len += float(f'{thread_len:.1f}')
                        thread_data[thread] = df_thread
                    avg_thread_len = round(tot_thread_len / len(threads), 3)
                    # Outliers
                    # TODO: Refactor to pandas to reduce lines and improve performance?
                    # TODO: Add cols for 1 and 2 std deviations?
                    # TODO: Might need to make 3 columns for these outliers: std deviations away from (i) keyword avg,
                    #  (ii) category avg, (iii) avg of all of our queried category/keyword threads.
                    # Calc std deviation
                    sum_square_distance = 0
                    for thread in threads:
                        df_thread = thread_data[thread]
                        thread_len = (list(df_thread['timestamp'])[-1]
                                      - list(df_thread['timestamp'])[0]) / seconds_per_day
                        sum_square_distance += (float(thread_len) - float(avg_thread_len)) ** 2
                    stddev_kw_threads = math.sqrt(sum_square_distance / len(threads))
                    # Calc how many std deviations away per thread
                    for thread in threads:
                        outlier = False
                        df_thread = thread_data[thread]
                        thread_len = (list(df_thread['timestamp'])[-1]
                                      - list(df_thread['timestamp'])[0]) / seconds_per_day
                        if thread_len > stddev_kw_threads + avg_thread_len \
                                or thread_len < avg_thread_len - stddev_kw_threads:
                            outlier = True
                        # Calc URL
                        t = dict(df_thread.iloc[0])  # representative row of whole df; all values should be same
                        url = 'https://chat.fhir.org/#narrow/' + \
                              f'{t["type"]}/{t["stream_id"]}-{t["display_recipient"]}' + f'/topic/{t["subject"]}'
                        # Append to report
                        kw_report = {
                            'category': category,
                            'keyword': k,
                            'keyword_spelling': s,
                            'context': context,
                            'avg_thread_len': str(avg_thread_len),
                            'thread_name': thread,
                            'thread_length_days': f'{thread_len:.1f}',
                            'thread_stddev_from_kw_avg_thread_len': '1+' if outlier else '0',
                            'thread_url': url,
                            'query_date': today
                        }
                        reports.append(kw_report)

    df_report = pd.DataFrame(reports)
    df_report = format_df(df_report)

    # Save & return
    df_report.to_csv(CONFIG['outpath_report2'], index=False)

    return df_report


# TODO: In order to account for the possibility that people could edit their prior messages, can add as a param to this
#  ...function "account_for_edits"=True, or "cache"=False
#  ...People like Peter Jordan are constantly editing their messages =P
# TODO: Add "as_of" field with today's date at the time we ran this script.
# TODO: spelling variations: v2 and V2 should be the same count, "Terminology Service" and
#  "Terminology Svc", "$lookup" and "lookup operation(s)", etc.
# TODO: keyword_variations: consider adding column to spreadsheet and using e.g. V2 variations would be "V2, Version 2"
def query_categories(
    category_keywords: TYPE_KEYWORDS_DICT, msg_cache_path=CONFIG['results_cache_path']
) -> pd.DataFrame:
    """Get all dictionaries"""
    # Load cache
    cache_df = _load_cached_messages(msg_cache_path)

    # Fetch data for all keywords for all categories
    new_messages: List[Dict] = []
    errors: List[Dict[str, str]] = []
    for category, keyword_spellings in category_keywords.items():
        for k, spellings in keyword_spellings.items():
            for spelling in spellings:
                # Get latest message ID from cache
                last_msg_id = 0  # Zulip API: 0 means no last message ID
                if len(cache_df) > 0:
                    cache_df_i = cache_df[cache_df['keyword_spelling'] == spelling]
                    if len(cache_df_i) > 0:
                        last_msg_id = list(cache_df_i['id'])[-1]
                # Raw messages from Zulip API
                kw_messages, error = query_keyword(keyword=spelling, anchor=last_msg_id)
                # Combine Zulip results w/ additional info
                kw_messages = [
                    {**x, **{'category': category, 'keyword': k, 'keyword_spelling': spelling}} for x in kw_messages]
                # Edge case handling
                new_messages += _handle_keyword_edge_cases(spelling, kw_messages)
                # Error report
                errors += [{'keyword_spelling': spelling, 'error_message': error}] if error else []

    # Save outputs
    # - raw messages
    df_raw_new = pd.DataFrame(new_messages)
    df_raw_new = format_df(df_raw_new)  # todo: this may not be necessary; remove?
    df_raw = pd.concat([cache_df, df_raw_new])
    df_raw = format_df(df_raw)
    df_raw.to_csv(CONFIG['outpath_raw_results'], index=False)
    df_raw.to_csv(msg_cache_path, index=False)
    # - errors
    if errors:
        df_errors = pd.DataFrame(errors)
        df_errors.to_csv(CONFIG['outpath_errors'], index=False)

    return df_raw


def _get_keywords(use_cached_keyword_inputs=False) -> TYPE_KEYWORDS_DICT:
    """Get keywords iterable"""
    df: pd.DataFrame = _load_keywords_df(use_cache_only=use_cached_keyword_inputs)
    # Convert to dictionary
    category_keywords: TYPE_KEYWORDS_DICT = {}
    categories = list(df['category'].unique())
    for c in categories:
        category_keywords[c] = {}
        df_i = df[df['category'] == c]
        for _index, row in df_i.iterrows():
            kw = row['keyword']
            spellings_str: str = row['spelling_variations']
            spellings_list: List[str] = spellings_str.split(INTRA_CELL_DELIMITER) if spellings_str else [kw]
            category_keywords[c][kw] = spellings_list

    return category_keywords


def _get_keyword_contexts(use_cached_keyword_inputs=False) -> Dict[str, List[str]]:
    """Get keyword contexts
    If keyword has no contexts, will not be included in the dictionary."""
    df: pd.DataFrame = _load_keywords_df(use_cache_only=use_cached_keyword_inputs)
    # Convert to dictionary
    keyword_contexts: Dict[str, List[str]] = {}
    for _index, row in df.iterrows():
        kw = row['keyword']
        context_str: str = row['context']
        context_list: List[str] = context_str.split(INTRA_CELL_DELIMITER) if context_str else []
        if context_list:
            keyword_contexts[kw] = context_list

    return keyword_contexts


def run(analyze_only=False, use_cached_keyword_inputs=False):
    """Run program"""
    keywords: TYPE_KEYWORDS_DICT = _get_keywords(use_cached_keyword_inputs)
    message_df: pd.DataFrame = query_categories(keywords) if not analyze_only else _load_cached_messages()
    kw_contexts: Dict[str, List[str]] = _get_keyword_contexts()
    # - report 1: counts and latest/oldest message timestamps && keywords w/ no results
    create_report1(df=message_df, category_keywords=keywords, kw_contexts=kw_contexts)
    # - report 2: thread lengths
    create_report2(df=message_df, category_keywords=keywords, kw_contexts=kw_contexts)


def cli():
    """Command line interface."""
    package_description = 'NLP analysis of https://chat.fhir.org'
    parser = ArgumentParser(description=package_description)
    parser.add_argument(
        '-a', '--analyze-only', required=False, action='store_true',
        help='If present, will perform analysis, but do no new queries.')
    parser.add_argument(
        '-c', '--use-cached-keyword-inputs', required=False, action='store_true',
        help='If present, will not check GoogleSheets for updates to keyword related input data.')
    kwargs = parser.parse_args()
    kwargs_dict: Dict = vars(kwargs)
    run(**kwargs_dict)


# Execution
if __name__ == '__main__':
    cli()
