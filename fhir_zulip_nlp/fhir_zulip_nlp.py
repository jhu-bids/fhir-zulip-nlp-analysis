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
  5. User roles google sheet:
     https://docs.google.com/spreadsheets/d/1OB0CEAkOhVTN71uIhzCo_iNaiD1B6qLqL7uwil5O22Q/edit#gid=1504038457

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
from copy import copy
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Union

import zulip
import matplotlib.pyplot as plt
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
# TODO: change CSV names to clarify that `zulip_raw_results.csv` is only for queried / matched messages
RAW_RESULTS_MATCHES_FILENAME = 'zulip_raw_results.csv'
RAW_RESULTS_ALL_FILENAME = 'zulip_raw_results_all.csv'
CONFIG = {
    'zuliprc_path': os.path.join(ENV_DIR, '.zuliprc'),  # rc = "runtime config"
    'streams': ['terminology'],
    'num_messages_per_query': 1000,
    'outpath_user_info': os.path.join(PROJECT_DIR, 'zulip_user_info.csv'),
    'outpath_report_counts': os.path.join(PROJECT_DIR, 'zulip_report1_counts.csv'),
    'outpath_report_thread_length': os.path.join(PROJECT_DIR, 'zulip_report2_thread_lengths.csv'),
    'outpath_report_users': os.path.join(PROJECT_DIR, 'zulip_report3_users.csv'),
    'outpath_report_roles': os.path.join(PROJECT_DIR, 'zulip_report4_user_roles.csv'),  # todo
    'outpath_errors': os.path.join(PROJECT_DIR, 'zulip_errors.csv'),
    'outpath_no_results': os.path.join(PROJECT_DIR, 'zulip_report_queries_with_no_results.csv'),
    'outpath_raw_results_messages_matches': os.path.join(PROJECT_DIR, RAW_RESULTS_MATCHES_FILENAME),
    'outpath_raw_results_messages_all': os.path.join(PROJECT_DIR, RAW_RESULTS_ALL_FILENAME),
    'outpath_raw_results_user_participation': os.path.join(PROJECT_DIR, 'zulip_raw_results_user_participation.csv'),
    'results_cache_path_messages_matches': os.path.join(CACHE_DIR, RAW_RESULTS_MATCHES_FILENAME),
    'results_cache_path_messages_all': os.path.join(CACHE_DIR, RAW_RESULTS_ALL_FILENAME),
    'keywords_cache_path': os.path.join(CACHE_DIR, 'keywords.csv'),
}
CLIENT = zulip.Client(config_file=CONFIG['zuliprc_path'])


# Functions
def format_timestamp(timestamp: int):
    """Format datetime string"""
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def _get_list_from_delimited_cell_string(s, delimiter=INTRA_CELL_DELIMITER):
    """From a single dataframe cell that contains a delmiter, convert that string to a list."""
    return [x.strip() for x in s.split(delimiter)]


def _load_cached_messages(msg_cache_path: str):
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
    """Get keywords data from GoogleSheets, else cache
        sheet_name: The name of the specific sheet within a GoogleSheet workbook."""
    if use_cache_only:
        df: pd.DataFrame = pd.read_csv(cache_path).fillna('')
    else:
        # PyBroadException: Broad because (a) don't know what failure returns now nor (b) might return in future, and
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
            if 'invalid_grant' in str(err):
                raise RuntimeError(
                    'GoogleSheets token will automatically be refreshed the next time this program is run. You can '
                    'simply run again immediately.')
            df: pd.DataFrame = pd.read_csv(cache_path).fillna('')

    # Massage
    df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

    return df


def message_pull(
    anchor: Union[int, str], num_before: int, num_after: int, stream: str = None, keyword: str = None
) -> Dict:
    """Get messages: https://zulip.com/api/get-messages"""
    request = {
        "anchor": anchor if anchor != 0 else 'oldest',
        "num_before": num_before,
        "num_after": num_after,
        "narrow": []}
    if keyword:
        request['narrow'].append({"operator": "search", "operand": keyword})
    if stream:
        request['narrow'].append({"operator": "stream", "operand": stream})
    result = CLIENT.get_messages(request)
    return result


def query_handler(
    keyword: str = None, stream: str = None, anchor: int = 0, num_after: int = CONFIG['num_messages_per_query']
) -> (List[Dict], Optional[str]):
    """Fetch data from API
    anchor: Integer message ID to anchor fetching of new messages. Supports special string values too; to learn more,
      see: https://zulip.com/api/get-messages
    num_after: The number of messages with IDs greater than the anchor to retrieve. Max: 5000. Recommended: <=1000."""
    err_msg = None
    messages: List[Dict] = []
    try:
        while True:
            res: Dict = message_pull(anchor=anchor, num_before=0, num_after=num_after, keyword=keyword, stream=stream)
            if 'retry-after' in res:  # rate limit, 200messages/user/minute
                wait_seconds_required: float = res['retry-after']
                time.sleep(wait_seconds_required * 1000)  # ms
                continue
            messages += res['messages']
            if not messages:
                break
            anchor = messages[-1]['id']  # returned messages are in chronological order; -1 is most recent in batch
            if res['found_newest']:  # this assumes API will reliably always return this
                break
    except Exception as err:
        err_msg = str(err)
        if keyword:
            print(f'Got error querying {keyword}. Original error: {err_msg}', file=sys.stderr)
            print(f'Stopping for "{keyword}" and continuing on with the next keyword.', file=sys.stderr)
        else:
            print(f'Error while querying. Exiting gracefully. Original error: {err_msg}', file=sys.stderr)

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
    """Filter out / etc. special edge cases in API responses"""
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


# todo: this could be a good place to use `nltk`
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
    threadlen = ''
    if len(df) > 0:
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


def create_report_counts(
    df: pd.DataFrame, category_keywords: TYPE_KEYWORDS_DICT, kw_contexts: Dict[str, List[str]]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Report: counts and latest/oldest message timestamps

    Warning: If this report is altered to show aggregate results (e.g. at the stream or category level), it is necessary
    to filter out duplicate threads that are matched by multiple keywords. For reference, this is done in the 'user
    participation' report, identifying threads by the combination of the 'stream' ('stream_id' or 'display_recipient')
    field and the 'thread name' (subject) field, and then de-duping.
    """
    reports: List[Dict] = []
    no_results: List[Dict] = []
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
                        no_results.append({'keyword': k, 'spelling': s, 'context': context, 'results': 0})
                    reports.append(kw_report)

    # Report
    df_report = pd.DataFrame(reports)
    df_report = format_df(df_report)

    # No results report
    df_no_results = pd.DataFrame(no_results)

    # Save & return
    df_report.to_csv(CONFIG['outpath_report_counts'], index=False)
    if len(df_no_results) > 0:
        df_no_results.to_csv(CONFIG['outpath_no_results'], index=False)

    return df_report, df_no_results


def create_report_thread_length(
    df: pd.DataFrame, category_keywords: TYPE_KEYWORDS_DICT, kw_contexts: Dict[str, List[str]],
    include_all_columns=False
) -> pd.DataFrame:
    """Report: thread lengths

    include_all_columns: Pending bugfix. If false, excludes these columns from report.

    Warning: If this report is altered to show aggregate results (e.g. message counts, average thread length, standard
    deviation of thread length) at the stream or category level), it is necessary to filter out duplicate threads that
    are matched by multiple keywords. For reference, this is done in the 'user participation' report, identifying
    threads by the combination of the 'stream' ('stream_id' or 'display_recipient') field and the 'thread name'
    (subject) field, and then de-duping.

    todo: fix category / all messages counts and stddev. Right now the counts are mostly 0; not being calculated based
     on the correct message selections. Not sure if there are stddev calc issues; could just be because of counts.
    """
    reports: List[Dict] = []
    seconds_per_day = 86400
    today = date.today()
    tot_all, std_all, num_all_threads = 0, 0, 0
    avg_total, std_tot = 0, 0
    for i, (category, keywords) in enumerate(category_keywords.items()):
        tot_category, var_category, avg_category, std_category, num_threads = 0, 0, 0, 0, 0

        for i2, (k, spellings) in enumerate(keywords.items()):
            contexts = kw_contexts.get(k, [])
            # add null context '': needed to capture messages where no context words appear
            contexts = contexts + [''] if '' not in contexts else contexts
            for spelling in spellings:
                df_i = df[df['keyword_spelling'] == spelling]
                for context in contexts:
                    df_j = _get_messages_with_context(df_i, context)
                    df_j = df_j.sort_values(['timestamp'])  # oldest first
                    threads: List[str] = list(df_j['subject'].unique())
                    # Average thread length
                    # TODO: Refactor to pandas?
                    tot_thread_len = 0
                    thread_data: Dict[str, pd.DataFrame] = {}
                    for thread in threads:
                        df_thread = df_j[df_j['subject'] == thread]
                        num_threads += 1
                        # TODO: Want to double check that timestamps are still/indeed sorted properly (i) here, and
                        #  (ii) everywhere else where we're doing timestamps like this
                        # TODO: better: rather than get the first & last, timestamp. should be able to get max() & min()
                        thread_len = (list(df_thread['timestamp'])[-1]
                                      - list(df_thread['timestamp'])[0]) / seconds_per_day
                        tot_thread_len += float(f'{thread_len:.1f}')
                        thread_data[thread] = df_thread
                        num_all_threads += 1
                        tot_all += thread_len
                    avg_len_kw_thread = round(tot_thread_len / len(threads), 3) if threads else 0
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
                        sum_square_distance += (float(thread_len) - float(avg_len_kw_thread)) ** 2
                    stddev_kw_threads = math.sqrt(sum_square_distance / len(threads)) if threads else 0
                    # Calc how many std deviations away per thread
                    tot_category += tot_thread_len
                    var_category += stddev_kw_threads ** 2
                    std_all += stddev_kw_threads ** 2
                    for i3, thread in enumerate(threads):
                        outlier = False
                        df_thread = thread_data[thread]
                        thread_len = (list(df_thread['timestamp'])[-1]
                                      - list(df_thread['timestamp'])[0]) / seconds_per_day
                        std_away = 0
                        if thread_len > stddev_kw_threads + avg_len_kw_thread \
                                or thread_len < avg_len_kw_thread - stddev_kw_threads:
                            outlier = True
                            std_away = abs(thread_len - avg_len_kw_thread) / stddev_kw_threads
                        if i3 == len(threads) - 1:
                            avg_category = round(tot_category / num_threads, 2)
                            std_category = round(math.sqrt(var_category / num_threads), 2)
                            num_threads = 0
                            tot_category = 0
                            var_category = 0
                            if i2 == len(list(keywords.keys())) - 1:
                                avg_total = round((tot_all / num_all_threads), 2)
                                std_tot = round(math.sqrt(std_all / num_all_threads), 2)
                        # Calc URL
                        thread_df = dict(df_thread.iloc[0])  # representative row of whole df; all values should be same
                        url = 'https://chat.fhir.org/#narrow/' + \
                              f'{thread_df["type"]}/{thread_df["stream_id"]}-{thread_df["display_recipient"]}' + \
                              f'/topic/{thread_df["subject"]}'
                        # Append to report
                        kw_report = {
                            'category': category,
                            'keyword': k,
                            'thread': thread,
                            'thread_url': url,
                            'thread_len_days': f'{thread_len:.1f}',
                            'avg_len_kw_outlier?': outlier,
                            'avg_len_kw': str(avg_len_kw_thread),
                            'stddev_kw': str(round(std_away, 2)),
                            'query_date': today,
                        }
                        if include_all_columns:
                            kw_report = {**kw_report, **{
                                'avg_len_category': avg_category,
                                'stddev_category': std_category,
                                'avg_len_total': str(avg_total),
                                'stddev_total': str(std_tot),
                            }}
                        avg_category, std_category, avg_total, std_tot = 0, 0, 0, 0
                        reports.append(kw_report)

    df_report = pd.DataFrame(reports)
    df_report = format_df(df_report)
    # Save & return
    df_report.to_csv(CONFIG['outpath_report_thread_length'], index=False)
    return df_report


def create_report_users(
    df_all: pd.DataFrame, df_matches: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """Report: Users"""
    # display_recipient: Data on the recipient of the message; either the name of a stream or a dictionary containing
    # basic data on the users who received the message. See: https://zulip.com/api/get-messages
    stream_name_field = 'display_recipient'

    # Add only messages from `df_all` that aren't in `df_matches`
    # - Create common primary key
    for df_i in [df_all, df_matches]:
        df_i['thread.id'] = df_i.apply(lambda x: x['display_recipient'] + x['subject'], axis=1)
    # - Split unmatched messages: (i) share a common thread in `df_matches`, and (ii) share no common thread
    existing_matched_msg_ids = set(df_matches['id'])
    existing_matched_thread_ids = set(df_matches['thread.id'])
    df_all_related_rows = []
    # TODO: If requested, we can use 'unrelated' data; that is, messages that did not match any of our keywords, nor did
    #  ...they appear in any threads containing any messages that matched any of our keywords.
    # df_all_unrelated_rows = []
    for _index, row in df_all.iterrows():
        if row['id'] not in existing_matched_msg_ids and row['thread.id'] in existing_matched_thread_ids:
            df_all_related_rows.append(row)
        # else:
        #     df_all_unrelated_rows.append(row)
    df_all_related = pd.DataFrame(df_all_related_rows)
    # df_all_unrelated = pd.DataFrame(df_all_unrelated_rows)

    # Get basic user data
    matched_user_ids: List[int] = list(df_matches['sender_id'].unique())
    df_combined = pd.concat([df_all, df_matches])
    user_ids: List[int] = list(df_combined['sender_id'].unique())
    user_data: Dict[int, Dict[str, Any]] = {}
    for user_id in user_ids:
        df_i = df_combined[df_combined['sender_id'] == user_id].iloc[0]
        user_data[user_id] = {
            'user_id': user_id,
            'full_name': df_i['sender_full_name'],
            'email': df_i['sender_email']
        }
    user_info_df = pd.DataFrame(user_data.values())
    user_info_df.to_csv(CONFIG['outpath_user_info'], index=False)

    # todo: I really don't like how repetitive this is
    # todo: This could also probably use a refactor after adding 'thread.id' logic; maybe via a thread-centric df.
    user_participation: List[Dict[str, Any]] = []
    df = df_matches  # assign to shorter variable for more readable code
    streams = df[stream_name_field].unique()
    for stream in streams:
        df_i = df[df[stream_name_field] == stream]
        categories = df_i['category'].unique()
        for c in categories:
            df_i2 = df_i[df_i['category'] == c]
            keywords = df_i2['keyword'].unique()
            for k in keywords:
                df_i3 = df_i2[df_i2['keyword'] == k]
                threads = df_i3['subject'].unique()
                for thread in threads:
                    df_i4 = df_i3[df_i3['subject'] == thread]
                    # Get all messages in thread, combining these matched messages w/ unmatched ones
                    df_thread_related = df_all_related[df_all_related['thread.id'] == stream + thread]
                    df_i4b = pd.concat([df_i4, df_thread_related]).fillna('')
                    # Get authorship vs non-authorship
                    participant_roles = {p: 'respondent' for p in df_i4b['sender_id'].unique()}
                    author_timestamp = min(df_i4b['timestamp'])
                    author_id: int = list(
                        df_i4b[df_i4b['timestamp'] == author_timestamp]['sender_id'].to_dict().values())[0]
                    participant_roles[author_id] = 'author'
                    # Populate: user_participation
                    for user_id, role in participant_roles.items():
                        row = {
                            'user.id': user_id,
                            'user.full_name': user_data[user_id]['full_name'],
                            'stream': stream,
                            'category': c,
                            'keyword': k,
                            'thread.name': thread,
                            'thread.id': stream + thread,
                            'user.role': role
                        }
                        user_participation.append(row)
    user_participation_df = pd.DataFrame(user_participation)
    user_participation_df.to_csv(CONFIG['outpath_raw_results_user_participation'], index=False)

    # TODO: I really don't like how repetitive this is; even worse than previous block
    # TODO: Have aggregated to keyword, category, and stream, but not to role agnostic of stream. Would be useful to add
    #  ...this, once streams feature is complete.
    user_participation_stats = []
    for user_id in matched_user_ids:
        df_i = user_participation_df[user_participation_df['user.id'] == user_id]
        streams = list(df_i['stream'].unique())
        for s in streams:
            df_i2 = df_i[df_i['stream'] == s]
            df_i2_unique_threads = copy(df_i2)
            df_i2_unique_threads = df_i2_unique_threads.drop_duplicates(subset='thread.id')
            role_counts: Dict[str, int] = df_i2_unique_threads['user.role'].value_counts().to_dict()
            for role, count in role_counts.items():
                row = {
                    'user.id': user_id,
                    'user.full_name': user_data[user_id]['full_name'],
                    'stream': s,
                    'category': '',
                    'keyword': '',
                    'thread.role': role,
                    'thread.count': count,
                }
                user_participation_stats.append(row)
            categories = list(df_i2['category'].unique())
            for c in categories:
                df_i3 = df_i2[df_i2['category'] == c]
                df_i3_unique_threads = copy(df_i3)
                df_i3_unique_threads = df_i3_unique_threads.drop_duplicates(subset='thread.id')
                role_counts: Dict[str, int] = df_i3_unique_threads['user.role'].value_counts().to_dict()
                for role, count in role_counts.items():
                    row = {
                        'user.id': user_id,
                        'user.full_name': user_data[user_id]['full_name'],
                        'stream': s,
                        'category': c,
                        'keyword': '',
                        'thread.role': role,
                        'thread.count': count,
                    }
                    user_participation_stats.append(row)
                keywords = list(df_i2['keyword'].unique())
                for k in keywords:
                    df_i4 = df_i3[df_i3['keyword'] == k]
                    role_counts: Dict[str, int] = df_i4['user.role'].value_counts().to_dict()
                    for role, count in role_counts.items():
                        row = {
                            'user.id': user_id,
                            'user.full_name': user_data[user_id]['full_name'],
                            'stream': s,
                            'category': c,
                            'keyword': k,
                            'thread.role': role,
                            'thread.count': count,
                        }
                        user_participation_stats.append(row)
    user_participation_stats_df = pd.DataFrame(user_participation_stats)
    user_participation_stats_df.to_csv(CONFIG['outpath_report_users'], index=False)

    return {
        'user_info': user_info_df,
        'user_participation': user_participation_df,
        'user_participation_stats': user_participation_stats_df
    }


def plot_category_msg_counts_by_year():
    """plot"""
    # create data
    df = pd.read_csv(CONFIG['outpath_raw_results_messages_matches']).fillna('')
    df['year'] = df['timestamp'].apply(
        lambda x: int(datetime.utcfromtimestamp(x).strftime('%Y')))

    # Iterate
    years = df['year'].unique()
    years.sort()
    categories = df['category'].unique()
    category_kw_year_counts = {}
    for c in categories:
        df_i = df[df['category'] == c]
        keywords = df_i['keyword'].unique()
        kw_year_counts= {}
        for kw in keywords:
            df_i2 = df_i[df_i['keyword'] == kw]
            year_counts = []
            for year in years:
                df_i3 = df_i2[df_i2['year'] == year]
                year_counts.append(len(df_i3))
            kw_year_counts[kw] = year_counts
        category_kw_year_counts[c] = kw_year_counts

    # Plot
    # TODO: save legend as separate file
    # todo: legend go on side (if more than 8-10 itmems only?)
    variations = [{'legend': False, 'line_labs': False}, {'legend': True, 'line_labs': True},
                  {'legend': True, 'line_labs': False}, {'legend': False, 'line_labs': True}]
    for v in variations:
        for c, kw_year_counts in category_kw_year_counts.items():
            fig, ax = plt.subplots()  #
            year_i = 0
            for kw, year_counts in kw_year_counts.items():
                ax.plot(years, year_counts, label=kw)
                if v['line_labs']:
                    # attempt 1
                    # ax.plot(years, year_counts)
                    # ax.lines[-1].set_label(kw)
                    # attempt 2
                    # line, = ax.plot(years, year_counts, label=kw)
                    # line.set_label(kw)
                    # attempt 3
                    # todo: how to get color?
                    ax.text(years[year_i], year_counts[year_i], kw)
                year_i = 0 if year_i == len(years) - 1 else year_i + 1
            if v['legend']:
                plt.legend()
            legend_str = '' if not v['legend'] else ' - legend'
            lab_str = '' if not v['line_labs'] else ' - line_labels'
            # legend_lab = '' if show_legend else ' - no legend'
            # ax.set_xlabel('Year')  # don't need; implicit
            ax.set_ylabel('n Messages')
            ax.set_title(c.replace('_', '').title())
            plt.savefig(os.path.join(PROJECT_DIR, f'kw year counts - {c}{legend_str}{lab_str}.png'))
            plt.close()


# TODO
# todo: add outpath to config
def plot_table_user_participation_counts(path) -> pd.DataFrame:
    """Create table"""
    df: pd.DataFrame = pd.read_csv(path).fillna('')
    # remove aggregation rows
    df = df[df['keyword'] != '']

    # iterate
    rows: List[Dict] = []
    users = df['user.full_name'].unique()
    for user in users:
        df_i = df[df['user.full_name'] == user]
        author_count = df_i[df_i['thread.role'] == 'author']['thread.count'].sum()
        respondent_count = df_i[df_i['thread.role'] == 'respondent']['thread.count'].sum()
        row = {
            'user.full_name': user,
            'author_count': author_count,
            'respondent_count': respondent_count,
            'tot_count': author_count + respondent_count,
        }
        rows.append(row)
    df2 = pd.DataFrame(rows)
    df2.to_csv('zulip_dataviz_table_user_activity.csv', index=False)
    return df2


def query_all_messages(
    streams=CONFIG['streams'], outpath=CONFIG['outpath_raw_results_messages_all'],
    cache_path=CONFIG['results_cache_path_messages_all']
):
    """Query all messages according to global `CONFIG`."""
    # Load cache
    cache_df: pd.DataFrame = _load_cached_messages(cache_path)

    # Fetch data for all keywords for all categories
    new_messages: List[Dict] = []
    errors: List[str] = []
    for stream in streams:
        # Get latest message ID from cache
        last_msg_id = 0  # Zulip API: 0 means no last message ID
        if len(cache_df) > 0:
            last_msg_id = list(cache_df['id'])[-1]
        # Raw messages from Zulip API
        kw_messages, error = query_handler(stream=stream, anchor=last_msg_id)
        new_messages += kw_messages
        if error:
            errors.append(error)

    # Save outputs
    # - raw messages
    # noinspection DuplicatedCode
    df_raw_new = pd.DataFrame(new_messages)
    df_raw = pd.concat([cache_df, df_raw_new])
    df_raw = df_raw.sort_values(['timestamp'], ascending=True)
    df_raw.to_csv(outpath, index=False)
    df_raw.to_csv(cache_path, index=False)
    # - errors
    if errors:
        df_errors = pd.DataFrame(errors)
        df_errors.to_csv(CONFIG['outpath_errors'], index=False, mode='a')

    return df_raw


# TODO: In order to account for the possibility that people could edit their prior messages, can add as a param to this
#  ...function "account_for_edits"=True, or "cache"=False
#  ...People like Peter Jordan are constantly editing their messages =P
# TODO: Add "as_of" field with today's date at the time we ran this script.
# TODO: spelling variations: v2 and V2 should be the same count, "Terminology Service" and
#  "Terminology Svc", "$lookup" and "lookup operation(s)", etc.
# TODO: keyword_variations: consider adding column to spreadsheet and using e.g. V2 variations would be "V2, Version 2"
def query_categories(
    category_keywords: TYPE_KEYWORDS_DICT, streams=CONFIG['streams'],
    outpath=CONFIG['outpath_raw_results_messages_matches'], cache_path=CONFIG['results_cache_path_messages_matches'],
) -> pd.DataFrame:
    """Get all dictionaries"""
    # Load cache
    cache_df: pd.DataFrame = _load_cached_messages(cache_path)

    # Fetch data for all keywords for all categories
    new_messages: List[Dict] = []
    errors: List[Dict[str, str]] = []
    for stream in streams:
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
                    kw_messages, error = query_handler(keyword=spelling, stream=stream, anchor=last_msg_id)
                    # Combine Zulip results w/ additional info
                    kw_messages = [
                        {**x, **{'category': category, 'keyword': k, 'keyword_spelling': spelling}}
                        for x in kw_messages]
                    # Edge case handling
                    new_messages += _handle_keyword_edge_cases(spelling, kw_messages)
                    # Error report
                    errors += [{'keyword_spelling': spelling, 'error_message': error}] if error else []

    # Save outputs
    # - raw messages
    # noinspection DuplicatedCode
    df_raw_new = pd.DataFrame(new_messages)
    df_raw_new = format_df(df_raw_new)  # todo: this may not be necessary; remove?
    df_raw = pd.concat([cache_df, df_raw_new])
    df_raw = format_df(df_raw)
    df_raw.to_csv(outpath, index=False)
    df_raw.to_csv(cache_path, index=False)
    # - errors
    if errors:
        df_errors = pd.DataFrame(errors)
        df_errors.to_csv(CONFIG['outpath_errors'], index=False, mode='a')

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
            spellings_list: List[str] = _get_list_from_delimited_cell_string(spellings_str) if spellings_str else [kw]
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
        context_list: List[str] = _get_list_from_delimited_cell_string(context_str) if context_str else []
        if context_list:
            keyword_contexts[kw] = context_list

    return keyword_contexts


def run(analyze_only=False, analytical_tables_only=False, use_cached_keyword_inputs=False):
    """Run program"""
    if not analytical_tables_only:
        # Get inputs
        keywords: TYPE_KEYWORDS_DICT = _get_keywords(use_cached_keyword_inputs)
        kw_contexts: Dict[str, List[str]] = _get_keyword_contexts()
        # Get messages
        messages_matches_df: pd.DataFrame = query_categories(keywords) if not analyze_only else \
            _load_cached_messages(CONFIG['results_cache_path_messages_matches'])
        messages_all_df: pd.DataFrame = query_all_messages() if not analyze_only else \
            _load_cached_messages(CONFIG['results_cache_path_messages_all'])
        # Create reports
        # TODO: Performance: thread_length and users too slow. Once faster, remove `analytical_tables_only` arg
        create_report_counts(df=messages_matches_df, category_keywords=keywords, kw_contexts=kw_contexts)
        create_report_thread_length(df=messages_matches_df, category_keywords=keywords, kw_contexts=kw_contexts)
        create_report_users(df_all=messages_all_df, df_matches=messages_matches_df)
    # Create analytical tables for data visualizations
    plot_table_user_participation_counts(CONFIG['outpath_report_users'])
    plot_category_msg_counts_by_year()


def cli():
    """Command line interface."""
    package_description = 'NLP analysis of https://chat.fhir.org'
    parser = ArgumentParser(description=package_description)
    parser.add_argument(
        '-a', '--analyze-only', required=False, action='store_true',
        help='If present, will perform analysis, but do no new queries.')
    parser.add_argument(
        '-p', '--plots-only', required=False, action='store_true',
        help='If present, will create plots and sometimes tables for those plots, but do no queries, '
             'nor create reports.')
    parser.add_argument(
        '-c', '--use-cached-keyword-inputs', required=False, action='store_true',
        help='If present, will not check GoogleSheets for updates to keyword related input data.')
    kwargs = parser.parse_args()
    kwargs_dict: Dict = vars(kwargs)
    run(**kwargs_dict)


# Execution
if __name__ == '__main__':
    cli()
