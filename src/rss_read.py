import os
import queue
import threading
import time
from datetime import timedelta, datetime
from threading import Lock

import feedparser
import numpy as np
import pandas as pd
from selenium import webdriver
# from selenium.webdriver import FirefoxOptions as Options
from selenium.webdriver import ChromeOptions as Options

file_lock = Lock()
webdriver_lock = Lock()
webdriver_path = [""]
output_directory = [""]
sources_path = [""]

update_log = queue.Queue()
error_log = queue.Queue()


def dequeue_logs(log_queue, log_func, file_path):
    while True:
        item = log_queue.get()
        log_func(item, file_path)
        log_queue.task_done()


def Batch(rss_url):
    feed = feedparser.parse(rss_url)

    if feed.bozo:
        if 'https://www.feedspot.com/infiniterss.php' in rss_url:
            publish_dates, titles, summaries = extractFeedspotHeadlines(rss_url)
        else:
            publish_dates, titles, summaries = [], [], []
    else:
        entries = [EntryInfo(x) for x in feed.entries]
        publish_dates = np.array([ei.publish_date for ei in entries])
        titles = np.array([ei.title for ei in entries])
        summaries = np.array([ei.summary for ei in entries])

    return publish_dates, titles, summaries


class EntryInfo(object):
    def __init__(self, entry: feedparser.FeedParserDict):
        self.entry = entry
        if 'published_parsed' not in entry.keys():
            date = datetime.now()
            date -= timedelta(seconds=date.second, microseconds=date.microsecond)
            published_parsed = date.timetuple()
            del date
        else:
            published_parsed = entry['published_parsed']
        title = entry['title']

        summary = entry['summary'] if 'summary' in entry else ""
        summary = summary.replace('<div style=""clear: both; padding-top: 0.2em;"">"', "").replace(
            '<div class=\"\"fbz_enclosure\"\" style=\"\"clear: left;\"\">\"', "")

        if '<img ' in summary:
            summary = summary[:summary.index('<img ')]
        if '\n' in summary:
            summary = summary[:summary.index("\n")]
        if "<" in summary:
            summary = summary[:summary.rindex("<")]
        self.title, self.summary = title, summary
        self.publish_date = datetime(*published_parsed[:6])

    def __str__(self):
        return "{0}\n{1}\n{2}".format(self.title, self.summary, self.publish_date)

    def __repr__(self):
        return str(self)


def extractFeedspotHeadlines(rss_url):
    """
    Gets headlines and dates from feedspot.com sources
    :param rss_url: string
    :return: numpy array of Entries
    """
    webdriver_lock.acquire()
    try:
        ops = Options()
        # ops.add_argument('--headless')

        # driver = webdriver.Firefox(executable_path=webdriver_path[0], options=ops, service_log_path='NUL')
        driver = webdriver.Chrome(executable_path=webdriver_path[0], options=ops, service_log_path='NUL')
        driver.get(rss_url)
        time.sleep(10 * np.random.random() + 5)
        titles = np.array([x.get_attribute('innerHTML') for x in driver.find_elements_by_class_name('ext_link') if
                           len(x.get_attribute('innerHTML'))])
        summaries = np.array([x[:x.index(' ..<a rel=')] for x in
                              [x.get_attribute('innerHTML') for x in driver.find_elements_by_class_name('fs_entry_desc')]])
        publish_dates = np.array([parse_datetime(x) for x in driver.find_elements_by_class_name('oc-sb')])
        driver.close()
    finally:
        webdriver_lock.release()
    return publish_dates, titles, summaries


def parse_datetime(element):
    ago = [x for x in [x for x in element.get_attribute("innerHTML").split("-") if len(x) > 0
                       and "ago" in x][0].split(" ") if len(x) > 0 and "\n" not in x and
           "\t" not in x][0]

    date = datetime.now()
    date -= timedelta(seconds=date.second, microseconds=date.microsecond)
    index_str = "".join([x for x in ago if x.isalpha()])
    if index_str in 'd h m'.split(" "):
        index = 'd h m'.split(" ").index(index_str)
        num = int("".join([x for x in ago if x.isalnum() and not x.isalpha()]))
        if index == 0:
            date -= timedelta(days=num, hours=date.hour, minutes=date.minute)
        elif index == 1:
            date -= timedelta(hours=num, minutes=date.minute)

        elif index == 2:
            date -= timedelta(minutes=num)
        else:
            error_log.put((datetime.now(), "Feedspot Source. Exact source unknown.",
                           "parse_datetime error: Unknown Date: \"{0}\" Using datetime.now() instead".format(ago)))
    elif index_str == "M":
        num = int("".join([x for x in ago if x.isalnum() and not x.isalpha()]))
        if date.month - num > 0:
            date = datetime(year=date.year, month=date.month - 1, day=date.day)
        else:

            date = datetime(year=date.year - 1, month=int(12 - abs(date.month - num)), day=date.day)
    return date

month_dict = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August',
              9: 'September', 10: 'October', 11: 'November', 12: 'December'}
def get_fname(source_str, pub_year=None, pub_month=None, most_recent_batch=False, ignore_month=True):
    """
    :param source_str: Name of source
    :param pub_year: Year designation, if not a Last Batch File
    :param pub_month: The month of the last batch file (currently ignored)
    :param most_recent_batch: If true, will ignore year and will return file name for Last Batch
    :param ignore_month: Defaults to True, will make separate files and folders for each month if false
    :return: The requested file name, either the year file name for a source or the Last Batch file name
    """
    if not most_recent_batch:
        year_folder = os.path.abspath('{0}/Saved Output/{1}'.format(output_directory[0], pub_year))
        month_folder = '{0}/{1}'.format(year_folder, month_dict[pub_month])
        filename = "{0}/{1}.csv".format(year_folder, source_str) if ignore_month else "{0}/{1}.csv".format(month_folder, source_str)
        if not (os.path.exists(year_folder) and os.path.isdir(year_folder)):
            file_lock.acquire()
            os.mkdir(year_folder)
            if not ignore_month:
                os.mkdir(month_folder)
            file_lock.release()
        elif not (os.path.exists(month_folder) and os.path.isdir(month_folder)) and not ignore_month:
            file_lock.acquire()
            os.mkdir(month_folder)
            file_lock.release()

        return filename
    # return os.path.abspath("{0}/Saved Output/{1} {2}.csv".format(output_directory[0], pub_year, source_str))
    else:
        return os.path.abspath("{0}/Last Batch/Last Batch {1}.csv".format(output_directory[0], source_str))


def insert_batch(source_name, untrimmed_dates: np.ndarray, untrimmed_titles: np.ndarray,
                 untrimmed_summaries: np.ndarray):
    """
    Goes through each item in the batch and groups the items by their publish year.
    Then inserts them into the proper file
    :param source_name: Name of news source
    :param dates
    :param titles
    :param summaries
    :return: Number of new sources
    """

    dates, titles, summaries, overlap, download_times = trim_batch(source_name, untrimmed_dates, untrimmed_titles,
                                                                   untrimmed_summaries)

    if 0 in [len(dates), len(titles), len(summaries)]:
        return 0, len(overlap)
    # Sometimes the length of overlap does not equal the length of download_times. In this event, just use the current
    # time
    if len(overlap) != len(download_times):
        download_times = np.array([datetime.now()] * len(overlap))
    years = np.unique(np.vectorize(lambda x: x.year)(dates))
    year_mask = lambda year: (dates >= datetime(year, 1, 1)) & (dates < datetime(year + 1, 1, 1))

    num_new_sources = 0
    for year in years:
        mask = year_mask(year)

        year_dates, year_titles, year_summaries = dates[mask], titles[mask], summaries[mask]
        months = np.unique(np.vectorize(lambda x: x.month)(dates))
        month_mask = lambda month: (year_dates >= datetime(year, month, 1)) & (year_dates < datetime(year if month != 12 else year + 1, month + 1 if month != 12 else 1, 1))
        for month in months:
            mask = month_mask(month)
            batch_dates, batch_titles, batch_summaries = year_dates[mask], year_titles[mask], year_summaries[mask]
            fname = get_fname(source_name, year, month)

            df = pd.DataFrame({'DATE': batch_dates, 'TITLE': batch_titles, 'SUMMARY': batch_summaries})
            if len(df) > 0:
                num_new_sources += len(df)
                if not os.path.isfile(fname):
                    df.to_csv(fname, index=False)
                else:
                    df.to_csv(fname, header=False, index=False, mode='a')
        del year, df, fname, batch_dates, batch_titles, batch_summaries, month, year_dates, year_titles, year_summaries
    # Now to write the Last Batch file for the next check
    # if there was any overlap, those overlapping rows need to be in the Last Batch file because there is
    # risk of them being read again on the next read
    if len(overlap) > 0:
        last_batch = (np.concatenate([dates, np.take(untrimmed_dates, overlap)]),
                      np.concatenate([titles, np.take(untrimmed_titles, overlap)]),
                      np.concatenate([summaries, np.take(untrimmed_summaries, overlap)]),
                      np.concatenate([np.full(dates.shape, datetime.now()), download_times]))
        del dates, titles, summaries, download_times
        dates, titles, summaries, download_times = last_batch
        del last_batch
    else:
        download_times = np.full(dates.shape, datetime.now())
    possible_overlap_batch = pd.DataFrame({'DOWNLOAD_TIME': pd.Series(download_times, dtype=download_times.dtype),
                                           'DATE': pd.Series(dates, dtype=dates.dtype), 'TITLE': titles,
                                           'SUMMARY': summaries})
    possible_overlap_batch.index.name = 'INDEX'
    if len(possible_overlap_batch) > 0:
        possible_overlap_batch.to_csv(get_fname(source_name, most_recent_batch=True))
    return num_new_sources, len(overlap)


def trim_batch(source_name, dates: np.ndarray, titles: np.ndarray, summaries: np.ndarray):
    """
    Returns copies without overlap values from dates, titles, and summaries as well as the indecies of the overlaps
    and the download_times of the overlaps.
    Overlaps being overlaps between the downloaded RSS feed and the last batch downloaded.
    :param source_name
    :param dates
    :param titles
    :param summaries
    :returns trimmed dates, trimmed titles, trimmed summaries, overlap, download_times
    """
    last_batch_fname = get_fname(source_name, most_recent_batch=True)
    if not os.path.isfile(last_batch_fname):
        return dates, titles, summaries, np.array([]), np.array([])
    # title_indecies = {title: i for title, i in zip(titles, range(len(titles)))}
    title_skip_rows, summary_skip_rows = set(), set()
    title_index_skip_rows = {}
    summary_index_skip_rows = {}
    chunk_size = min(max(len(dates), 25), 50)


    for col_name, col, skip_rows, idx_skip_rows in [('TITLE', titles, title_skip_rows, title_index_skip_rows),
                                                    ('SUMMARY', summaries, summary_skip_rows, summary_index_skip_rows)]:
        range_arr = np.array(range(len(col)))
        col_dict = {x: set(range_arr[col == x]) for x in set(col)}
        with pd.read_csv(get_fname(source_name, most_recent_batch=True), chunksize=chunk_size, usecols=['INDEX', col_name],
                         index_col='INDEX',
                         low_memory=True) as reader:
            for chunk in reader:
                for i in range(len(chunk)):
                    item = chunk[col_name][i + chunk.index[0]]
                    if isinstance(item, float) and np.isnan(item) and "" in col_dict and len(col_dict[""]) > 0:
                        skip_rows.update(col_dict[""])
                        idx_skip_rows[chunk.index[i]] = col_dict[""]
                    elif isinstance(item, str) and item in col_dict and len(col_dict[item]) > 0:
                        skip_rows.update(col_dict[item])
                        idx_skip_rows[chunk.index[i]] = col_dict[item]

            reader.close()
            del reader, col_dict, range_arr, item

    overlap = title_skip_rows  # .intersection(summary_skip_rows)
    idx_overlap = {x for x in set(title_index_skip_rows.keys()).intersection(set(summary_index_skip_rows.keys())) if
                   set(title_index_skip_rows[x]) == set(summary_index_skip_rows[x])}
    # idx_overlap = set(title_index_skip_rows.keys())
    del title_skip_rows, summary_skip_rows, skip_rows, idx_skip_rows, title_index_skip_rows, summary_index_skip_rows
    if len(overlap) == 0:
        return dates, titles, summaries, np.array([]), np.array([])

    overlap = np.sort(list(overlap))
    new_dates, new_titles, new_summaries = np.delete(dates, overlap), np.delete(titles, overlap), np.delete(summaries,
                                                                                                            overlap)
    download_times = []
    with pd.read_csv(get_fname(source_name, most_recent_batch=True), chunksize=chunk_size, usecols=['INDEX', 'DOWNLOAD_TIME'],
                     index_col='INDEX', low_memory=True) as reader:
        for chunk in reader:
            download_times.append(chunk['DOWNLOAD_TIME'][[x in idx_overlap for x in chunk.index]].values)
    download_times = np.concatenate(download_times)
    return new_dates, new_titles, new_summaries, overlap, download_times


def check_source(source_name, rss_feed):
    dates, titles, summaries = Batch(rss_feed)
    num_new_sources = insert_batch(source_name, dates, titles, summaries)
    return num_new_sources


def run_thread(source_name, rss_url, interval):
    while True:
        try:
            new_sources, num_overlap = check_source(source_name, rss_url)
            update_log.put((datetime.now(), source_name, new_sources, num_overlap))
        except Exception as error:
            error_log.put((datetime.now(), source_name,
                           error))  # "{0} FAILED: {1} : Error Message: {2}".format(datetime.now(), source_name, error))
            time.sleep(10)
            continue
        time.sleep(interval)


def log_data(variables, file_path):
    f = open(file_path, "a")
    f.write("\t".join([str(x) for x in variables]) + "\n"
            )
    f.close()



def chunk_list(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out


def main():
    f = open("../config.txt", "r")
    lines = f.read().split("\n")
    f.close()
    lines = [os.path.abspath(x) for x in lines]
    webdriver_path[0] = lines[0]
    output_directory[0] = lines[1]
    sources_path[0] = lines[2]
    update_log_path = lines[3]
    error_log_path = lines[4]

    # Set up saved output folder
    if not os.path.isdir("{0}/Saved Output".format(output_directory[0])):
        os.mkdir("{0}/Saved Output".format(output_directory[0]))
    # Set up last batch folder
    if not os.path.isdir("{0}/Last Batch".format(output_directory[0])):
        os.mkdir("{0}/Last Batch".format(output_directory[0]))

    # Setup update log and error log
    updater_thread = threading.Thread(target=dequeue_logs, args=(update_log, log_data, update_log_path), daemon=True)
    updater_thread.start()
    error_thread = threading.Thread(target=dequeue_logs, args=(error_log, log_data, error_log_path), daemon=True)
    error_thread.start()

    if not os.path.isfile(update_log_path):
        update_log.put("DATE,SOURCE,NEW,OVERLAP".split(","))
    if not os.path.isfile(error_log_path):
        error_log.put("DATE,SOURCE,MESSAGE".split(","))

    # Start threads for each source
    sources = pd.read_csv(sources_path[0])

    [threading.Thread(target=run_thread,
                      args=(sources['NAME'][i], sources['RSS_URL'][i], sources['PUBLISH_FREQUENCY'][i])).start() for i
     in range(len(sources))]


if __name__ == '__main__':
    main()
