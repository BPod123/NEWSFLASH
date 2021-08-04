import os
import time
from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import feedparser
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver import FirefoxOptions as Options


def parse_batches(sources: pd.DataFrame):
    """
    Reads the RSS Feeds from different sources and converts the batches from each source to a dataframe
    :param sources:
    :return:
    """
    return np.array([Batch(sources, i) for i in range(len(sources))])


def Batch(sources: pd.DataFrame, index_row: int):
    source_name, rss_url = sources['NAME'][index_row], sources['RSS_URL'][index_row]
    # timetuple for time struct

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
        summary = entry['summary']
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
    f = open("../webdriverpath.txt", "r")
    webdriver_path = f.read()
    f.close()
    del f
    ops = Options()
    ops.add_argument('--headless')
    driver = webdriver.Firefox(executable_path=webdriver_path, options=ops)
    driver.get(rss_url)
    titles, summaries, publish_dates = [], [], []
    titles = np.array([x.get_attribute('innerHTML') for x in driver.find_elements_by_class_name('ext_link') if
                       len(x.get_attribute('innerHTML'))])
    summaries = np.array([x[:x.index(' ..<a rel=')] for x in
                          [x.get_attribute('innerHTML') for x in driver.find_elements_by_class_name('fs_entry_desc')]])
    publish_dates = np.array([parse_datetime(x) for x in driver.find_elements_by_class_name('oc-sb')])
    driver.close()
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
            print("Unknown Date:\n{0}".format(ago))
    return date

    #
    #
    # spans = element.get_attribute('innerHTML').split("<span")
    # target = [s for s in spans if " ago" in s][0]
    # target = [s for s in target.split("-")]


def get_fname(source_str, pub_year, most_recent_batch=False):
    if not most_recent_batch:
        return os.path.abspath("../output/{0} {1}.csv".format(pub_year, source_str))
    else:
        return os.path.abspath("../output/Last Batch {0}.csv".format(source_str))


def insert_batch(source_name, untrimmed_dates: np.ndarray, untrimmed_titles: np.ndarray,
                 untrimmed_summaries: np.ndarray):
    """
    Goes through each item in the batch and groups the items by their publish year.
    Then inserts them into the proper file
    :param source_name: Name of news source
    :param dates
    :param titles
    :param summaries
    :return:
    """

    dates, titles, summaries, overlap, download_times = trim_batch(source_name, untrimmed_dates, untrimmed_titles,
                                                                   untrimmed_summaries)

    if 0 in [len(dates), len(titles), len(summaries)]:
        return
    years = np.unique(np.vectorize(lambda x: x.year)(dates))
    year_mask = lambda year: (dates >= datetime(year, 1, 1)) & (dates < datetime(year + 1, 1, 1))

    for year in years:
        mask = year_mask(year)
        batch_dates, batch_titles, batch_summaries = dates[mask], titles[mask], summaries[mask]
        del mask

        fname = get_fname(source_name, year)

        df = pd.DataFrame({'DATE': batch_dates, 'TITLE': batch_titles, 'SUMMARY': batch_summaries})
        if not os.path.isfile(fname):
            df.to_csv(fname, index=False)
        else:
            df.to_csv(fname, header=False, index=False, mode='a')
        del year, df, fname, batch_dates, batch_titles, batch_summaries
    # Now to write the Last Batch file for the next check
    # if there was any overlap, those overlapping rows need to be in the Last Batch file because there is
    # risk of them being read again on the next read
    ovelap_len = len(overlap)
    if len(overlap) > 0:
        last_batch = (np.concatenate([dates, np.take(untrimmed_dates, overlap)]),
                      np.concatenate([titles, np.take(untrimmed_titles, overlap)]),
                      np.concatenate([summaries, np.take(untrimmed_summaries, overlap)]),
                      np.concatenate([np.full(dates.shape, datetime.now()), download_times]))
        del dates, titles, summaries, overlap, download_times
        dates, titles, summaries, download_times = last_batch
        del last_batch
    else:
        download_times = np.full(dates.shape, datetime.now())
    try:
        possible_overlap_batch = pd.DataFrame({'DOWNLOAD_TIME': pd.Series(download_times, dtype=download_times.dtype),
                                               'DATE': pd.Series(dates, dtype=dates.dtype), 'TITLE': titles,
                                               'SUMMARY': summaries})
    except:
        z = 3
    possible_overlap_batch.index.name = 'INDEX'
    possible_overlap_batch.to_csv(get_fname(source_name, None, most_recent_batch=True))


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
    last_batch_fname = get_fname(source_name, None, True)
    if not os.path.isfile(last_batch_fname):
        return dates, titles, summaries, np.array([]), np.array([])
    # title_indecies = {title: i for title, i in zip(titles, range(len(titles)))}
    title_skip_rows, summary_skip_rows = set(), set()
    title_index_skip_rows = set()
    summary_index_skip_rows = set()
    chunk_size = min(max(len(dates), 25), 50)

    for col_name, col, skip_rows, idx_skip_rows in [('TITLE', titles, title_skip_rows, title_index_skip_rows),
                                                    ('SUMMARY', summaries, summary_skip_rows, summary_index_skip_rows)]:
        range_arr = np.array(range(len(col)))
        col_dict = {x: set(range_arr[col == x]) for x in set(col)}
        with pd.read_csv(get_fname(source_name, None, True), chunksize=chunk_size, usecols=['INDEX', col_name],
                         index_col='INDEX',
                         low_memory=True) as reader:
            for chunk in reader:
                for i in range(len(chunk)):
                    item = chunk[col_name][i + chunk.index[0]]
                    if isinstance(item, float) and np.isnan(item) and "" in col_dict:
                        skip_rows.update(col_dict[""])
                        idx_skip_rows.add(chunk.index[i])
                    elif isinstance(item, str) and item in col_dict:
                        skip_rows.update(col_dict[item])
                        idx_skip_rows.add(chunk.index[i])

            reader.close()
            del reader, col_dict, range_arr, item

    # title_skip_rows = set(np.unique(np.concatenate(title_skip_rows)))
    # summary_skip_rows = set(np.unique(np.concatenate(summary_skip_rows)))
    # overlap = np.array(list(title_skip_rows.intersection(summary_skip_rows)))
    overlap = title_skip_rows.intersection(summary_skip_rows)
    idx_overlap = title_index_skip_rows.intersection(summary_index_skip_rows)
    del title_skip_rows, summary_skip_rows, skip_rows, idx_skip_rows, title_index_skip_rows, summary_index_skip_rows
    if len(overlap) == 0:
        return dates, titles, summaries, np.array([]), np.array([])

    overlap = np.sort(list(overlap))
    new_dates, new_titles, new_summaries = np.delete(dates, overlap), np.delete(titles, overlap), np.delete(summaries,
                                                                                                            overlap)
    download_times = []
    with pd.read_csv(get_fname(source_name, None, True), chunksize=chunk_size, usecols=['INDEX', 'DOWNLOAD_TIME'],
                     index_col='INDEX', low_memory=True) as reader:
        for chunk in reader:
            download_times.append(chunk['DOWNLOAD_TIME'][[x in idx_overlap for x in chunk.index]].values)
    download_times = np.concatenate(download_times)
    return new_dates, new_titles, new_summaries, overlap, download_times


def check_source(sources, i):
    dates, titles, summaries = Batch(sources, i)
    insert_batch(sources['NAME'][i], dates, titles, summaries)


if __name__ == '__main__':
    # TODO make separate threads for each source that sleep different amount of times
    # Also, updates Sources.csv to have the frequency at which sources are checked
    # When setting up the threads, run each batch update once, then schedule them to update at different times.

    sources = pd.read_csv('Sources.csv')
    while(True):
        start = datetime.now()
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(check_source, sources, i) for i in range(len(sources))]
            executor.shutdown()
        end = datetime.now()
        print("Download Completed at {0}. Time Taken: {1}".format(datetime.now(), end - start))
        del start, end
        time.sleep(10800) # 10,800 seconds in 3 hours
