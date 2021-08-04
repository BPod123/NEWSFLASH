import os
import mysql.connector
from selenium import webdriver
import os.path
from datetime import date, datetime, timedelta


def getDb():
    f = open(os.path.abspath('../mydbcredentials.txt'), "r")
    user, passwd = tuple(f.readlines())
    db = mysql.connector.connect(
        host='localhost',
        user=user,
        passwd=passwd,
        database='HeadlineDB'
    )
    del user, passwd
    return db


def updateAllHeadlines(webdriver_path):
    """
    Method to be called from elsewhere in the project.
    Will go through each source and add any new headlines to the database
    """
    db = getDb()
    curser = db.cursor()
    # noinspection SqlResolve
    curser.execute("SELECT * FROM SOURCE")
    sources = curser.fetchall()
    db.close()
    del curser, db

    # Get headlines for each source
    failed_retrievals = []  # list of sources whose headlines were not able to be obtained
    source_headlines = {}
    for source in sources:
        try:
            updateHeadlines(source, webdriver_path)
        except Exception as e:
            print("Failed to retrieve {0}".format(source))
            print(e)
            failed_retrievals.append(source)


def updateHeadlines(source, webdriver_path):
    headlines = retrieveHeadlines(source, webdriver_path)
    db = getDb()
    # Check the last several articles from this source to see if this article is a repeat.
    # Do not not want to store the same article multiple times if the rss feed did not update frequently enough
    curser = db.cursor()
    # noinspection SqlResolve
    curser.execute("""
    SELECT headline.headline 
    FROM HEADLINE AS headline 
    WHERE headline.source_id='{0}'
    ORDER BY headline.date DESC
    LIMIT {1}
    """.format(source[0], str(max(len(headlines) * 5, 50))))
    previous_hls = set(curser.fetchall())
    new_headlines = headlines.difference(previous_hls)
    # Create to update the database
    cmd_str = ""
    for hl in new_headlines:
        cmd_str += "INSERT INTO HEADLINE VALUES (uuid(), '{0}', '{1}', '{2}')".format(source[0], )










def retrieveHeadlines(source: tuple, webdriver_path: str):
    """
    Adds new headlines to the database
    :param source: Tuple (Name, rss_url)
    :param webdriver_path: path to the webdriver
    :return: list of headlines from the source
    """
    name, rss_url = source[1], source[2]
    driver = webdriver.Firefox(executable_path=webdriver_path)
    driver.get(rss_url)
    headlines = set()
    if 'feedspot.com' in rss_url:
        headlines = extractFeedspotHeadlines(driver)
    elif 'rss.cnn.com' in rss_url:
        headlines = extractRssCnnHeadlines(driver)
    else:
        headlines = set()

    driver.close()
    return headlines


def extractFeedspotHeadlines(driver):
    """
    Gets headlines and dates from feedspot.com sources
    :param driver: Selenium Webdriver
    :return: list of headlines
    """
    headlines = set()
    article_entries = driver.find_elements_by_class_name('rssitem')
    for article in article_entries:
        link = article.find_element_by_class_name('ext_link')
        source_publish_time_tag = article.find_element_by_class_name('oc-sb')

        headline = link.text
        try:
            time_strs = [x for x in source_publish_time_tag.text.split("-")[1].split(" ") if len(x) > 0]
            if 'ago' in time_strs:
                rel_words = time_strs[:time_strs.index('ago')]
                if len(rel_words) == 1:
                    word = rel_words[0]
                    # split the time into numbers and letters
                    first_letter_indx = min(range(len(word)),
                                            key = lambda i: i if word[i].isalpha() else len(word) + i)

                    if first_letter_indx < len(word) and word[first_letter_indx].isalpha():
                        num = int(word[:first_letter_indx])
                        if 'h' in word:
                            time_diff = timedelta(hours=-num)
                        elif 'm' in word:
                            time_diff = timedelta(minutes=-num)
                        else:
                            time_diff = timedelta(0)
                        article_date = datetime.now() + time_diff
            else:
                article_date = date.today()


        except:
            article_date = date.today()
        headlines.add((headline, article_date))
        del article, link, headline
    del article_entries
    return headlines


def extractRssCnnHeadlines(driver):
    """
    Gets headlines and dates from rss.cnn.com sources
    :param driver: Selenium Webdriver
    :return: list of headlines
    """
    headlines = set()
    z = 3

    pass
