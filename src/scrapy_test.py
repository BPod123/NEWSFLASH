from rss_read import *
def run_test(rss_url):
    publish_dates, titles, summaries = np.array([]), np.array([]), np.array([])
    print("Making request")
    resp = requests.get(rss_url)
    print("Request received\nMaking HTMLResponse")
    response = HtmlResponse(url=rss_url, body=resp.content)
    print("HTMLResponse made")
    sel = Selector(response)
    print("Selector made")
    entry_items = sel.css(".entry__item")
    publish_dates = []
    titles = []
    summaries = []
    for i, string in enumerate(list(map(str,map(Selector.get, entry_items))), start=1):
        print(f"\n\n\n\n{i})")
        print(string)
    for i, entry in enumerate(entry_items, start=1):
        print(f"{i})")
        title_link = entry.css(".entry__item_title").css("a")[0].get()
        title = re.findall(r"(?<=>).*(?=</a>)", title_link)[0]
        print(f"title: {title}")
        pub_time_info = parse_datetime(entry.css(".entry__item_time").xpath("text()").get().strip())
        print(f"Publish time: {pub_time_info}")
        excerpt = entry.css(".entry__item_excerpt").xpath("text()").get().strip()
        if excerpt.endswith(".."):
            excerpt = excerpt[:-2]
        print(f"Excerpt: {excerpt}")
        titles.append(title)
        summaries.append(excerpt)
        publish_dates.append(pub_time_info)
    return np.array(publish_dates), np.array(titles), np.array(summaries)
def start_test():
    urls = [
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5261952&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5261947&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5261944&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5245958&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5245948&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5245935&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5245929&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5245912&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5243665&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5243663&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5243652&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5243650&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5243646&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5242090&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5242079&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5242057&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=5242033&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=followbtn&followfeedid=4555082&q=site:https%3A%2F%2Fwww.theatlantic.com%2Ffeed%2Fchannel%2Fpolitics%2F",
        "https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244063&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244061&q=site:",
        "https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5232981&q=site:https%3A%2F%2Fwww.theatlantic.com%2Ffeed%2Fall%2F"
    ]
    names = [
        "The Atlantic Technology",
        "The Atlantic Global",
        "The Atlantic Business",
        "NBC Science",
        "NBC World",
        "NBC Politics",
        "NBC US",
        "NBC Coronavirus",
        "Sky News Business",
        "Sky News Technology",
        "Sky News Political News",
        "Sky News World News",
        "Sky News UK",
        "NPR Politics",
        "NPR Europe",
        "NPR Asia",
        "NPR National",
        "The Atlantic Politics",
        "FOX Business Markets",
        "FOX Business Economy",
        "The Atlantic World"
    ]
    fails = []
    for name, url in zip(names, urls):
        print("Running Test for {name}: {url}")
        results = run_test(url)
        if any((len(x) == 0 for x in results)):
            fails.append((name, url))
    if len(fails) > 0:
        print(f"{len(fails)} tests failed:")
        for i, (name, url) in enumerate(fails, start=1):
            print(f"{i}) {name} - {url}")
    else:
        print("All tests passed")
if __name__ == '__main__':
    start_test()