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
    for entry in entry_items:
        title_link = entry.css(".entry__item_title").css("a")[0].get()
        title = re.findall(r"(?<=>).*(?=</a>)", title_link)[0]
        pub_time_info = parse_datetime(entry.css(".entry__item_time").xpath("text()").get().strip())
        excerpt = entry.css(".entry__item_excerpt").xpath("text()").get().strip()
        if excerpt.endswith(".."):
            excerpt = excerpt[:-2]
        titles.append(title)
        summaries.append(excerpt)
        publish_dates.append(pub_time_info)
    return np.array(publish_dates), np.array(titles), np.array(summaries)
def start_test():
    run_test('https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244063&q=site:')
if __name__ == '__main__':
    run_test('https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244063&q=site:')