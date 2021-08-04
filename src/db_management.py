import os
import mysql.connector

# f = open(os.path.abspath('../mydbcredentials.txt'), "r")
# user, passwd = tuple(f.readlines())
# db = mysql.connector.connect(
#     host='localhost',
#     user=user,
#     passwd=passwd,
#     database='HeadlineDB'
# )
# curser = db.cursor()
# del user, passwd


# def setup_sources():
#     sources = [('48c5b62a-f1ae-11eb-a062-a4bb6d4d225a', 'FOX Business Markets',
#                 'https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244063&q=site:'),
#                ('48c5cb66-f1ae-11eb-a062-a4bb6d4d225a', 'FOX Business Economy',
#                 'https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244061&q=site:'),
#                ('9715f654-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Top Stories', 'http://rss.cnn.com/rss/cnn_topstories.rss'),
#                ('971610b9-f1ab-11eb-a062-a4bb6d4d225a', 'CNN World', 'http://rss.cnn.com/rss/cnn_world.rss'),
#                ('971616a1-f1ab-11eb-a062-a4bb6d4d225a', 'CNN U.S.', 'http://rss.cnn.com/rss/cnn_us.rss'),
#                ('97161bae-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Business (CNNMoney.com)',
#                 'http://rss.cnn.com/rss/money_latest.rss'),
#                ('971620ac-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Politics', 'http://rss.cnn.com/rss/cnn_allpolitics.rss'),
#                ('971625ce-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Technology', 'http://rss.cnn.com/rss/cnn_tech.rss'),
#                ('97163f36-f1ab-11eb-a062-a4bb6d4d225a', 'CNN 10',
#                 'http://rss.cnn.com/services/podcasting/cnn10/rss.xml'),
#                ('97164510-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Most Recent', 'http://rss.cnn.com/rss/cnn_latest.rss'),
#                ('97164a4b-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Underscored', 'http://rss.cnn.com/cnn-underscored.rss'),
#                ('f0006fd9-f1ac-11eb-a062-a4bb6d4d225a', 'FOX Latest Headlines (all sections)',
#                 'http://feeds.foxnews.com/foxnews/latest'),
#                ('f0008e46-f1ac-11eb-a062-a4bb6d4d225a', 'FOX National', 'http://feeds.foxnews.com/foxnews/national'),
#                ('f000951c-f1ac-11eb-a062-a4bb6d4d225a', 'FOX World', 'http://feeds.foxnews.com/foxnews/world'),
#                ('f0009b9f-f1ac-11eb-a062-a4bb6d4d225a', 'FOX Politics', 'http://feeds.foxnews.com/foxnews/politics'),
#                ('f000a7b7-f1ac-11eb-a062-a4bb6d4d225a', 'FOX SciTech', 'http://feeds.foxnews.com/foxnews/scitech')
#                ]
#     for source in sources:
#         uuid, name, rss_url = source
#         curser.execute(
#             "INSERT INTO SOURCE (id,name,rss_url) VALUES (\'{0}\', \'{1}\', \'{2}\')".format(uuid, name, rss_url))
#     db.commit()

if __name__ == '__main__':
    # sources = [('48c5b62a-f1ae-11eb-a062-a4bb6d4d225a', 'FOX Business Markets',
    #             'https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244063&q=site:'),
    #            ('48c5cb66-f1ae-11eb-a062-a4bb6d4d225a', 'FOX Business Economy',
    #             'https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244061&q=site:'),
    #            ('9715f654-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Top Stories', 'http://rss.cnn.com/rss/cnn_topstories.rss'),
    #            ('971610b9-f1ab-11eb-a062-a4bb6d4d225a', 'CNN World', 'http://rss.cnn.com/rss/cnn_world.rss'),
    #            ('971616a1-f1ab-11eb-a062-a4bb6d4d225a', 'CNN U.S.', 'http://rss.cnn.com/rss/cnn_us.rss'),
    #            ('97161bae-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Business (CNNMoney.com)',
    #             'http://rss.cnn.com/rss/money_latest.rss'),
    #            ('971620ac-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Politics', 'http://rss.cnn.com/rss/cnn_allpolitics.rss'),
    #            ('971625ce-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Technology', 'http://rss.cnn.com/rss/cnn_tech.rss'),
    #            ('97163f36-f1ab-11eb-a062-a4bb6d4d225a', 'CNN 10',
    #             'http://rss.cnn.com/services/podcasting/cnn10/rss.xml'),
    #            ('97164510-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Most Recent', 'http://rss.cnn.com/rss/cnn_latest.rss'),
    #            ('97164a4b-f1ab-11eb-a062-a4bb6d4d225a', 'CNN Underscored', 'http://rss.cnn.com/cnn-underscored.rss'),
    #            ('f0006fd9-f1ac-11eb-a062-a4bb6d4d225a', 'FOX Latest Headlines (all sections)',
    #             'http://feeds.foxnews.com/foxnews/latest'),
    #            ('f0008e46-f1ac-11eb-a062-a4bb6d4d225a', 'FOX National', 'http://feeds.foxnews.com/foxnews/national'),
    #            ('f000951c-f1ac-11eb-a062-a4bb6d4d225a', 'FOX World', 'http://feeds.foxnews.com/foxnews/world'),
    #            ('f0009b9f-f1ac-11eb-a062-a4bb6d4d225a', 'FOX Politics', 'http://feeds.foxnews.com/foxnews/politics'),
    #            ('f000a7b7-f1ac-11eb-a062-a4bb6d4d225a', 'FOX SciTech', 'http://feeds.foxnews.com/foxnews/scitech')
    #            ]
    # sources = [tuple(x[1:]) for x in sources]
    # print(sources)
    sources = [('FOX Business Markets', 'https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244063&q=site:'),
     ('FOX Business Economy', 'https://www.feedspot.com/infiniterss.php?_src=feed_title&followfeedid=5244061&q=site:'),
     ('CNN Top Stories', 'http://rss.cnn.com/rss/cnn_topstories.rss'),
     ('CNN World', 'http://rss.cnn.com/rss/cnn_world.rss'), ('CNN U.S.', 'http://rss.cnn.com/rss/cnn_us.rss'),
     ('CNN Business (CNNMoney.com)', 'http://rss.cnn.com/rss/money_latest.rss'),
     ('CNN Politics', 'http://rss.cnn.com/rss/cnn_allpolitics.rss'),
     ('CNN Technology', 'http://rss.cnn.com/rss/cnn_tech.rss'),
     ('CNN 10', 'http://rss.cnn.com/services/podcasting/cnn10/rss.xml'),
     ('CNN Most Recent', 'http://rss.cnn.com/rss/cnn_latest.rss'),
     ('CNN Underscored', 'http://rss.cnn.com/cnn-underscored.rss'),
     ('FOX Latest Headlines (all sections)', 'http://feeds.foxnews.com/foxnews/latest'),
     ('FOX National', 'http://feeds.foxnews.com/foxnews/national'),
     ('FOX World', 'http://feeds.foxnews.com/foxnews/world'),
     ('FOX Politics', 'http://feeds.foxnews.com/foxnews/politics'),
     ('FOX SciTech', 'http://feeds.foxnews.com/foxnews/scitech')]
    s_string = lambda x: "{0},{1}".format(x[0], x[1])
    string = "\n".join(["NAME,RSS_URL"] + [s_string(x) for x in sources])
    f = open("Sources.csv", "w")
    f.write(string)
    f.close()


    # curser.execute("Select * from SOURCE")
    # results = curser.fetchall()
    # for r in results:
    #     print(r)
