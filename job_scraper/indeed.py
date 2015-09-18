# -*- coding: utf-8 -*-

import requests, pandas as pd, sqlite3 as db
from bs4 import BeautifulSoup

from Queue import Queue
from threading import Thread
import time, arrow
 
class mine_jobs():

    dsn = {
        'database_file': './jobs.db',
    }

    conn            =   False

    num_threads     =   15
    urls            =   []
    data            =   []

    start           =   arrow.now().timestamp

    def __init__(self, num_threads=15):

        self.num_threads    =   num_threads
        self.q              =   Queue(maxsize=0)

        self.set_dbh()
        
    def __del__(self):
        self.conn.close()

    def get_dbh(self):
        return self.conn

    def set_dbh(self):
        self.conn = db.connect(self.dsn['database_file'])

    def get_http(self, url):

        result  =   requests.get(url)

        if result.status_code == 200:
            return result.content
        else:
            raise Exception('Response not 200')

    def run_threaded(self):

        for i in range(self.num_threads):
            worker = Thread(target=self.mine_job_urls, args=(self.q,))
            worker.setDaemon(True)
            worker.start()
         
        for url in self.urls:
            self.q.put(url)

        # stop jobs after they're done
        self.q.join()

        runtime         =   (arrow.now().timestamp - self.start)
        total_records   =   int(len(self.data))
        meantime        =   float(total_records) / float(runtime)

        print "Data collection complete:"
        print "%d records collected" % total_records
        print "Completed in %d seconds" % runtime
        print "μ%d/second" % meantime

    def mine_job_urls(self, q):
        
        while True:
         
            url = q.get()
            
            # self.mine_search_result(url)
            ## Thread ID useful to know?  Use this: q.get()
 
            self.mine_search_result(url)
            time.sleep(3)

            q.task_done()
 
    def get_page_meta(self, url=False):
        
        content = self.get_http(url)
        return content

    def mine_search_result(self, url):
        
        page_content    =   miner.get_page_meta(url=url)
        soup            =   BeautifulSoup(page_content)
        divs            =   soup.findAll('div', {'class': 'row'})

        for div in divs:

            if 'pagead' in div.find('a')['href']:
                continue

            self.data.append({
                'title':    div.find('a')['title'],
                'company':  div.find('span', {'itemprop': 'name'}).text.strip(),
                'date':     div.find('span', {'class': 'date'}).text.strip(),
                'url':      div.find('a')['href'],
                'summary':  div.find('span', {'class': 'summary'}).text.strip()
            })

            print "Mining ", url

    def mine_indeed(self, limit=501):
        # &start=10
        for page_num in xrange(10, limit, 10):
             self.urls.append("http://www.indeed.com/jobs?q=Data+Scientist&l=San+Francisco,+CA&start=%d" % page_num)

        self.run_threaded()




miner = mine_jobs(num_threads=25)
# miner.run_threaded()
miner.mine_indeed(limit=2111)
miner_df = pd.DataFrame(miner.data)
miner_df.to_sql('jobs', con=miner.get_dbh(), if_exists='replace')

# miner_grp = miner_df.groupby(['title', 'company', 'url'])
# print miner_grp.count().sort()   


# print miner.urls

# page_content = miner.get_page_meta(url='')

# soup = BeautifulSoup(page_content)
# divs = soup.findAll('div', {'class': 'row'})

# scraped = []

# for div in divs:

#     if 'pagead' in div.find('a')['href']:
#         continue

#     scraped.append({
#         'title':    div.find('a')['title'],
#         'company':  div.find('span', {'itemprop': 'name'}).text.strip(),
#         'date':     div.find('span', {'class': 'date'}).text.strip(),
#         'url':      div.find('a')['href'],
#         'summary':  div.find('span', {'class': 'summary'}).text.strip()
#     })


# page_df = pd.DataFrame(scraped)

# print page_df.head()
