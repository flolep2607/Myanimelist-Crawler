import requests
from bs4 import BeautifulSoup
import queue
import threading
import time

class MALCrawler(object):
    
    def __init__(self, seed_id):

        self.queue_ids = queue.Queue()
        self.queue_lk = threading.Lock()
        
        self.visited_ids = set()
        
        self.processed = 0
        self.request_headers = {"User-Agent":"Mozilla/5.0"}
        
        self.print_lk = threading.Lock()
        
        self.add_anime(seed_id)
        while not self.process_next_anime():
            p

        workers = []
        for i in range(1):
            worker = threading.Thread(target=self.retrieve_animes, args=[i])
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()
        print(self.visited_ids)
        print('{} processed'.format(self.processed))
            

    def retrieve_animes(self, worker_id):
        
        print('starting worker {}'.format(worker_id))
        
        while True:
            print('worker {} retrieving next'.format(worker_id))
            if not self.process_next_anime():
                if self.queue_ids.empty():
                    break
            

    def process_next_anime(self):
        
        self.queue_lk.acquire()

        if self.queue_ids.empty():
            self.queue_lk.release()
            return False
        anime_id = self.queue_ids.get()

        self.queue_lk.release()
        
        url = self.get_url(anime_id)
        page = requests.get(url, self.request_headers)

        if page.status_code != 200:
            self.queue_ids.put(anime_id)
            return False
        
        soup = BeautifulSoup(page.text, 'html.parser')
        
        self.print_lk.acquire()

        self.processed += 1
        if self.processed % 1000 == 0:
            print('{} animes have been processed')
            print(self.visited_ids)
        print(self.get_name(soup, anime_id))
        print(self.get_recs(soup, anime_id))

        self.print_lk.release()
        
        time.sleep(0.5)
        
        return True
        

    def get_url(self, anime_id):
        
        return 'https://myanimelist.net/anime/{}'.format(anime_id)
    

    def get_name(self, soup, anime_id):
        
        name_tag = soup.find('span', itemprop='name')
        if not name_tag:
            print('{} has no name...'.format(anime_id))
            return ''
        name = name_tag.string
        return name
    
    
    def get_recs(self, soup, anime_id):

        anchors = soup.find_all('a', href=True)
        rec_anime_ids = []
        
        # get links formmated as /recommendations/anime/id1-id2
        for anchor in anchors:
            link = anchor['href']
            if '/recommendations/' in link:
                link_tokens = link.split('/')
                anime_ids = link_tokens[-1].split('-')
                if anime_ids[0] == anime_id:
                    rec_anime_ids.append(anime_ids[1])
                else:
                    rec_anime_ids.append(anime_ids[0])

        for rec_anime_id in rec_anime_ids:
            self.add_anime(rec_anime_id)
            
        return rec_anime_ids

    def add_anime(self, anime_id):

        if anime_id in self.visited_ids:
            return
        self.queue_ids.put(anime_id)
        self.visited_ids.add(anime_id)


if __name__ == '__main__':

    seed_url = '13759'
    crawler = MALCrawler(seed_url)

