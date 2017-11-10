import requests
from bs4 import BeautifulSoup
import queue
import threading
import time
import re
import csv

class MALIndexCrawler(object):

    def __init__(self):

        self.queue_ids = queue.Queue()
        self.queue_lk = threading.Lock()
        
        self.visited_ids = set()
        
        self.processed = 0
        self.request_headers = {"User-Agent":"Mozilla/5.0"}
        
        self.process_lk = threading.Lock()
        
        output_file = open('mal.csv', 'w', newline='')
        self.file_writter = csv.writer(output_file)
        self.file_writter.writerow([
                'anime_id',
                'name',
                'score',
                'score_count',
                'rank',
                'popularity',
                'members',
                'favorites',
                'show_type',
                'episodes',
                'status',
                'aired',
                'premiered',
                'broadcast',
                'producers',
                'licensors',
                'studios',
                'source',
                'genres',
                'duration',
                'rating',
                'recommendations',
                'description'
                ])

        workers = []
        for i in range(0):
            break
            worker = threading.Thread(target=self.retrieve_animes, args=[i])
            worker.start()
            workers.append(worker)
        
        self.retrieve_animes(0)
        
        for worker in workers:
            worker.join()

        output_file.close()
        print(self.visited_ids)
        print('{} processed'.format(self.processed))
            

    def retrieve_animes(self, worker_id):
        
        print('starting worker {}'.format(worker_id))
        
        while True:
            print('worker {} retrieving next'.format(worker_id))
            if not self.process_next_anime():
                break
            print(self.processed)

    def process_next_anime(self):
        
        self.queue_lk.acquire()

        if self.queue_ids.empty():
            print('FINISHED PAGE: processed {}'.format(self.processed))
            if not self.retrieve_index():
                self.queue_lk.release()
                return False
            print('QUEUE SIZE: {}'.format(self.queue_ids.qsize()))
        
        anime_id = self.queue_ids.get()

        self.queue_lk.release()
    
        url = self.get_page_url(anime_id)
        
        while True:
            anime_page = requests.get(url, self.request_headers)
            if anime_page.status_code == 200:
                break
            print('throttled')
            time.sleep(0.8)
        
        soup = BeautifulSoup(anime_page.text, 'html.parser')
        
        name = self.get_name(soup)
        description = self.get_desc(soup)
        score, score_count = self.get_score(soup)
        rank = self.get_rank(soup)
        popularity = self.get_popularity(soup)
        members = self.get_members(soup)
        favorites = self.get_favorites(soup)
        
        show_type = self.get_type(soup)
        episodes = self.get_episodes(soup)
        status = self.get_status(soup)
        aired = self.get_aired(soup)
        premiered = self.get_premiered(soup)
        broadcast = self.get_broadcast(soup)
        producers = self.get_producers(soup)
        licensors = self.get_licensors(soup)
        studios = self.get_studios(soup)
        source = self.get_source(soup)
        genres = self.get_genres(soup)
        duration = self.get_duration(soup)
        rating = self.get_rating(soup)
        
        recs = self.get_num_recs(soup, anime_id)
        
        
        self.process_lk.acquire()
        
        self.processed += 1
        self.visited_ids.add(anime_id)
        
        row = [
            anime_id,
            name.encode('ascii','ignore'),
            score,
            score_count,
            rank,
            popularity,
            members,
            favorites,
            show_type,
            episodes,
            status,
            aired,
            premiered,
            broadcast,
            producers,
            licensors,
            studios,
            source,
            genres,
            duration,
            rating,
            recs,
            description.encode('ascii','ignore')
        ]
        

        self.file_writter.writerow(row)
        print(row)
        
        self.process_lk.release()
        
        time.sleep(0.6)
        
        return True
    
    def retrieve_index(self):
        
        url = 'https://myanimelist.net/topanime.php?limit={}'.format(
                self.processed
                )
        
        while True:
            page = requests.get(url, self.request_headers)
            if page.status_code == 200:
                break
        
        soup = BeautifulSoup(page.text, 'html.parser')
        
        anchors = soup.find_all('a', href=True, class_='mr8')
        
        added_anime_ids = set()
        
        # get links formmated as /anime/id1-id2
        for anchor in anchors:
            link = anchor['href']
            if re.match(r'.*/anime/[0-9]+/.*', link):
                link_tokens = link.split('/')
                anime_id_index = link_tokens.index('anime') + 1
                anime_id = link_tokens[anime_id_index]
                
                if not anime_id.isdigit():
                    print('id not formtted correctly...')
                    
                if anime_id in added_anime_ids:
                    continue
                
                print(anime_id, ',', end='')
                added_anime_ids.add(anime_id)
                self.queue_ids.put(anime_id)
        print()
        return not self.queue_ids.empty()                
                
                
    def get_page_url(self, anime_id):
        
        url = 'https://myanimelist.net/anime/{}'.format(anime_id)
        return url
    

    def get_name(self, soup):
        
        name_tag = soup.find('span', itemprop='name')
        
        if not name_tag:
            return ''
        
        name = name_tag.string.strip()
        return name
    
    def get_desc(self, soup):
        
        desc_tag = soup.find('span', itemprop='description')
        
        if not desc_tag:
            return ''
        
        desc = re.sub('\n', ' ', desc_tag.text.strip())
        return desc
    
    def get_score(self, soup):
        
        score_tag = soup.find('div', {'data-title' : 'score'})
        if not score_tag:
            return ''
        
        score = score_tag.string.strip()
        users = re.sub(',', '', score_tag['data-user'].strip().split(' ')[0])
        return score, users
    

    def get_side_data(self, soup, label):

        label_tag = soup.find('span', string=label)
        if not label_tag:
            print('missing {}'.format(label))
            return ''
        data = label_tag.next_sibling.strip()
        
        if data == '':
            entries = []
            anchor_tags = label_tag.parent.findChildren('a')
            for anchor_tag in anchor_tags:
                entries.append(anchor_tag.text)
            data = ','.join(entries)
        return data
    
    def get_rank(self, soup):
        return re.sub('#', '', self.get_side_data(soup, 'Ranked:'))

    def get_popularity(self, soup):
        return re.sub('#', '', self.get_side_data(soup, 'Popularity:'))
    
    def get_members(self, soup):
        return re.sub(',', '', self.get_side_data(soup, 'Members:'))

    def get_favorites(self, soup):
        return re.sub(',', '', self.get_side_data(soup, 'Favorites:'))
    
    def get_type(self, soup):
        return self.get_side_data(soup, 'Type:')
    
    def get_episodes(self, soup):
        return self.get_side_data(soup, 'Episodes:')
    
    def get_status(self, soup):
        return self.get_side_data(soup, 'Status:')
    
    def get_aired(self, soup):
        return self.get_side_data(soup, 'Aired:')
    
    def get_premiered(self, soup):
        return self.get_side_data(soup, 'Premiered:')
    
    def get_broadcast(self, soup):
        return self.get_side_data(soup, 'Broadcast:')
    
    def get_producers(self, soup):
        return self.get_side_data(soup, 'Producers:')
    
    def get_licensors(self, soup):
        return self.get_side_data(soup, 'Licensors:')
    
    def get_studios(self, soup):
        return self.get_side_data(soup, 'Studios:')
    
    def get_source(self, soup):
        return self.get_side_data(soup, 'Source:')
    
    def get_genres(self, soup):
        return self.get_side_data(soup, 'Genres:')
    
    def get_duration(self, soup):
        return self.get_side_data(soup, 'Duration:')
    
    def get_rating(self, soup):
        return self.get_side_data(soup, 'Rating:')
    
    
    def get_num_recs(self, soup, anime_id):

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
            
        return ','.join(rec_anime_ids)
        

if __name__ == '__main__':
    crawler = MALIndexCrawler()

