import requests
from bs4 import BeautifulSoup
import queue
import threading
import time
import re
import csv

class MALCharacterCrawler(object):

    def __init__(self):

        self._anime_ids = queue.Queue()
        self._char_ids = queue.Queue()
        
        self.request_headers = {"User-Agent":"Mozilla/5.0"}
        
        
        self._load_anime_ids()
            
        self._processed = 0
        
        output_file = open('mal_character2.csv', 'w', newline='')
        self.file_writter = csv.writer(output_file)
        self.file_writter.writerow([
                'anime_id',
                'name',
                'main',
                'favorites',
                'gender_guess',
                'age',
                'birthday',
                'height',
                'weight',
                'zodiac',
                'blood',
                'desc'
                ])
        

        workers = []
        
        for i in range(0):
            break
            worker = threading.Thread(target=self._get_animes, args=[i])
            worker.start()
            workers.append(worker)
        
        self._get_animes(0)
        
        for worker in workers:
            worker.join()

        output_file.close()
            
    def _load_anime_ids(self):
        
        with open('animes_dump.csv', 'r') as animes_file:
            anime_reader = csv.reader(animes_file, delimiter=',', quotechar='"')
            for row in anime_reader:
                if int(row[0]) < 56:
                    continue
                self._anime_ids.put(row[0])
        
    def _get_animes(self, worker_id):
        
        print('starting worker {}'.format(worker_id))
        
        while self._anime_ids.qsize() > 0:
            anime_id = self._anime_ids.get()
            print('worker {} retrieving anime {}'.format(worker_id, anime_id))
            self._get_characters(anime_id)
            self._process_characters(anime_id)
            

    def _get_characters(self, anime_id):
        
        url = self._get_page_url(anime_id)
        soup = self._get_page_content(url)
        
        char_container = soup.find('div', {'class' : 'detail-characters-list'})
        
        if not char_container:
            print ('characters not found for anime: {}'.format(anime_id))
            return

        anchors = char_container.find_all('a', {'class' : 'fw-n'})

        # get links formmated as character/<charid>/<charname>
        for anchor in anchors:
            link = anchor['href']
            if '/character/' in link:
                link_tokens = link.split('/')
                char_id = link_tokens[-2]
                print(char_id)
                
                role = anchor.parent.parent.findNext('small').contents[0]
                self._char_ids.put((char_id, role, link))
            else:
                print('not a character link!')
    

    def _process_characters(self, anime_id):

        while not self._char_ids.empty():
            char_info = self._char_ids.get()
            char_id, role, link = char_info
            
            soup = self._get_page_content(link)
            
            name = soup.find('h1', {'class' : 'h1'}).contents[0]
            column = soup.find('td', {'class' : 'borderClass'})
            members_raw = str(column).split('<br/>')[-1]
            members = re.sub(',', '', members_raw.split(' ')[-1].split('\n')[0])
            
            
            breadcrumbs = soup.find('div', {'class' : 'breadcrumb'})
            
            td = breadcrumbs.parent
            for tag in td.find_all('div'):
                tag.replaceWith('')
            for tag in td.find_all('table'):
                tag.replaceWith('')
            for tag in td.find_all('h2'):
                tag.replaceWith('')
            
            segments = td.text.split('\n')
            
            data_tags = (
                    'Gender',
                    'Age',
                    'Birthday',
                    'Height',
                    'Weight',
                    'Zodiac',
                    'Blood type',
                    'Desc'
            )

            data = {}
            desc_blocks = []

            for seg in segments:
                if seg.strip() == '':
                    continue
                
                is_tag = False
                for tag in data_tags:
                    if seg.find(tag) == 0:
                        data[tag] = self._extract_char_data(seg, tag)
                        is_tag = True
                        break
                    
                if is_tag:
                    continue
                
                desc_blocks.append(seg)
                # print('Line: {}'.format(seg))

            desc = ' '.join(desc_blocks)
            data[data_tags[-1]] = desc.encode('ascii','ignore')
            data[data_tags[0]] = self._predict_gender(desc)

            row = [anime_id, name.encode('ascii','ignore'), role, members]
            row.extend([data.get(tag) for tag in data_tags])
            
            self.file_writter.writerow(row)
            
            print(anime_id, char_id, name)
            self._processed += 1
            
            time.sleep(0.8)
            if self._processed % 100 == 0:
                print('PROCESSED {}!!!'.format(self._processed))

    def _extract_char_data(self, seg, tag):

        return seg[seg.index(tag) + len(tag) + 2:].strip()
    
    
    def _predict_gender(self, desc):

        male_keywords = len(re.findall('(\W|^|\.)(he|him)(\W|$)', desc))
        female_keywords = len(re.findall('(\W|^|\.)(she|her)(\W|$)', desc))
        
        if male_keywords >= female_keywords:
            return 'm'
        return 'f'
    
    
    def _get_page_url(self, anime_id):
        
        url = 'https://myanimelist.net/anime/{}'.format(anime_id)
        return url
    
    def _get_page_content(self, url):
        
        while True:
            anime_page = requests.get(url, self.request_headers)
            if anime_page.status_code == 200:
                break
            print('throttled')
            time.sleep(0.8)
        
        return BeautifulSoup(anime_page.text, 'html.parser')
        

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
        

if __name__ == '__main__':
    crawler = MALCharacterCrawler()
