import requests
from bs4 import BeautifulSoup
import queue

class MALCrawler(object):
    
    def __init__(self, seed_url):
        self.url_queue = queue.Queue()
        self.url_queue.put(seed_url)
        print(seed_url)
        while (not self.url_queue.empty()):
            url = self.url_queue.get()
            page = requests.get(url)
            soup = BeautifulSoup(page.text, 'html.parser')
            
            print(self.get_name(soup))

    def get_name(self, soup):
        name_tag = soup.find('span', itemprop='name')
        name = name_tag.string
        
        return name


if __name__ == '__main__':

    seed_url = 'https://myanimelist.net/anime/13759/Sakurasou_no_Pet_na_Kanojo'
    print('what')
    crawler = MALCrawler(seed_url)
    

