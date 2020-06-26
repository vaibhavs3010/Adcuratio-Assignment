'''Spider file to scrape Hackernews'''

import pymongo
import requests  # can be done without this too, but its tedious
import scrapy

#from credentials import LINK_PREVIEW_KEY

LINK_PREVIEW_KEY = '5272e29cb573aff7e60ade31820305ab'

MONGO_CLIENT = pymongo.MongoClient("mongodb://localhost:27017/")
HN_DB = MONGO_CLIENT['hn_database']

headings = HN_DB["headings"]
metadata = HN_DB["metadata"]

class HNSpider(scrapy.Spider):
    name = 'hackernews-spider'
    start_urls = ['https://news.ycombinator.com']

    def parse(self, response):
        page_data = []
        item_list = response.xpath(
            '//table[@class="itemlist"]//tr[not(@class) or contains(@class, "athing")]')
        ele_for_next = item_list[-1]
        item_list = item_list[:-1]
        i = 0
        while i < len(item_list):
            data = {}
            data['title'] = item_list[i].xpath(
                './/a[@class="storylink"]/text()')[0].get()
            data['url'] = item_list[i].xpath(
                './/a[@class="storylink"]/@href')[0].get()
            data['image_url'] = requests.get(
                    'https://api.linkpreview.net',
                    params={'key': LINK_PREVIEW_KEY, 'q': data['url']}
                    ).json()['image']
            data['id'] = int(item_list[i].xpath('./@id')[0].get())

            try:
                data['votes'] = int(
                    item_list[i+1].xpath(
                        './/span[@class="score"]/text()')[0].get().split()[0])
            except IndexError:
                data['votes'] = None
            
            try:
                data['author'] = item_list[i+1].xpath(
                    './/a[@class="hnuser"]/text()')[0].get()
            except IndexError:
                data['author'] = None
            
            page_data.append(data)
            yield data
            
            i += 2


        # Now insert data to MongoDB
        headings_data = [
            {'url': data['url'], 'title': data['title']}
            for data in page_data
            ]
        metadata_items = [
            {
                'url': data['url'], '_id': data['id'],
                'image_url': data['image_url'], 'votes': data['votes'],
                'author': data['author']
            } for data in page_data]
        
        # ordered=False avoids duplicate insertion based on unique _id
        try:
            headings.insert_many(headings_data, ordered=False)
            metadata.insert_many(metadata_items, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            panic = filter(lambda x: x['code'] != 11000, e.details['writeErrors'])
            try:
                raise pymongo.errors.BulkWriteError(next(panic)['errmsg'])
            except StopIteration:  # no errors other than duplicate key ones
                pass

        for next_page in ele_for_next.xpath('.//a/@href'):
            yield response.follow(next_page, self.parse)
