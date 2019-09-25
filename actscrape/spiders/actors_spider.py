import scrapy
from scrapy.exceptions import CloseSpider
import pandas as pd
import re

class Paparazzi(scrapy.Spider):
    name = "paparazzi"
    base_url = "https://en.wikipedia.org"
    start_urls= [ 
        base_url + '/wiki/Category:American_male_film_actors?from=Be', 
        base_url + '/wiki/Category:American_male_film_actors?from=Mo'
    ]
    
    colmap = {'m': 'Married', 'div': 'Divored', 'sep': 'Separated', 'died': 'Died'}

    links = pd.DataFrame(columns = ['Spouse1', 'Spouse2', 'Married', 'Divorced', 'Separated', 'Died'])
    matchpat = '([^(]+)\((.+?)\)'

    def __init__(self):
        checked = pd.read_csv('ppl.csv')['0'].tolist()
        self.already_scraped = set(checked)

    def parse(self, response):
        sel = "div.mw-category-group > ul:nth-child(2) > li >a::attr(href)"
        links = response.css(sel).getall()
        for link in links:
            person = link
            if person in self.already_scraped:
                continue
            new_url = self.base_url + person
            self.already_scraped.add(person)
            yield scrapy.Request(url = new_url, callback = self.parseactor)
        
        buttons = response.css('#mw-pages > a')
        for button in buttons: 
            if button.css('*::text').get() == 'next page':
                new_url = self.base_url + button.css('*::attr(href)').get()
                yield scrapy.Request(url = new_url, callback = self.parse)
                break
    
    def parseactor(self, response):
        actor = response.url.split('/')[-1]
        sel = ".infobox > tbody > tr"
        auto_card = response.css(sel)
        for row in auto_card:
            txt = row.css("th *::text")
            if not len(txt):
                continue
            txt = txt[0].get()
            if 'spouse' not in txt.lower():
                continue
            spouses = row.css('td > div')
            
            for spouse in spouses:
                if spouse in self.already_scraped:
                    continue

                spouselink = spouse.css('*::attr(href)').get()

                if not spouselink:
                    continue
                
                marriage = pd.Series()
                marriage['Spouse1'] = actor
                spousename = spouselink.split('/')[-1]
                marriage['Spouse2'] = spousename

                texts = spouse.css('*::text').getall()
                
                infos = ' '.join(texts).split('\n')
                
                sps = [re.match(self.matchpat, x).groups() for x in infos if len(x)]
                for sp in sps:
                    if sp[0].replace(' ', '_') in spouselink:
                        changes = sp[1].split(';')
                        for change in changes:
                            if 'died' not in change:
                                c = change.split('.')[0].replace(' ', '')
                            else:
                                change = 'died'
                            year = change[-4:]
                            marriage[self.colmap[c]] = int(year)
                            

                self.links = self.links.append(marriage, ignore_index = True)

                self.already_scraped.add(spouse)
                yield scrapy.Request(self.base_url + spouselink, callback = self.parseactor)
                if len(self.already_scraped) > 3000:
                    CloseSpider('maxed out')

    def closed(self, spider):
        self.links
        self.links.to_csv('testing2.csv', index = False)
        pd.DataFrame(list(self.already_scraped)).to_csv('ppl2.csv')

