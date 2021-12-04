# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class BulkCompetitions(scrapy.Item):
    bulk = scrapy.Field()

class CompetitionItem(scrapy.Item):
    country = scrapy.Field()
    params = scrapy.Field()
    competition_name = scrapy.Field()
    sport_id = scrapy.Field()
    eventi = scrapy.Field()
    ciclo = scrapy.Field()
    count_eventi = scrapy.Field()



class EventItem(scrapy.Item):
    event_name = scrapy.Field()
    params = scrapy.Field()
    href = scrapy.Field()
    data_datainizio = scrapy.Field()
    gruppo_quota = scrapy.Field()
    quote = scrapy.Field()

class BulkQuotaItem(scrapy.Item):
    bulk = scrapy.Field()

class QuotaItem(scrapy.Item):
    valore_quota = scrapy.Field()
    data_giocabilita = scrapy.Field()
    cs = scrapy.Field()
    cod_scom = scrapy.Field()
    de = scrapy.Field()
    iad = scrapy.Field()
    params = scrapy.Field()
    ciclo = scrapy.Field()
    event_id = scrapy.Field()
    selection_id = scrapy.Field()
    market_id = scrapy.Field()
    tipo_quota = scrapy.Field()
    url = scrapy.Field()

