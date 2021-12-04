# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import scrapy
from marathonbet.items import CompetitionItem, EventItem, QuotaItem, BulkCompetitions,BulkQuotaItem
from marathonbet import dbi
import mysql.connector

bookie_id = 44
test = False
if test:
    competitions = 'competitions_test'
    events = 'events_test'
    bookie_odds = 'bookie_odds_test'
else:
    competitions = 'competitions'
    events = 'events'
    bookie_odds = 'bookie_odds'


class MysqlPipeline:
    connection = None
    cursor = None
    competition_id = dict()
    bookie_id = bookie_id
    next_priority_event = 8000
    request_counter = 0

    def open_spider(self, spider):
        self.connection = dbi.get_db_connection()
        self.cursor = self.connection.cursor()
        # global test
        # if test:
        #     # svuoto le tabelle di test
        #     q = f"truncate table {competitions}"
        #     self.cursor.execute(q)
        #     q = f"truncate table {events}"
        #     self.cursor.execute(q)
        #     q = f"truncate table {bookie_odds}"
        #     self.cursor.execute(q)
        #     self.connection.commit()

    def close_spider(self, spider):
        query_di_chiusura = f"""
           update {bookie_odds} set hidden = 1 
           where bookie_id = %(bookie_id)s 
             and cycle in (
               select * from (
                 select cycle 
                 from {bookie_odds} 
                 where bookie_id = %(bookie_id)s 
                 group by cycle order by cycle desc limit 100 offset 1
               ) a)
            """
        self.cursor.execute(query_di_chiusura, dict(bookie_id=self.bookie_id))
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):
        if isinstance(item, BulkCompetitions):
            for c in item['bulk']:
                params = c['params']
                ciclo = c['ciclo']
                sport_id = c['sport_id']
                attr = dict(
                    competition = c['competition_name'],
                    sport_id = c['sport_id'],
                    country = c['country'],
                    bookie_id = self.bookie_id,
                    params = params
                )
                insert_skeleton = f"""
                    insert into {competitions} (competition,sport_id,country,bookie_id,params) values (
                        %(competition)s, %(sport_id)s, %(country)s, %(bookie_id)s, %(params)s
                        ) on duplicate key update update_time=NOW(), competition = %(competition)s, country = %(country)s;
                """
                self.cursor.execute(insert_skeleton, attr)
                competition_id = self.cursor.lastrowid
                if competition_id == 0:
                    # quando succede recupero l'id
                    q = f"select id from {competitions} where bookie_id=%s and params=%s limit 0,1"
                    self.cursor.execute(q, (self.bookie_id, params))
                    res = self.cursor.fetchall()
                    competition_id = res[0][0]

                self.connection.commit()
                for evento in c['eventi']:
                    # inserisco l'evento
                    insert_skeleton = f"""
                        insert into {events} (event,open_date,bookie_id,params,competition_id) values (
                            %(event)s, %(open_date)s, %(bookie_id)s, %(params)s , %(competition_id)s
                        )
                        on duplicate key update event=%(event)s, open_date=%(open_date)s, update_time=NOW()
                        """
                    evento_params = evento['params']
                    nome_evento = evento['event_name']
                    nome_evento = nome_evento.replace(' - ',' v ')

                    attr = dict(
                        event = nome_evento,
                        open_date = evento['data_datainizio'],
                        bookie_id = self.bookie_id,
                        params = evento_params,
                        competition_id = competition_id
                    )
                    self.cursor.execute(insert_skeleton, attr)
                    event_id = self.cursor.lastrowid
                    if event_id == 0:
                        # quando succede recupero l'id
                        q = f"select id from {events} where bookie_id=%s and params=%s limit 0,1"
                        self.cursor.execute(q, (self.bookie_id, evento_params))
                        res = self.cursor.fetchall()
                        event_id = res[0][0]
                    self.connection.commit()

                    self.save_quote(spider, evento['quote'], event_id=event_id, ciclo=ciclo)

                    if sport_id == 1:
                        headers = {
    "authority": "www.marathonbet.it",
    "pragma": "no-cache",
    "cache-control": "no-cache",
    "sec-ch-ua": "\" Not;A Brand\";v=\"99\", \"Google Chrome\";v=\"91\", \"Chromium\";v=\"91\"",
    "sec-ch-ua-mobile": "?0",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "navigate",
    "sec-fetch-user": "?1",
    "sec-fetch-dest": "document",
    "referer": "https://www.marathonbet.it/it/?cpcids=all&liveTab=popular",
    "accept-language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7"
}

                        #self.request_counter += 1
                        url_quote = 'https://www.marathonbet.it'+evento['href']
                        r = scrapy.Request(url_quote, headers=headers, callback=spider.parser_quote, dont_filter=True,cb_kwargs={"sport_id":sport_id, "ciclo": c['ciclo'], "event_id": event_id})
                        #r.sport_id = sport_id
                        #r.ciclo = c['ciclo']
                        #r.event_id = event_id
                        #self.next_priority_event -= 1
                        #r.priority = self.next_priority_event
                        #spider.header_handler(r)
                        spider.crawler.engine.crawl(r, spider)

        if isinstance(item, BulkQuotaItem):
            self.request_counter -= 1
            # spider.logger.info(f'Coda {self.request_counter} ')
            self.save_quote(spider, item['bulk'])

        return item


    def save_quote(self,spider, quote, event_id=False, ciclo=False):
        tutte_le_query = []
        for q in quote:
            if not event_id:
                event_id = q['event_id']

            market_id = q['market_id']
            selection_id = q['selection_id']
            odds = float(q['valore_quota'])
            if not ciclo:
                ciclo = q['ciclo']
            data_giocabilita = int(q['data_giocabilita'])
            insert_query = f" insert into {bookie_odds}" \
                           f" (   event_id, bookie_id,  market_id,   selection_id, odds,   game_play,  cycle)" \
                           f" values({event_id},{self.bookie_id},{market_id},{selection_id},{odds},{data_giocabilita},{ciclo})" \
                           f" on duplicate key update odds={odds}, cycle={ciclo}, hidden=0 "
            tutte_le_query.append(insert_query)

        if len(tutte_le_query) > 0:
            qux = ";".join(tutte_le_query)
            results = self.connection.cmd_query_iter(qux)
            tot_saved = 0
            for result in results:
                tot_saved += result['affected_rows']
            # if tot_saved != len(tutte_le_query):
            #     spider.logger.info('- - - - - - - - - - - - > Problema')

            self.connection.commit()
