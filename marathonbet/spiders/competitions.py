import scrapy
from marathonbet.spiders.mappatura_marathonbet import data_selection_key_2_ninja
from marathonbet.items import CompetitionItem, BulkCompetitions, EventItem, QuotaItem, BulkQuotaItem
from scrapy.shell import inspect_response
import time
import random
from parsel import Selector
import datetime, locale
import json
from marathonbet.spiders.user_agents import userAgentsList
from importlib.machinery import SourceFileLoader
import logging
#proxies_list_aux = SourceFileLoader(
#    "module.name",
#    "/home/ubuntu/scrapyd/proxies_list.py"
#).load_module()
#proxies_list = proxies_list_aux.proxies_list
# from marathonbet.spiders.proxies_list import proxies_list


class CompetitionsSpider(scrapy.Spider):
    name = 'competitions'
    ciclo = int(time.time())
    next_priority_event = 90000
    page = {
        1:0,
        2:0,
        3:0,
        4:0
    }
    #proxy_usage = 0
    #proxy = ''

    def make_page_url(self, page, sport_id):
        #                    1618232467026, 1618232467026
        rnd = random.randint(1111111111111, 9999999999999)
        rnd = str(rnd)
        if sport_id == 1:
            return f'https://www.marathonbet.it/it/betting/Football+-+11?interval=ALL_TIME&page={page}&pageAction=getPage&_={rnd}'
        elif sport_id==2:
            return f'https://www.marathonbet.it/it/betting/Tennis+-+2398?interval=ALL_TIME&page={page}&pageAction=getPage&_={rnd}'
        elif sport_id==3:
            return f'https://www.marathonbet.it/it/betting/Basketball+-+6?interval=ALL_TIME&page={page}&pageAction=getPage&_={rnd}'
        elif sport_id==4:
            return f'https://www.marathonbet.it/it/betting/Table+Tennis+-+382549?interval=ALL_TIME&page={page}&pageAction=getPage&_={rnd}'

    def start_requests(self):
        richieste_successive = []
        # creo 4 request per gli sport 1,2,3,4
        for sport_id in (1,2,3,4):
            begin_url = self.make_page_url(page = self.page[sport_id], sport_id=sport_id)
            r = scrapy.Request(begin_url,cb_kwargs={"sport_id":sport_id})
            #r.sport_id = sport_id
            r.headers.setdefault('Accept', 'application/json, text/plain, */*')
            self.header_handler(r)
            self.next_priority_event -= 1
            r.priority = self.next_priority_event
            richieste_successive.append(r)
        return richieste_successive

    def header_handler(self, r):
        # imposto il proxy
        #if self.proxy_usage >=100 or self.proxy_usage == 0:
            # r.meta['proxy'] = 'ninjabet:Ninja2018!@192.109.110.213:12345'
            #self.proxy = random.choice(proxies_list)
            #self.proxy_usage = 1

        #r.meta['proxy'] = self.proxy

        # r.meta['proxy'] = 'ninjabet:Ninja2018!@192.109.110.213:12345'
        #self.proxy_usage += 1
        #self.logger.warning('---- proxy > %s' + self.proxy)

        r.headers.setdefault('Accept', 'application/json, text/plain, */*')
        r.headers.setdefault('Accept-Encoding', 'gzip, deflate, br')
        q1 = random.randint(6,9) / 10.0
        q2 = random.randint(1,9) / 10.0
        q3 = random.randint(1,9) / 10.0
        r.headers.setdefault('Accept-Language', 'it-IT,it;q=%s,en-US;q=%s,en;q=%s' % (q1,q2,q3) )
        r.headers.setdefault('Cache-Control', 'no-cache')
        r.headers.setdefault('Connection', 'keep-alive')
        r.headers.setdefault('Origin', 'https://www.marathonbet.it')
        r.headers.setdefault('Pragma', 'no-cache')
        r.headers.setdefault('Referer', 'https://www.marathonbet.it/it/?cpcids=all&liveTab=popular')
        r.headers.setdefault('TE', 'Trailers')
        ua = random.choice(userAgentsList)
        #ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        r.headers.setdefault('User-Agent', ua)

    def parse(self, response,sport_id):
        #self.logger.info(response.request.url)
        richieste_successive = []
        competizioni_trovate = []
        #sport_id = response.request.sport_id

        # controllo se c'e' un'atra pagina
        res = json.loads(response.body.decode('utf-8'))
        if res[1]['prop'] == 'hasNextPage' and res[1]['val'] == True:
            # creo la richiesta per la pagina successiva
            self.page[sport_id] += 1

            next_page_url = self.make_page_url(page=self.page[sport_id], sport_id=sport_id)
            r = scrapy.Request(next_page_url,cb_kwargs={"sport_id":sport_id})
            #r.sport_id = sport_id
            r.headers.setdefault('Accept', 'application/json, text/plain, */*')
            self.header_handler(r)
            self.next_priority_event -= 1
            r.priority = self.next_priority_event
            richieste_successive.append(r)

        body = res[0]['content']
        s = Selector(text=body)
        competitions_to_discard = ["Risultati finali", "Giocatore", "Squadra", "marcatore"]
        elements_to_discard = ["qualificazione", "turno", "andata", "ritorno", "gironi", "finale", "girone", "semifinali", "regolare"]
        for cat in s.css('.category-container'):
            competition = ''
            href = cat.css('.category-label-link').attrib['href']
            for aux in cat.css('.category-label .nowrap'):
                element = aux.css('::text').get()
                if any(element_to_discard in element.lower() for element_to_discard in elements_to_discard):
                    break
                competition += element
            if any(competition_to_discard in competition for competition_to_discard in competitions_to_discard):
                continue
            competition_params = href.split("+-+")[1]
            if sport_id == 1:
                #competition_params = "/".join(href.split("/")[:6])
                if href.split("/")[4] == "Internationals":
                    country = "Internazionali"
                    competition_name = competition.replace("."," ")
                elif href.split("/")[4] == "Clubs.+International":
                    country = "Club Internazionale"
                    competition_name = competition.replace("."," ")
                elif href.split("/")[4] == "Women":
                    country = competition.split(".")[1]
                    competition_name = competition.replace("."," ")
                else:
                    country = competition.split(".")[0]
                    competition_name = competition.replace("."," ")
            if sport_id == 2:
                #competition_params = "/".join(href.split("/")[:6])                
                country = competition.split(".")[1]
                competition_name = competition.replace("."," ")
            if sport_id == 3:
                #competition_params = "/".join(href.split("/")[:7])                
                if href.split("/")[4] == "Clubs.+International":
                    country = "Club Internazionale"
                    competition_name = competition.replace("."," ")
                elif href.split("/")[4] == "NBA":
                    country = "USA"
                    competition_name = "NBA"
                else:
                    country = competition.split(".")[0]
                    competition_name = competition.replace("."," ")
                #    try:
                #        competition_name = competition.split(".")[1] + " " + competition.split(".")[2]
                #    except:
                #        competition_name = competition
            if sport_id == 4:
                #competition_params = "/".join(href.split("/")[:6])                
                country = competition.split(".")[1]
                competition_name = competition.replace("."," ")
            # competition_params = cat.attrib['data-category-treeid']
            # self.logger.info('  %s' % (competition_name,))
            # recupero il nome delle quote
            # nomi_colonne = []
            # for col in cat.css('.coupone'):
            #     nomi_colonne.append( [i for i in  col.css('::text').extract() if len(i.strip())>0][0])

            eventi_trovati = []
            # for e in cat.css('.member-area-content-table'):
            for e in cat.css('table.coupon-row-item:not(.coupone-labels)'):
                eux = e.css('.member-area-content-table')
                trs = eux.css('tr')
                href = trs[0].css('.member-link').attrib['href']
                params = href.split("+-+")[1]
                num = trs[0].css('.member-number::text').get()
                num = int(float(num))
                flag_quote_inverse = ''
                if num == 1:
                    casa = trs[0].css('[data-member-link]::text').get()
                    ospite = trs[1].css('[data-member-link]::text').get()
                else:
                    flag_quote_inverse = '_inverso'
                    casa = trs[1].css('[data-member-link]::text').get()
                    ospite = trs[0].css('[data-member-link]::text').get()

                nome_evento = f"{casa} v {ospite}"

                # documentazione: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
                locale.setlocale(locale.LC_TIME, ('it', 'UTF-8'))
                data_ora = trs[0].css('.date::text').get().strip()
                now = datetime.datetime.now()
                year = now.year
                day = now.day
                month = now.month
                #try:
                #    dux = datetime.datetime.strptime(data_ora, '%d %b %H:%M')
                #    dux = dux.replace(year=year)
                #except:
                #    dux = datetime.datetime.strptime(data_ora, '%H:%M')
                #    dux = dux.replace(year=year, month=month, day=day)
                spazi = data_ora.count(" ")
                if spazi == 0:
                    dux = datetime.datetime.strptime(data_ora, '%H:%M')
                    dux = dux.replace(year=year, month=month, day=day)
                elif spazi == 2:
                    dux = datetime.datetime.strptime(data_ora, '%d %b %H:%M')
                    dux = dux.replace(year=year)
                else:
                    dux = datetime.datetime.strptime(data_ora, '%d %b %Y %H:%M')
                data_ora_evento = dux

                # recupero le quote
                quote_trovate = []
                for idx, q in enumerate(e.css('.active-selection')):
                    quota = float(q.xpath('@data-selection-price').get())
                    #logging.info(casa+" "+ospite+" "+str(quota))
                    # tipo_quota = nomi_colonne[idx]
                    data_selection_key = q.xpath('@data-selection-key').get()
                    mux = data_selection_key_2_ninja(data_selection_key, sport_id=sport_id)
                    if mux == False:
                        continue
                    #qux = "".join([i for i in q.css("::text").extract() if len(i.strip()) >0])
                    try:
                        valore_quota = float(q.xpath('@data-selection-price').get())
                    except:
                        continue

                    quote_trovate.append(QuotaItem(
                        valore_quota = valore_quota,
                        market_id = mux['market_id' + flag_quote_inverse],
                        selection_id = mux['selection_id' + flag_quote_inverse],
                        data_giocabilita=1,
                    ))

                eventi_trovati.append(EventItem(
                    event_name = nome_evento,
                    params = params,
                    href = href,
                    data_datainizio = data_ora_evento,
                    quote = quote_trovate
                ))

            competizioni_trovate.append(CompetitionItem(
                competition_name = competition_name,
                country = country,
                ciclo = self.ciclo,
                eventi = eventi_trovati,
                count_eventi = len(eventi_trovati),
                params= competition_params ,
                sport_id = sport_id
            ))
        bulk = [BulkCompetitions(bulk= competizioni_trovate)]
        outcome = [] + richieste_successive + bulk
        return outcome

    def parser_quote(self, response, event_id, sport_id, ciclo):
        #self.logger.info(response.request.url)
        richieste_successive = []
        competizioni_trovate = []
        # inspect_response(response,self)
        #event_id = response.request.event_id
        #ciclo = response.request.ciclo
        #sport_id = response.request.sport_id
        quote_trovate = []
        #for m in response.css('.market-inline-block-table-wrapper'):
        for idx, q in enumerate(response.css('.active-selection')):
                data_selection_key = q.xpath('@data-selection-key').get()
                mux = data_selection_key_2_ninja(data_selection_key, sport_id=sport_id)
                if mux == False:
                    continue
                valore_quota = float(q.xpath('@data-selection-price').get())
                #logging.info(str(valore_quota))
                quote_trovate.append(dict(
                    valore_quota=valore_quota,
                    event_id = event_id,
                    market_id = mux['market_id'],
                    selection_id = mux['selection_id'],
                    ciclo = ciclo,
                    data_giocabilita = 1,
                ))

        return BulkQuotaItem(
            bulk = quote_trovate
        )
