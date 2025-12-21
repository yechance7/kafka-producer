import time
import json
import logging
from confluent_kafka import Producer
from apis.crypto_data.crypto_news import CryptoNewsAPI
from datetime import datetime

BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'

class CryptoNewsProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST}
        self.producer = Producer(self.conf)
        self._set_logger()
        self.last_news_id = None

    def _set_logger(self):
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s]:%(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.log = logging.getLogger(__name__)

    def delivery_callback(self, err, msg):
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)

    def produce(self):
        api = CryptoNewsAPI()
        first_run = True # 초기 대량 수집 확인용 플래그

        while True:
            now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 첫 실행 시 지정된 날짜부터 1000개 수집
            if first_run and self.last_news_id is None:
                items = api.call(limit=1000, start_date='2025-09-09')
                first_run = False
            else:
                items = api.call(limit=1000) 
            
            new_items = []
            for item in items:
                if self.last_news_id and item['id'] == self.last_news_id:
                    break
                new_items.append(item)

            if not new_items:
                self.log.info("새로운 뉴스가 없습니다. 대기합니다.")
            else:
                self.log.info(f"새로운 뉴스 {len(new_items)}건 발견! 전송을 시작합니다.")
                
                for item in reversed(new_items):
                    item['NEWS_ID'] = item.pop('id')
                    item['PUB_DTTM'] = item.pop('publishedDate')
                    item['TITLE'] = item.pop('title')
                    item['URL'] = item.pop('url')
                    item['DESC'] = item.pop('description')
                    item['SRC_NM'] = item.pop('source')
                    item['TICKERS'] = item.pop('tickers')
                    item['CRT_DTTM'] = now_dt

                    try:
                        self.producer.produce(
                            topic=self.topic,
                            key=json.dumps({'NEWS_ID': item['NEWS_ID'], 'CRT_DTTM': item['CRT_DTTM']}, ensure_ascii=False),
                            value=json.dumps(item, ensure_ascii=False),
                            on_delivery=self.delivery_callback
                        )
                        self.last_news_id = item['NEWS_ID']
                    except BufferError:
                        self.log.error('%% Local producer queue is full: try again\n')

                self.producer.poll(0)
                self.log.info('%% Waiting for %d deliveries\n' % len(self.producer))
                self.producer.flush()

            # 수집 주기 15초로 단축
            time.sleep(15)

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()