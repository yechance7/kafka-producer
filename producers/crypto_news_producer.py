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
        while True:
            now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            items = api.call() # 최신 뉴스 100개 호출
            
            new_items = []
            for item in items:
                # 1. 중복 체크: 이미 처리한 ID를 만나면 루프 중단
                if self.last_news_id and item['id'] == self.last_news_id:
                    break
                new_items.append(item)

            if not new_items:
                self.log.info("새로운 뉴스가 없습니다. 대기합니다.")
            else:
                self.log.info(f"새로운 뉴스 {len(new_items)}건 발견! 전송을 시작합니다.")
                
                # 2. 역순 정렬: 과거 뉴스부터 브로커에 쌓이도록 함 (선택 사항)
                for item in reversed(new_items):
                    # 데이터 전처리
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
                        # 마지막 전송 ID 업데이트
                        self.last_news_id = item['NEWS_ID']
                    except BufferError:
                        self.log.error('%% Local producer queue is full: try again\n')

                self.producer.poll(0)
                self.log.info('%% Waiting for %d deliveries\n' % len(self.producer))
                self.producer.flush()

            # 1분 대기
            time.sleep(60)

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()