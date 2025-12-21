import time
import json
import logging
from confluent_kafka import Producer
from apis.seoul_data.crypto_news import CryptoNewsAPI # 파일명에 맞춰 수정
from datetime import datetime

BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092' # 유지

class CryptoNewsProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST}
        self.producer = Producer(self.conf)
        self._set_logger()

    def _set_logger(self): # 유지
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s]:%(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.log = logging.getLogger(__name__)

    def delivery_callback(self, err, msg): # 유지
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)

    def produce(self):
        api = CryptoNewsAPI()
        while True:
            now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            items = api.call() # 기본 btc, eth 뉴스 호출
            
            for item in items:
                # 컬럼명 변경 로직 (Tiingo 예시 데이터 기반)
                item['NEWS_ID'] = item.pop('id')
                item['PUB_DTTM'] = item.pop('publishedDate')
                item['TITLE'] = item.pop('title')
                item['URL'] = item.pop('url')
                item['DESC'] = item.pop('description')
                item['SRC_NM'] = item.pop('source')
                item['TICKERS'] = item.pop('tickers')
                
                # 컬럼 추가
                item['CRT_DTTM'] = now_dt

                # produce 실행
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=json.dumps({'NEWS_ID': item['NEWS_ID'], 'CRT_DTTM': item['CRT_DTTM']}, ensure_ascii=False),
                        value=json.dumps(item, ensure_ascii=False),
                        on_delivery=self.delivery_callback
                    )
                except BufferError:
                    self.log.error('%% Local producer queue is full: try again\n')

            # 서비스 콜백 및 Flush
            self.producer.poll(0)
            self.log.info('%% Waiting for %d deliveries\n' % len(self.producer))
            self.producer.flush()

            # API 호출 주기 (뉴스 데이터 특성상 1분으로 설정)
            time.sleep(60)

if __name__ == '__main__':
    bicycle_producer = CryptoNewsProducer(topic='apis.crypto.news')
    bicycle_producer.produce()