import time
import json
import logging
from confluent_kafka import Producer
from apis.crypto_data.crypto_news import CryptoNewsAPI
from datetime import datetime, timedelta

BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'

class CryptoNewsProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST}
        self.producer = Producer(self.conf)
        self._set_logger()
        self.last_news_id = None
        self.oldest_date_fetched = None # 페이징용 상태 변수

    def _set_logger(self):
        logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        self.log = logging.getLogger(__name__)

    def delivery_callback(self, err, msg):
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)

    def produce(self):
        api = CryptoNewsAPI()
        target_start_date = '2025-09-09'

        while True:
            now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # oldest_date_fetched가 있으면 그 이전 데이터를, 없으면 최신부터 가져옴
            items = api.call(limit=1000, start_date=target_start_date, end_date=self.oldest_date_fetched)
            
            if not items:
                self.log.info("더 이상 가져올 데이터가 없습니다. 실시간 대기 모드로 전환합니다.")
                time.sleep(60)
                self.oldest_date_fetched = None # 다시 최신부터 체크하도록 초기화
                continue

            new_items = []
            for item in items:
                # 실시간 모드일 때만 중복 체크
                if self.oldest_date_fetched is None and self.last_news_id and item['id'] == self.last_news_id:
                    break
                new_items.append(item)

            if not new_items:
                self.log.info("새로운 뉴스가 없습니다.")
                self.oldest_date_fetched = None
            else:
                self.log.info(f"{len(new_items)}건의 데이터를 처리합니다.")
                # 전송 시에는 과거 -> 최신 순서로 전송
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

                    self.producer.produce(
                        topic=self.topic,
                        key=json.dumps({'NEWS_ID': item['NEWS_ID'], 'CRT_DTTM': item['CRT_DTTM']}, ensure_ascii=False),
                        value=json.dumps(item, ensure_ascii=False),
                        on_delivery=self.delivery_callback
                    )
                    
                    if self.oldest_date_fetched is None:
                        self.last_news_id = item['NEWS_ID']

                self.producer.flush()
                
                # 페이징 핵심: 이번에 가져온 1000개 중 가장 과거 뉴스의 날짜를 저장
                # 다음 루프에서는 이 날짜 이전(endDate)의 1000개를 가져오게 됨
                last_item_date = items[-1]['publishedDate']
                self.oldest_date_fetched = last_item_date
                self.log.info(f"다음 조회를 위해 endDate 설정: {self.oldest_date_fetched}")

            time.sleep(5) # 과거 데이터 수집 시에는 빠르게 루프

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()