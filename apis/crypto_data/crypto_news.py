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

    def _set_logger(self):
        logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        self.log = logging.getLogger(__name__)

    def delivery_callback(self, err, msg):
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)

    def produce(self):
        api = CryptoNewsAPI()
        current_date = datetime.strptime('2025-09-01', '%Y-%m-%d')
        
        while True:
            # 오늘 날짜 확인
            today = datetime.now()
            if current_date > today:
                self.log.info("모든 과거 데이터 수집 완료. 1시간 대기 모드로 전환합니다.")
                time.sleep(3600)
                continue

            start_str = current_date.strftime('%Y-%m-%d')

            next_date = current_date + timedelta(days=7)
            
            if next_date > today:
                next_date = today + timedelta(days=1)
            
            end_str = next_date.strftime('%Y-%m-%d')
            
            self.log.info(f"===> [{start_str} ~ {end_str}] 범위 데이터 수집 중...")
            # Tiingo API 호출 (최대 1000건)
            items = api.call(limit=1000, start_date=start_str, end_date=end_str)
            
            if items:
                if len(items) == 1000:
                    self.log.warning(f"주의: {start_str} 주간 데이터가 1000건을 초과했을 가능성이 있습니다.")

                for item in reversed(items):
                    item['NEWS_ID'] = item.pop('id')
                    item['PUB_DTTM'] = item.pop('publishedDate')
                    item['TITLE'] = item.pop('title')
                    item['URL'] = item.pop('url')
                    item['DESC'] = item.pop('description')
                    item['SRC_NM'] = item.pop('source')
                    item['TICKERS'] = item.pop('tickers')
                    item['CRT_DTTM'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    self.producer.produce(
                        topic=self.topic,
                        key=json.dumps({'NEWS_ID': item['NEWS_ID']}, ensure_ascii=False),
                        value=json.dumps(item, ensure_ascii=False),
                        on_delivery=self.delivery_callback
                    )
                
                self.producer.flush()
                self.log.info(f"성공: {start_str} 주간 데이터 {len(items)}건 처리 완료.")
            else:
                self.log.info(f"결과 없음: {start_str} 주간에 데이터가 없습니다.")

            # 다음 주로 이동
            current_date = next_date
            time.sleep(10) 

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()