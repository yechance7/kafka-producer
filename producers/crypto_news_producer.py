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
        # 수집 시작일 설정
        current_date = datetime.strptime('2025-09-09', '%Y-%m-%d')
        
        while True:
            # 수집 대상일 (오늘을 넘어가면 대기)
            if current_date.date() > datetime.now().date():
                self.log.info("모든 과거 데이터가 최신 날짜까지 수집되었습니다. 1시간 대기합니다.")
                time.sleep(3600)
                continue

            start_str = current_date.strftime('%Y-%m-%d')
            # endDate는 미포함(publishedDate < endDate)이므로 다음날로 설정
            next_date = current_date + timedelta(days=1)
            end_str = next_date.strftime('%Y-%m-%d')
            
            self.log.info(f">>> {start_str} 날짜 데이터 수집 시도...")
            items = api.call(limit=1000, start_date=start_str, end_date=end_str)
            
            # 해당 날짜에 데이터가 있는 경우만 처리
            if items:
                for item in reversed(items): # 과거 순서로 전송
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
                self.log.info(f"OK: {start_str} 데이터 {len(items)}건 전송 완료.")
            else:
                self.log.info(f"SKIP: {start_str} 날짜에 뉴스가 없습니다.")

            # 처리가 끝나면 다음 날짜로 변경 (성공/실패 상관없이 전진)
            current_date = next_date
            time.sleep(2) # API 속도 제한 방지용 대기

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()