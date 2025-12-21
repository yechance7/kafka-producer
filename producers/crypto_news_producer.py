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
        # 수집을 시작할 날짜 설정
        current_date = datetime.strptime('2025-09-09', '%Y-%m-%d')
        
        while True:
            # 수집할 종료 날짜 (시작일 + 1일)
            next_date = current_date + timedelta(days=1)
            start_str = current_date.strftime('%Y-%m-%d')
            end_str = next_date.strftime('%Y-%m-%d')
            
            # 오늘 날짜를 넘어가면 다시 처음부터 혹은 실시간 모드로 대기
            if current_date > datetime.now():
                self.log.info("모든 과거 데이터 수집 완료. 1시간 대기 후 다시 확인합니다.")
                time.sleep(3600)
                current_date = datetime.strptime('2025-09-09', '%Y-%m-%d')
                continue

            self.log.info(f"{start_str} ~ {end_str} 범위 뉴스 조회 시작")
            # startDate와 endDate를 명확히 지정하여 호출
            items = api.call(limit=1000, start_date=start_str, end_date=end_str)
            
            if items:
                for item in items:
                    # 데이터 전처리
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
                self.log.info(f"{start_str} 데이터 {len(items)}건 전송 완료")
            
            # 다음 날짜로 이동
            current_date = next_date
            time.sleep(1) # API 부하 방지를 위한 짧은 대기

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()