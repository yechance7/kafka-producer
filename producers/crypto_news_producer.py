import time
import json
import logging
import boto3
from confluent_kafka import Producer
from apis.crypto_data.crypto_news import CryptoNewsAPI
from datetime import datetime, timedelta

# 환경 설정
BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'
BUCKET_NAME = 'ybigta-mlops-landing-zone-324037321745'
STATE_KEY = 'producer_metadata/last_date.txt'
SENT_IDS_KEY = 'producer_metadata/sent_news_ids.json' # 중복 방지용 ID 저장소

class CryptoNewsProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST}
        self.producer = Producer(self.conf)
        self.s3_client = boto3.client('s3', region_name='ap-northeast-2')
        self.sent_ids = set() 
        self._set_logger()
        self._load_sent_ids() # 시작 시 기존 전송 기록 로드

    def _set_logger(self):
        logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        self.log = logging.getLogger(__name__)

    def _load_sent_ids(self):
        """S3에서 이미 전송된 뉴스 ID 목록을 로드하여 중복 방지"""
        try:
            response = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=SENT_IDS_KEY)
            ids = json.loads(response['Body'].read().decode('utf-8'))
            self.sent_ids = set(ids)
            self.log.info(f"기존 전송 기록 {len(self.sent_ids)}건 로드 완료.")
        except Exception:
            self.sent_ids = set()
            self.log.info("기존 전송 기록이 없습니다. 새로 시작합니다.")

    def _save_sent_ids(self):
        """전송된 ID 목록을 S3에 보관하여 재시작 시 중복 방지"""
        try:
            # 메모리 관리를 위해 최근 5000건의 ID만 유지
            id_list = list(self.sent_ids)[-5000:]
            self.s3_client.put_object(
                Bucket=BUCKET_NAME, Key=SENT_IDS_KEY,
                Body=json.dumps(id_list)
            )
        except Exception as e:
            self.log.error(f"ID 저장 실패: {e}")

    def _send_to_kafka(self, items):
        """중복을 필터링하여 Kafka로 전송"""
        new_items_count = 0
        for item in reversed(items):
            news_id = str(item.get('id'))
            
            # [중복 체크] 이미 보낸 ID면 스킵
            if news_id in self.sent_ids:
                continue

            # 데이터 정제 로직
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
                value=json.dumps(item, ensure_ascii=False)
            )
            self.sent_ids.add(news_id)
            new_items_count += 1
            
        if new_items_count > 0:
            self.producer.flush()
            self._save_sent_ids()
        return new_items_count

    def produce(self):
        api = CryptoNewsAPI()
        
        # 1. 수집 시작 날짜 결정
        try:
            res = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
            last_date_str = res['Body'].read().decode('utf-8').strip()
        except Exception:
            last_date_str = '2025-09-01'
        
        current_date = datetime.strptime(last_date_str, '%Y-%m-%d')
        self.log.info(f"파이프라인 가동. 시작점: {last_date_str}")

        # 2. 과거 데이터 수집 모드 (어제 날짜까지 모두 긁어옴)
        while current_date.date() < datetime.now().date():
            start_str = current_date.strftime('%Y-%m-%d')
            next_date = current_date + timedelta(days=1)
            end_str = next_date.strftime('%Y-%m-%d')
            
            self.log.info(f">>> [과거수집] {start_str} 수집 중...")
            items = api.call(limit=1000, start_date=start_str, end_date=end_str)
            
            if items:
                cnt = self._send_to_kafka(items)
                self.log.info(f"전송 완료: {start_str} ({cnt}건 신규)")
            
            current_date = next_date
            # S3에 진행 상태 업데이트
            self.s3_client.put_object(Bucket=BUCKET_NAME, Key=STATE_KEY, Body=current_date.strftime('%Y-%m-%d'))
            time.sleep(2) 

        # 3. 실시간 수집 모드 (3분 주기)
        self.log.info("과거 데이터 수집 완료. 실시간 모드(3분 주기)로 전환합니다.")
        while True:
            today_str = datetime.now().strftime('%Y-%m-%d')
            items = api.call(limit=100, start_date=today_str) 
            
            if items:
                cnt = self._send_to_kafka(items)
                if cnt > 0:
                    self.log.info(f"실시간 신규 데이터 {cnt}건 전송 완료.")
            
            self.log.info("3분 대기 후 다음 체크를 진행합니다.")
            time.sleep(180) # 3분 주기 설정

if __name__ == '__main__':
    producer = CryptoNewsProducer(topic='apis.crypto.news')
    producer.produce()