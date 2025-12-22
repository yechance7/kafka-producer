import time
import json
import logging
import os
import boto3
from confluent_kafka import Producer
from apis.crypto_data.crypto_news import CryptoNewsAPI
from datetime import datetime, timedelta

# 환경 설정
BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'
BUCKET_NAME = 'ybigta-mlops-landing-zone-324037321745'
STATE_KEY = 'producer_metadata/last_date.txt' # 수집 지점 저장 경로

class CryptoNewsProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST}
        self.producer = Producer(self.conf)
        # S3 클라이언트 설정 (권한은 IAM Role을 통해 부여됨)
        self.s3_client = boto3.client('s3', region_name='ap-northeast-2')
        self._set_logger()

    def _set_logger(self):
        logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        self.log = logging.getLogger(__name__)

    def delivery_callback(self, err, msg):
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)

    def save_state_to_s3(self, current_date_str):
        """배포 후에도 상태 유지를 위해 S3에 마지막 수집 날짜 기록"""
        try:
            self.s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=STATE_KEY,
                Body=current_date_str.encode('utf-8')
            )
            self.log.info(f"S3 상태 저장 성공: {current_date_str}")
        except Exception as e:
            self.log.error(f"S3 상태 저장 실패: {e}")

    def load_state_from_s3(self):
        """S3에서 수집 날짜 로드. 파일이 없으면 초기 날짜 2025-09-01 사용"""
        try:
            response = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
            return response['Body'].read().decode('utf-8').strip()
        except self.s3_client.exceptions.NoSuchKey:
            return '2025-09-01'
        except Exception as e:
            self.log.error(f"S3 상태 로드 중 에러: {e}")
            return '2025-09-01'

    def _send_to_kafka(self, items):
        """수집된 기사들을 Kafka 토픽으로 전송"""
        for item in reversed(items):
            # 뉴스 필드 정제
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

    def produce(self):
        api = CryptoNewsAPI()
        # 1. S3에서 시작 날짜 가져오기 (CI/CD 배포 시에도 유지됨)
        last_date_str = self.load_state_from_s3()
        current_date = datetime.strptime(last_date_str, '%Y-%m-%d')
        
        self.log.info(f"뉴스 수집 파이프라인 시작 (지점: {last_date_str})")

        while True:
            now = datetime.now()
            
            # 2. 과거 데이터 수집 모드 (9월 1일 ~ 어제)
            if current_date.date() < now.date():
                start_str = current_date.strftime('%Y-%m-%d')
                next_date = current_date + timedelta(days=1)
                end_str = next_date.strftime('%Y-%m-%d')
                
                self.log.info(f">>> [과거수집] {start_str} 데이터를 가져오는 중...")
                items = api.call(limit=1000, start_date=start_str, end_date=end_str)
                
                if items:
                    self._send_to_kafka(items)
                    self.log.info(f"전송 완료: {start_str} ({len(items)}건)")
                
                # 수집 날짜 갱신 및 S3 업데이트
                current_date = next_date
                self.save_state_to_s3(current_date.strftime('%Y-%m-%d'))
                time.sleep(5) 

            # 3. 실시간 최신 뉴스 수집 모드 (3분 주기)
            else:
                self.log.info("최신 뉴스 체크 중 (3분 주기 스케줄링)...")
                
                today_str = now.strftime('%Y-%m-%d')
                items = api.call(limit=100, start_date=today_str)
                
                if items:
                    self._send_to_kafka(items)
                    self.log.info(f"최신 데이터 {len(items)}건 전송.")
                
                time.sleep(180) # 3분 대기

if __name__ == '__main__':
    news_producer = CryptoNewsProducer(topic='apis.crypto.news')
    news_producer.produce()