FROM python:3.9-slim

WORKDIR /app

# cron 설치
RUN apt-get update && apt-get install -y cron

# 필요한 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY app.py .
COPY scripts/ ./scripts/
COPY start.sh .
COPY db.env .
COPY crontab /etc/cron.d/app-cron

# 실행 권한 부여
RUN chmod +x start.sh
RUN chmod 0644 /etc/cron.d/app-cron
RUN crontab /etc/cron.d/app-cron

# 환경 변수 설정
ENV HOST=localhost
ENV DATABASE=imsolo
ENV USER=root
ENV PASSWORD=2561

# 컨테이너 실행 시 시작 스크립트 실행
CMD ["./start.sh"]
