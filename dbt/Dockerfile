FROM ghcr.io/dbt-labs/dbt-core:1.10.0

USER root

# dbt-athena 어댑터 설치 (버전 수정)
RUN pip install --no-cache-dir \
    'dbt-core==1.10.0' \
    'dbt-athena-community==1.9.4' \
    'click<8.2'

# dbt 디렉토리 생성
RUN mkdir -p /root/.dbt

WORKDIR /usr/app

EXPOSE 8001

# profiles.yml 심볼릭 링크 및 대기
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["ln -sf /usr/app/profiles.yml /root/.dbt/profiles.yml && sleep infinity"]