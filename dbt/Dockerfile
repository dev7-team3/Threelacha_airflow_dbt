FROM ghcr.io/dbt-labs/dbt-core:1.8.0

USER root

# 버전을 명시적으로 고정하여 설치
RUN pip install --no-cache-dir \
    'dbt-core==1.8.0' \
    'dbt-trino==1.8.0' \
    'click<8.2'

# profiles.yml 자동 링크
RUN mkdir -p /root/.dbt

WORKDIR /usr/app

# ENTRYPOINT를 bash로 변경
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["ln -sf /usr/app/profiles.yml /root/.dbt/profiles.yml && sleep infinity"]
