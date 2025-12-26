FROM ghcr.io/dbt-labs/dbt-core:1.8.0

USER root

# dbt-athena 어댑터 설치
RUN pip install --no-cache-dir \
    'dbt-core==1.8.0' \
    'dbt-athena-community==1.8.0' \
    'click<8.2'

# profiles.yml 자동 링크
RUN mkdir -p /root/.dbt

WORKDIR /usr/app

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["ln -sf /usr/app/profiles.yml /root/.dbt/profiles.yml && sleep infinity"]