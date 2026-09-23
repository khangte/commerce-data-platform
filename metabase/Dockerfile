FROM metabase/metabase:v0.63.16.1 AS metabase_release

# DuckDB JDBC native 라이브러리는 musl 호환 계층에서 안정적으로 동작하지 않으므로 glibc Java 런타임을 사용한다.
FROM eclipse-temurin:25-jre

RUN apt-get update \
    && apt-get install --no-install-recommends -y wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=metabase_release /app /app

WORKDIR /app

EXPOSE 3000

ENTRYPOINT ["/app/run_metabase.sh"]
