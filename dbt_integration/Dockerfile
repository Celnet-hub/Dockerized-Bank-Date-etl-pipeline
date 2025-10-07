FROM python:3.11-slim

WORKDIR /usr/app/dbt

# Install git and dbt-postgres
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip install dbt-postgres

COPY . .

ENTRYPOINT ["dbt"]