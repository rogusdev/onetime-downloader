# onetime-downloader
Generate onetime download links for files stored in some kind of (cloud based?) storage

## TODO:
- [x] 2 api keys in header for curl: 1) add and list available files 2) create and list download links
- [x] get endpoint returns json list of available files in storage
- [x] post endpoint to add available file to storage
- [x] post endpoint to create/store + return custom (random) url to download one of those files with tracking of use
- [x] get endpoint to download file using custom url
- [x] updated downloaded_at and prevent future downloads when downloading file
- [x] get endpoint returns json list of available links for a given filename
- [x] separate out modules into files and maybe folders (storage providers)
- [ ] unit tests for ^
- [x] dockerfile to run ^
- [ ] JSON error responses when things go wrong

- [ ] rate limiting by IP that increases as more limits get triggered
- [x] support storage provider: dynamodb
- [x] support storage provider: postgres
- [ ] support storage provider: redis
- [ ] support storage provider: mongo
- [ ] support other storage providers, plugin style: s3, gcs, azure blob, mysql, rds, aurora

- [ ] google sso browser login to view list in UI
- [ ] button to generate link for file in UI
- [ ] support other SSO auth providers, plugin style


## Setup

Docker:
```
docker network create www

docker rm -f onetime-downloader
docker build -t onetime-downloader .
docker run --env-file .env -e PG_HOST=postgres-www -p 8080:8080 --network=www --name onetime-downloader onetime-downloader

docker run -d --restart=always --env-file .env -e PG_HOST=postgres-www --network=www -l 'caddy'='downloads.chrisrogus.com' -l 'caddy.reverse_proxy'='\$CONTAINER_IP:8080' --name onetime-downloader onetime-downloader

docker exec -it onetime-downloader bash
```

## Initialize

### Postgres

```
--DROP SCHEMA IF EXISTS onetime CASCADE;
CREATE SCHEMA IF NOT EXISTS onetime;

CREATE TABLE IF NOT EXISTS onetime.files (
    filename TEXT NOT NULL PRIMARY KEY,
    contents BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS onetime.links (
    token TEXT NOT NULL PRIMARY KEY,
    filename TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    downloaded_at BIGINT,
    ip_address TEXT
);
```

docker:
```
docker rm -f postgres-www
docker run -d --restart=always -p 5432:5432 --network=www -v $PWD/postgres-data:/var/lib/postgresql --name postgres-www -e POSTGRES_PASSWORD=badpassword postgres:12.4-alpine

docker run -it --rm --network www postgres:12.4-alpine psql -h postgres-www -U postgres
psql -h localhost -p 5432 -U postgres -d postgres

\d onetime.*
```

### Dynamodb

```
aws dynamodb delete-table \
    --profile rogusdev-chris \
    --table-name Onetime.Links


aws dynamodb create-table \
    --profile rogusdev-chris \
    --table-name Onetime.Files \
    --attribute-definitions \
        AttributeName=Filename,AttributeType=S \
    --key-schema \
        AttributeName=Filename,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

#        AttributeName=Contents,AttributeType=B \
#        AttributeName=CreatedAt,AttributeType=N \
#        AttributeName=UpdatedAt,AttributeType=N \

aws dynamodb create-table \
    --profile rogusdev-chris \
    --table-name Onetime.Links \
    --attribute-definitions \
        AttributeName=Token,AttributeType=S \
    --key-schema \
        AttributeName=Token,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

#        AttributeName=Filename,AttributeType=S \
#        AttributeName=CreatedAt,AttributeType=N \
#        AttributeName=DownloadedAt,AttributeType=N \
#        AttributeName=Ip,AttributeType=N \
```
