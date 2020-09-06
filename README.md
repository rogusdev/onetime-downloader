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
- [ ] dockerfile to run ^

- [ ] rate limiting by IP that increases as more limits get triggered
- [x] support storage provider: dynamodb
- [ ] support storage provider: postgres
- [ ] support storage provider: redis
- [ ] support storage provider: mongo
- [ ] support other storage providers, plugin style: s3, gcs, azure blob, mysql, rds, aurora

- [ ] google sso browser login to view list in UI
- [ ] button to generate link for file in UI
- [ ] support other SSO auth providers, plugin style


docker network create www

docker rm -f postgres-www
docker run -d --restart=always -p 5432:5432 --network=www -v /vagrant/postgres-data:/var/lib/postgresql --name postgres-www -e POSTGRES_PASSWORD=badpassword postgres:12.4-alpine

docker run -it --rm --network www postgres:12.4-alpine psql -h postgres-www -U postgres

cat << EOF > /vagrant/.pgpass
postgres-www:5432:postgres:postgres:badpassword
EOF
