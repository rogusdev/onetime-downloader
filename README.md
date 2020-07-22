# onetime-downloader
Generate onetime download links for files stored in some kind of (cloud based?) storage

## TODO:
- [ ] 2 api keys in header for curl: 1) add and list available files 2) create and list download links
- [ ] get endpoint returns json list of available files in storage
- [ ] post endpoint to add available file to storage
- [ ] post endpoint to create/store + return custom (random) url to download one of those files with tracking of use
- [ ] get endpoint to download file using custom url (updated downloaded_at)
- [ ] get endpoint to list avaiulable links for a given file

- [ ] rate limiting by IP that increases as more limits get triggered
- [ ] support other storage providers, plugin style: s3, gcs, azure blob, postgres, mysql, rds, aurora, mongo

- [ ] google sso browser login to view list in UI
- [ ] button to generate link for file in UI
- [ ] support other SSO auth providers, plugin style
