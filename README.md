# single-use-downloader
Generate onetime download links for files stored in cloud storage

# TODO:
- get endpoint returns json list of files in an aws s3 bucket
- post endpoint returns custom (random) url to download one of those files with tracking of use
- get endpoint to download file using custom url (updated downloaded_at)
- request limiting by IP for some security
- google sso browser login to view list
- button to generate link for file
- list links for files
- support other cloud storage providers, plugin style
- support other SSO auth providers, plugin style
