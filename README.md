# Géorisques Metadata Updater — DocumentCloud Add-On

[Please see the Add-On documentation](https://github.com/MuckRock/documentcloud-hello-world-addon/wiki/)

This Add-On keeps the **installation-level metadata** of already-uploaded Géorisques
documents in sync with the latest Géorisques source data. It is the maintenance
counterpart to the Géorisques scraper (`documentcloud-georisques-scraper`, which does the
initial upload): the scraper stamps each document with an `installation_aiot_code`, and this
Add-On revisits those documents and refreshes their `installation_*` metadata from the
current Géorisques CSV export.