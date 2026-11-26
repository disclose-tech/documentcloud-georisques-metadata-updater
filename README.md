# Géorisques Metadata Updater — DocumentCloud Add-On

[Please see the Add-On documentation](https://github.com/MuckRock/documentcloud-hello-world-addon/wiki/)

This Add-On keeps the **installation-level metadata** of already-uploaded Géorisques
documents in sync with the latest Géorisques source data. It is the maintenance
counterpart to the Géorisques scraper (`documentcloud-georisques-scraper`, which does the
initial upload): the scraper stamps each document with an `installation_aiot_code`, and this
Add-On revisits those documents and refreshes their `installation_*` metadata from the
Géorisques installations API.

When an installation has no INSEE code, the Add-On geocodes the installation's commune with
data.geopf.fr, as the scraper does, and derives `departments` from the code; a renamed
installation's description (`<raison sociale> (<AIOT>)`) follows the new name.

A document whose installation Géorisques has blanked (no name, no place) is made private, and
public again once the name returns; a document made private by hand stays private.
