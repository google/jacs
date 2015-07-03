## JACS: (Geo)Json API for Cloud SQL

JACS is an App Engine application that exposes an API for accessing geospatial
data located in [Cloud SQL](https://cloud.google.com/sql/docs).

The API mimics the Google Maps Engine API, and provides a convenient way 
for your Google Maps API application to fetch data for the Data Layer, without
needing to maintain a SQL Database or web server.

## Deploy
Please see the [Deployment Guide](https://github.com/google/jacs/wiki/Deployment-guide) in the JACS Wiki

## Next Steps
You need to populate you Cloud SQL database with geospatial data, and create a
front-end application to display your map data. You can host the HTML and javascript in the same App Engine instance.

### Installing Libraries
See the [Third party
libraries](https://developers.google.com/appengine/docs/python/tools/libraries27)
page for libraries that are already included in the SDK.  To include SDK
libraries, add them in your app.yaml file. Other than libraries included in
the SDK, only pure python libraries may be added to an App Engine project.

### Feedback
Star this repo if you found it useful. Use the [github issue tracker](https://github.com/google/jacs/issues)
to give feedback on JACS.

## Contributing changes
See [CONTRIB.md](CONTRIB.md)

## Licensing
See [LICENSE](LICENSE)

## Not a Google product
This is not an official Google product (experimental or otherwise), it is
just code that happens to be owned by Google.

## Contributors
* Wolf Bergenheim
* Kyle Mackenzie
