## JACS: (Geo)Json API for Cloud SQL

JACS is an App Engine application that exposes an API for accessing geospatial
data located in [Cloud SQL](https://cloud.google.com/sql/docs).

The API mimics the Google Maps Engine API, and provides a convenient way 
for your Google Maps API application to fetch data for the Data Layer, without
needing to maintain a SQL Database or web server.

## Run Locally
1. Install the [App Engine Python SDK](https://developers.google.com/appengine/downloads).
See the README file for directions. You'll need python 2.7 and [pip 1.4 or later](http://www.pip-installer.org/en/latest/installing.html) installed too.

2. Install dependencies in the project's lib directory.
   Note: App Engine can only import libraries from inside your project directory.

   ```
   cd jacs
   pip install -r requirements.txt -t lib
   ```
3. Run this project locally from the command line:

   ```
   dev_appserver.py .
   ```

Visit the application [http://localhost:8080](http://localhost:8080)

See [the development server documentation](https://developers.google.com/appengine/docs/python/tools/devserver)
for options when running dev_appserver.

## Deploy
To deploy the application:

1. Use the [Admin Console](https://appengine.google.com) to create a
   project/app id. (App id and project id are identical)
1. [Deploy the
   application](https://developers.google.com/appengine/docs/python/tools/uploadinganapp) with

   ```
   appcfg.py -A <your-project-id> --oauth2 update .
   ```
1. Congratulations!  Your application is now live at your-app-id.appspot.com

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
Star this repo if you found it useful. Use the github issue tracker to give
feedback on this repo.

## Contributing changes
See [CONTRIB.md](CONTRIB.md)

## Licensing
See [LICENSE](LICENSE)

## Not a Google product
This is not an official Google product (experimental or otherwise), it is
just code that happens to be owned by Google.

## Contributors
Wolf Bergenheim
