# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A light REST API to query the backed data.

This is not production-quality software, but intended as a proof-of-concept.
This is intentionally mimiking the Google Maps Engine API.
Currently supports only the Tables.features list operation at url:
/tables/{db}:{table}/features

Supports the following path parameters:
* id

Supports the following query parameters:
* limit
* orderBy
* select
* where

All other query parameters are ignored.
"""

import json
import logging
import os
import traceback
import MySQLdb
import re

import sqlalchemy

# flask, geojson,geomet and sqlparse are external dependencies.
# Install them by running pip install -r requirements.txt -t lib
import sqlparse

import flask

import geojson

from google.appengine.api import users
from google.appengine.api import oauth

import jacs.features
import jacs.auth


CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')





# This is your CloudSQL instance
#_INSTANCE = 'project-lightning-strike:dev-eu'
_INSTANCE = 'valued-rigging-830:test3'
_GEOMETRY_FIELD = 'geometry'
# This is the host to connect to in the dev server.
# This can be the IP address of your CloudSQL server, if you want to test that.
#_MYSQL_HOST = 'localhost'
_MYSQL_HOST = '173.194.250.121'
_MYSQL_DATABASE = 'db1'
_MYSQL_USER = 'root'
_MYSQL_PASSWORD = 'changeme'

_SQL_TEST_ENGINE='mysql+mysqldb://%(username)s:%(password)s@%(host)s/%(database)s' % {
    'username': _MYSQL_USER,
    'password': _MYSQL_PASSWORD,
    'host': _MYSQL_HOST,
    'database': _MYSQL_DATABASE,
    'instance': _INSTANCE
    }

_SQL_PROD_ENGINE='mysql+gaerdbms:///%(database)s?instance=%(instance)s' % {
    'database': _MYSQL_DATABASE,
    'instance': _INSTANCE
    }

# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.
app = flask.Flask(__name__)


@app.before_request
def before_request():
    if (os.getenv('SERVER_SOFTWARE') and
        os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
        flask.g.engine = sqlalchemy.create_engine(_SQL_PROD_ENGINE, echo=False)
    else:
        flask.g.engine = sqlalchemy.create_engine(_SQL_TEST_ENGINE, echo=True)

    try:
        flask.g.features = jacs.features.Features(flask.g.engine, _GEOMETRY_FIELD)
    except sqlalchemy.exc.DBAPIError as e:
        build_response({'error': 'Database Error %s' % str(e), 'status': 500})

@app.route('/tables/<table>/features')
def do_features_list(table):
    """Handle the parsing of the request and return the geojson.

    This routes all the /tables/... requests to the handler.
    See http://flask.pocoo.org/docs/0.10/api/#flask.Flask.route

    Args:
      database: The name of the database to use, this is picked from the URL.
      table: The database table to query from, this is picked from the URL.
    Returns:
      A flask.Response object with the GeoJSON to be returned, or an error JSON.
    """
    where = flask.request.args.get('where', default='true')
    select = flask.request.args.get('select', default='')
    limit = flask.request.args.get('limit')
    order_by = flask.request.args.get('orderBy')
    intersects = flask.request.args.get('intersects')
    offset = flask.request.args.get('offset')

    result = flask.g.features.list(table, select, where,
        limit=limit, offset=offset, order_by=order_by,
        intersects=intersects)

    return build_response(result, geojson.dumps)


@app.route('/tables/<table>/features/batchInsert', methods=['POST'])
def do_feature_create(table):
    result = flask.g.features.create(table, flask.request.data)
    return build_response(result)


@app.route('/tables/<table>/features/batchPatch', methods=['PATCH'])
def do_feature_update(table):
    result = flask.g.features.update(table,flask.request.data)
    return build_response(result)


@app.route('/tables/<table>/features/batchDelete', methods=['POST'])
def do_feature_delete(table):
    where = flask.request.args.get('where')
    limit = flask.request.args.get('limit')
    order_by = flask.request.args.get('order_by')
    data = {'primary_keys':[]}
    try:
        data = json.loads(flask.request.data)
    except ValueError as e:
        return build_response({
            'error':"Unable to parse request data. %s" % (e),
            'status': 400})

    keys = data['primary_keys']
    result = flask.g.features.delete(table, keys, where=where,
            limit=limit, order_by=order_by)

    return build_response(result)


def build_response(result, method=json.dumps):
    status = 200
    if 'status' in result:
        status = result['status']
    if 'error' in result:
        method = json.dumps
        if status is None:
            status = 500
    return flask.Response(
            response=method(result),
            mimetype='application/json',
            status = status)


@app.route('/pip/<database>:<table>')
def do_pip(database, table):
    """Handle the parsing of the point in polygon request and return a polygon.

    This routes all the /pip/... requests to the handler.
    See http://flask.pocoo.org/docs/0.10/api/#flask.Flask.route

    Args:
      database: The name of the database to use, this is picked from the URL.
      table: The database table to query from, this is picked from the URL.
    Returns:
      A flask.Response object with the GeoJSON to be returned, or an error JSON.
    """
    lat = float(flask.request.args.get('lat', default=0.0))
    lng = float(flask.request.args.get('lng', default=0.0))
    select = flask.request.args.get('select', default='')
    try:
        pip = PointInPolygon(_INSTANCE, database, table)
    except MySQLdb.OperationalError as e:
        error = {'error': 'Database Error %s' % str(e)}
    return flask.Response(
            response=json.dumps(error),
            mimetype='application/json',
            status=500)

    polygon = pip.pip(lat, lng, select)
    if 'error' in polygon:
        return flask.Response(
            response=json.dumps(polygon),
            mimetype='application/json',
            status=500)
    else:
        return flask.Response(
            response=geojson.dumps(polygon, sort_keys=True),
            mimetype='application/json',
            status=200)


@app.errorhandler(404)
def page_not_found(_):
    """Return a custom 404 error."""
    return 'Sorry, Nothing at this URL.', 404


@app.errorhandler(500)
def internal_error(_):
    """Return a custom 500 error."""
    return 'Sorry, unexpected error: {}'.format(traceback.format_exc()), 500



class PointInPolygon(object):
    """This class handles the pip requests.

    It uses Features to query the db.
    """

    def __init__(self, instance, database, table):
        """Create a Features instance and store the table.

        Args:
          instance: The name of the CloudSQL instance.
          database: The name of the database to use.
          table: The table that contains the features that we want to look up.
        """
        self._features = Features(instance, database)
        self._table = table

    def pip(self, lat, lng, fields):
        """This method returns the polygon that contains the given coordinate.

        It uses the Feature class to make the actual query.

        Args:
          lat: The latitude
          lng: The longitude
          fields: The fields to select as data

        Returns:
          A geojson polygon or an error dict.
        """
        point = "GeomFromText('POINT(%f %f)')" % (lng, lat)
        return self._features.list(
                self._table, fields,
                'ST_CONTAINS(%s,%s)' % (_GEOMETRY_FIELD, point),
                limit=1)
