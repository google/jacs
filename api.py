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

from google.appengine.api import users
from google.appengine.api import memcache
from google.appengine.api import oauth

import flask
import sqlalchemy
import sqlparse
import geojson

import jacs.features
import jacs.auth


CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

_INSTANCE = os.environ['INSTANCE']
_GEOMETRY_FIELD = os.environ['GEOMETRY_FIELD']
_MYSQL_HOST = os.environ['MYSQL_HOST']
_MYSQL_DATABASE = os.environ['MYSQL_DATABASE']
_MYSQL_USER = os.environ['MYSQL_USER']
_MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']

_SQL_TEST_ENGINE='mysql+mysqldb://%(username)s:%(password)s@%(host)s/%(database)s?charset=utf8' % {
    'username': _MYSQL_USER,
    'password': _MYSQL_PASSWORD,
    'host': _MYSQL_HOST,
    'database': _MYSQL_DATABASE,
    'instance': _INSTANCE
    }

_SQL_PROD_ENGINE='mysql+gaerdbms:///%(database)s?instance=%(instance)s?charset=utf8' % {
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

    cache = memcache.get(flask.request.url)
    if cache is not None:
        response = flask.Response(
            response=cache,
            mimetype='application/json',
            status = 200)
    else:
        result = flask.g.features.list(
            table, select, where, limit=limit, offset=offset,
            order_by=order_by, intersects=intersects)
        response = build_response(result, build_features_list_response)
        data = response.get_data()
        if len(data) < 1000000:
            logging.info('adding response to memcache with key %s', flask.request.url)
            memcache.add(flask.request.url, data, 3600)
    return response


def build_features_list_response(result):
    features = []
    for feature in result['features']:
        key = '%s:%d' % (flask.request.base_url, feature['id'])
        cached_json = memcache.get(key)
        if cached_json is None:
            cached_json = geojson.dumps(
                feature, ensure_ascii=True, check_circular=False,
                allow_nan=True, encoding="utf-8", sort_keys=False)
            if len(cached_json) > 1000000:
                l = len(cached_json)
                group_count = l / 1000000 + 1
                group_size = l / group_count
                groups = range (0, l, group_size) + [l]
                memcache.add(key, 'sharded:%d' % group_count)
                for i in range(1, group_count):
                    g_key = '%s.%d' % (key, i)
                    shard = cached_json[groups[i-1]:groups[i]]
                    memcache.add(g_key, shard, 3600)
            else:
                memcache.add(key, cached_json, 3600)
        elif cached_json.startswith('sharded:'):
            group_count = int(cached_json.split(':')[1])
            shards = []
            for i in range(1,group_count+1):
                g_key = '%s.%d' % (key, i)
                shard = memcache.get(g_key)
                if shard is None:
                    cached_json = geojson.dumps(
                        feature, ensure_ascii=True, check_circular=False,
                        allow_nan=True, encoding="utf-8", sort_keys=False)
                else:
                    shards.append(shard)
                if not cached_json:
                    cached_json = ''.join(shards)
        features.append(cached_json)

    return '{"type":"FeatureCollection","features":[%s]}' % ','.join(features)



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


@app.route('/api/user/me')
def do_me():
    user = jacs.auth.get_user(flask.request.args.get('url'))
    if not 'userid' in user:
        user['status'] = 401
        return build_response(user)
    else:
        return build_response(user)


def build_response(result, method=json.dumps):
    status = 200
    if 'status' in result:
        status = result['status']
    if 'error' in result:
        method = json.dumps
        if status is None:
            status = 500
    return flask.Response(
        response=method(result), mimetype='application/json', status = status)


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
