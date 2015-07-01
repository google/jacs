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
from google.appengine.ext import ndb

import flask
import sqlalchemy
import sqlparse
import geojson

import jacs.features
import jacs.auth
import jacs.styles


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


class CacheEntry(ndb.Model):
    cache_key = ndb.StringProperty()


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
        response = build_features_list_response(table, result)
        data = response.get_data()
        if len(data) < 1000000:
            logging.info('adding response to memcache with key %s', flask.request.url)
            memcache.add(flask.request.url, data)
            ancestor_key = ndb.Key("CacheKey", "full_page_keys")
            cache_entry = CacheEntry(parent=ancestor_key, cache_key=flask.request.url)
            cache_entry.put()
    return response


def build_features_list_response(table, result):
    status = 200
    response = ''
    if 'status' in result:
        status = result['status']
    if 'error' in result:
        if status is None:
            status = 500
        response = json.dumps(result)
    features = []
    for feature in result['features']:
        key = '%s:%d' % (table, feature['id'])
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
                    memcache.add(g_key, shard)
            else:
                memcache.add(key, cached_json)
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

    response = '{"type":"FeatureCollection","features":[%s]}' % ','.join(features)
    return flask.Response(
        response=response, mimetype='application/json', status = status)


def clear_page_cache():
    logging.info('Clearing ALL page caches')
    ancestor_key = ndb.Key("CacheKey", "full_page_keys")
    cache_entries = CacheEntry.query(ancestor=ancestor_key).fetch()
    for cache_entry in cache_entries:
        memcache.delete(cache_entry.cache_key)
        logging.info('Cleared cache for %s', cache_entry.cache_key)
        cache_entry.key.delete()



def clear_feature_cache(table, keys):
    if not keys:
        logging.info('Clearing ALL cached features')
        memcache.flush_all()
    else:
        for k in keys:
            key = '%s:%s' % (table, k)
            cache_entry = memcache.get(key)
            logging.info('Checking if feature %s is in cache: %s', key, cache_entry)
            if cache_entry:
                if cache_entry.startswith('sharded:'):
                    group_count = int(cached_json.split(':')[1])
                    shards = []
                    for i in range(1,group_count+1):
                        g_key = '%s.%d' % (key, i)
                        memcache.delete(g_key)
                        logging.info('Cleared cache for %s', g_key)
                memcache.delete(key)
                logging.info('Cleared cache for %s', key)


@app.route('/tables/<table>/features/batchInsert', methods=['POST'])
def do_feature_create(table):
    result = flask.g.features.create(table, flask.request.data)
    clear_page_cache()
    return build_response(result)


@app.route('/tables/<table>/features/batchPatch', methods=['PATCH'])
def do_feature_update(table):
    result = flask.g.features.update(table,flask.request.data)
    clear_page_cache()
    keys = []
    data = {'featires': []}
    try:
        data = json.loads(flask.request.data)
    except ValueError as e:
        pass
    for f in data['features']:
        # we don't know at this point what the PK of this table is,
        # so assume all fields are.
        # TODO: Make this better
        keys = keys + f['properties'].values()
    clear_feature_cache(table, keys)
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

    clear_page_cache()
    clear_feature_cache(table, keys)
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


@app.route('/api/user/me')
def do_me():
    user = jacs.auth.get_user(flask.request.args.get('url'))
    if not 'userid' in user:
        user['status'] = 401
        return build_response(user)
    else:
        return build_response(user)


@app.route('/styles/formpost', methods=['POST'])
def styles_formpost():
    """Saves an uploaded style file, and returns a key to it.

    If the key= URL parameter is given, we use the given key,
    otherwise generate a key."""

    key = flask.request.args.values('name')
    logging.debug('POST style with name %s', name)
    displayRules = flask.request.args.values('displayRules')
    style = jacs.styles.Styles(displayRules=flask.json.loads(displayRules),
                               name=name)
    style.put()
    return flask.Response(response=style.to_json(),
                          mimetype='application/json',
                          status=200)


@app.route('/styles/by-name/<name>', methods=['POST'])
def styles_post_by_name(key):
    """Saves the style of a layer in the NDB with name as a key."""

    logging.debug('POST by-name with name %s', name)
    displayRules = flask.request.get_data()
    style = jacs.styles.Styles(flask.json.loads(displayRules), name=name)
    style.put()
    return flask.Response(response=style.to_json(),
                          mimetype='application/json',
                          status=200)


@app.route('/styles/by-key/<key>', methods=['POST'])
def styles_post_by_key(key):
    """Saves the style of a layer in the NDB with name as a key."""

    logging.debug('POST by-key with key %s', name)
    displayRules = flask.request.get_data()
    style = jacs.styles.Styles(flask.json.loads(displayRules), key=key)
    style.put()
    return flask.Response(response=style.to_json(),
                          mimetype='application/json',
                          status=200)


@app.route('/styles/by-key/<key>', methods=['GET'])
def styles_get_by_key(key):
    """Gets the style json by the key."""
    style = jacs.styles.Styles(key=key)
    if not style.found:
        return flask.Response(response='invalid key',
                              mimetype='text/plain',
                              status=404)

    return flask.Response(response=style.to_json(),
                          mimetype='application/json',
                          status=200)


@app.route('/styles/by-key/<key>/declarative', methods=['GET'])
def styles_get_declarative_by_key(key):
    """Generate a JS function to style a feature."""
    style = jacs.styles.Styles(key=key)
    if not style.found:
        return flask.Response(response='invalid key',
                              mimetype='text/plain',
                              status=404)

    return flask.Response(response=style.to_js(),
                          mimetype='application/javascript',
                          status=200)


@app.route('/styles/by-name/<name>', methods=['GET'])
def styles_get_by_name(name):
    """Gets the style json by the key."""
    style = jacs.styles.Styles(name=name)
    if not style.found:
        return flask.Response(response='invalid key',
                              mimetype='text/plain',
                              status=404)

    return flask.Response(response=style.to_json(),
                          mimetype='application/json',
                          status=200)


@app.route('/styles/by-name/<name>/declarative', methods=['GET'])
def styles_get_declarative_by_name(name):
    """Generate a JS function to style a feature."""
    style = jacs.styles.Styles(name=name)
    if not style.found:
        return flask.Response(response='invalid key',
                              mimetype='text/plain',
                              status=404)

    return flask.Response(response=style.to_js(),
                          mimetype='application/javascript',
                          status=200)


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
