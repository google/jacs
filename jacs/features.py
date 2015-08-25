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

import decimal
import logging
import json

import geojson
import geomet.wkb
import geomet.wkt
import sqlalchemy
import sqlalchemy.exc

import geometry_util
import auth
import types


class Features(object):
    """Implements the tables endpoint in the REST API.

    This class handles all tables/{db}:{table}/features requests.
    """

    def __init__(self, engine, geometry_field):
        """
        Args:
            engine: A sqlalchemy Cloud SQL engine
            geometry_field: The field that the geometry is stored in. Typically
                it is 'geometry'.
        """
        self._geometry_field = geometry_field
        self._engine = engine
        self._metadata = sqlalchemy.MetaData()

    def initialize_table(self, table):
        return sqlalchemy.Table(
                table, self._metadata,
                sqlalchemy.Column(self._geometry_field, types.Geometry),
                autoload=True, autoload_with=self._engine)

    def list(self, table, select, where,
             limit=None, offset=None, order_by=None, intersects=None):
        """Send the query to the database and return the result as GeoJSON.

        Args:
          table: The Table to use.
          select: A comma-separated list of columns to use. Anything that is
              valid SQL is accepted. This value needs rigorous error checking.
          where: A valid SQL where statement. Also needs a lot of checking.
          limit: The limit the number of returned entries.
          offset: Result offset
          order_by: A valid SQL order by statement.
          intersects: A geometry that the result should intersect. Supports both
              WKT and GeoJSON

        Returns:
          A GeoJSON FeatureCollection representing the returned features, or
              a dict explaining the error.
        """

        if not auth.authorize("read", table):
            return error_message('Unauthorized', status=401)
        features = []
        cols = []

        tbl = self.initialize_table(table)
        primary_key = get_primary_key(tbl)
        select_list = []

        if select:
            select = select.split(",")
            for s in select:
                if s != primary_key.name and s != '':
                    if s in tbl.c:
                        select_list.append(sqlalchemy.sql.column(s))
                    else:
                        select_list.append(sqlalchemy.sql.literal_column(s))
            # Also select geometry if not already selected.
            select_list.append(tbl.c[self._geometry_field])
            select_list.append(primary_key.name)
        else:
            for column in tbl.columns.values():
                select_list.append(column)

        query = sqlalchemy.sql.select(select_list)
        if intersects:
            logging.debug('Exploring the intersects parameter: %s', intersects)
            geometry = geometry_util.parse_geometry(intersects, True)
            if geometry is not None:
                query = query.where(sqlalchemy.sql.expression.func.ST_Intersects(
                    tbl.columns[self._geometry_field], geometry) == True)

        if where:
            where = '(%s)' % where
            query = query.where(sqlalchemy.text(where))

        if limit:
            query = query.limit(limit)

        if order_by:
            query = query.order(order_by)

        if offset:
            query = query.offset(offset)

        # Connect and execute the query
        try:
            connection = self._engine.connect()
            rows = connection.execute(query)
        except sqlalchemy.exc.SQLAlchemyError as e:
            # This error should probably be made better in a production system.
            return error_message('Something went wrong: {}'.format(e))

        # now we read the rows and generate geojson out of them.
        for row in rows:
            wkbgeom = row[self._geometry_field]
            props = {}
            result_columns = row.items()
            for column in result_columns:
                if column[1] is not None and column[0] != self._geometry_field:
                    if isinstance(column[1], decimal.Decimal):
                        props[column[0]] = float(column[1])
                    elif isinstance(column[1], type(u'unicode')):
                        props[column[0]] = column[1].encode('utf-8', 'ignore')
                    else:
                        props[column[0]] = column[1]

            feature_id = props[primary_key.name]
            geom = geomet.wkb.loads(wkbgeom)

            feature = geojson.Feature(geometry=geom, properties=props,
                                      id=feature_id)
            # Add the feature to our list of features.
            features.append(feature)
        # Return the list of features as a FeatureCollection.
        return geojson.FeatureCollection(features)

    def create(self, table, features):
        """ Creates new records in table corresponding to the pass GeoJSON features.

        Args:
          table: The Table to use.
          features: String of GeoJSON features.

        Returns:
            On success an empty dictionary is returned.
            On error, a dictionary with error information is returned.
        """
        if not auth.authorize("write", table):
            return error_message('Unauthorized', status=401)

        #attempt to parse geojson
        try:
            features = geojson.loads(features)['features']
        except ValueError as e:
            return error_message("Unable to parse request data. %s" % (e))

        #loads the table schema from the database
        tbl = self.initialize_table(table)
        data = []
        for index, feature in enumerate(features):
            #Make sure all attributes are columns in the table
            try:
                verify_attributes(tbl.columns, feature['properties'])
            except ValueError as e:
                return error_message(e, index=index)

            properties = feature['properties']
            #Add the geometry field
            properties[self._geometry_field] = geomet.wkt.dumps(feature['geometry'])
            data.append(properties)

        try:
            connection = self._engine.connect()
            transaction = connection.begin()
            connection.execute(tbl.insert(), data)
            transaction.commit()
        except sqlalchemy.exc.SQLAlchemyError as e:
            transaction.rollback()
            return error_message("Database error: %s" % e)
        return []

    def update(self, table, features):
        """ Updates a feature with corresponding values. Only properties of the feature
        that have a value will be updated. If geometry is given, it is replaced.

        Args:
          table: The Table to use.
          features: String of GeoJSON features.
        Returns:
            On success an empty dictionary is returned.
            On error, a dictionary with error information is returned.
        """
        if not auth.authorize("write", table):
            return error_message('Unauthorized', status=401)

        #attempt to parse geojson
        try:
            features = geojson.loads(features)['features']
        except ValueError as e:
            return error_message("Unable to parse request data. %s" % (e))

        tbl = self.initialize_table(table)
        primary_key = get_primary_key(tbl)
        if primary_key is None:
            return error_message('Primary key is not defined for table')
        index = None
        feature_id = None
        try:
            connection = self._engine.connect()
            transaction = connection.begin()
            for index, feature in enumerate(features):
                query = tbl.update()

                feature_id = get_feature_id(primary_key.name, feature)
                if feature_id is None:
                    return error_message("No primary key", index=index)

                #Make sure all attributes are columns in the table
                try:
                    verify_attributes(tbl.columns, feature['properties'])
                except ValueError as e:
                    return error_message(e, index=index, feature_id=feature_id)

                query = query.where(primary_key == feature_id)
                del(feature['properties'][primary_key.name])

                if 'geometry' in feature and feature['geometry'] is not None:
                    feature['properties'][self._geometry_field] = feature['geometry']
                query = query.values(feature['properties'])
                connection.execute(query)
            transaction.commit()
        except sqlalchemy.exc.SQLAlchemyError as e:
            transaction.rollback()
            return error_message(("Database error: %s" % e), feature_id=feature_id, index=index)
        return []

    def delete(self, table, keys, where=None, limit=None, order_by=None):
        """ Deletes all features with id in list of keys

        Args:
          table: The Table to use.
          keys: primary keys to remove
          where: filter to delete matching records
          limit: max records to delete
          order_by: order of records to delete
        Returns:
            On success an empty dictionary is returned.
            On error, a dictionary with error information is returned.
        """
        if not auth.authorize("write", table):
            return error_message('Unauthorized', status=401)
        if keys is not None or where is not None:
            tbl = self.initialize_table(table)
            primary_key = get_primary_key(tbl)

            query = tbl.delete()

            if where is not None:
                query = query.where(sqlalchemy.text(where))
            if limit is not None:
                query = query.limit(limit)
            if order_by is not None:
                query = query.order(order_by)
            if keys is not None:
                query = query.where(primary_key.in_(keys))
            try:
                connection = self._engine.connect()
                transaction = connection.begin()
                connection.execute(query)
                transaction.commit()
            except sqlalchemy.exc.SQLAlchemyError as e:
                transaction.rollback()
                return error_message("Database error: %s" % e)
            return []
        else:
            return error_message("Either list of keys or where statement required")


def get_primary_key(table):
    for c in table.columns:
        if c.primary_key:
            return c
    return None


def get_feature_id(primary_key, feature):
    if 'id' in feature and feature['id'] is not None:
        return feature['id']
    properties = feature['properties']
    if primary_key in properties and properties[primary_key] is not None:
        return feature['properties'][primary_key]
    return None


def error_message(message, feature_id=None,index=None,status=400):
    error = {'error': message, 'status': status}
    if feature_id != None:
        error["feature_id"] = feature_id
    if index != None:
        error["index"] = index
    return error


def verify_attributes(columns, attributes):
    """ Makes sure all attributes are columns in the table

    Args:
        columns: list of columns to verify against
        attributes: attributes to check
    Returns:
        None on success
    """
    for p in attributes:
        if p not in columns:
           raise ValueError("Invalid attribute: %s" % p)
    return None


class ST_Intersects(sqlalchemy.sql.functions.GenericFunction):
    type = sqlalchemy.types.Boolean
