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
import json
import logging

import geojson

import geomet.wkb
import geomet.wkt



def parse_geometry(geometry_raw, rewrite_circle=False):


    geometry_statement = None
    # is it WKT?
    try:
        geometry_statement = "GeomFromText('%s')" % (
            geomet.wkt.dumps(geomet.wkt.loads(geometry_raw)))
    except ValueError as err:
        logging.debug('    ... not WKT')
    # is it GeoJSON?
    if not geometry_statement:
        try:
            geometry_statement = "GeomFromText('%s')" % (
                geomet.wkt.dumps(geojson.loads(geometry_raw)))
        except ValueError as err:
            logging.debug('    ... not GeoJSON')
    if not geometry_statement and rewrite_circle and 'CIRCLE' in geometry_raw:
        # now see if it a CIRCLE(long lat, rad_in_m)
        re_res = re.findall(
            r'CIRCLE\s*\(\s*([0-9.]+)\s+([0-9.]+)\s*,\s*([0-9.]+)\s*\)',
            geometry_raw)
        if len(re_res[0]) == 3:
            lng = float(re_res[0][0])
            lat = float(re_res[0][1])
            rad = float(re_res[0][2])
            geometry_statement = ('Buffer(POINT(%f, %f), %f)' %
                      (lng, lat, (rad/1000/111.045)))
            logging.debug('%s becomes %s', geometry_raw,
                          geometry_statement)
        else:
            logging.warn('ignoring malformed intersects statement:%s',
                         geometry_raw)
    return geometry_statement





