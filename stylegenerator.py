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
"""Creates a JS function to be used with Maps API Data Layer declarative rules.
"""
import logging
import json

import flask

app = flask.Flask(__name__)

from google.appengine.ext import ndb


class Style(ndb.Model):
    """Models a GME style."""
    data = ndb.TextProperty()

def parse_stroke(stroke):
    """Parse a stroke JSON object and return a Data layer style object"""
    style_obj = {}
    if 'color' in stroke:
        style_obj['strokeColor'] = stroke['color']
    if 'opacity' in stroke:
        style_obj['strokeOpacity'] = stroke['opacity']
    if 'width' in stroke:
        style_obj['strokeWeight'] = stroke['width']
    return style_obj


@app.route('/styles/upload', methods=['POST'])
def style_upload():
    """Saves an uploaded style file, and returns a key to it.

    If the key= URL parameter is given, we use the given key,
    otherwise generate a key."""

    key = flask.request.args.values('key')
    logging.debug('POST style with key %s', key)
    style_data = flask.request.values('style')
    style = create_update_style(style_data, key=key)
    return flask.Response(response=style.key.urlsafe(),
                          mimetype='text/plain',
                          status=200)


@app.route('/styles/save/<key>', methods=['POST'])
def style_put(key):
    """Saves the style of a layer in the NDB with key as a key."""

    logging.debug('PUT with key %s', key)
    style_data = flask.request.get_data()
    style = create_update_style(style_data, key=key)
    return flask.Response(response=style.key.urlsafe(),
                          mimetype='text/plain',
                          status=200)


def create_update_style(data, key=None):
    """Creates a new Style object, saves it to the ndb, and returns.

    If given a key, that key will be used, and thus the style
    can be updated by saving on the same key.
    """
    style = None
    if key:
        style = Style(data=data, id=key)
    else:
        style = Style(data=data)
    style.put()
    logging.debug('Saved style: %s as %s', key, style.data)
    return style


@app.route('/styles/<key>', methods=['GET'])
def style_get(key):
    """Gets the style json by the key."""
    style_key = ndb.Key(urlsafe=key)
    if not style_key:
        return flask.Response(response='invalid url safe key',
                              mimetype='text/plain',
                              status=404)
    style_data = style_key.get().data
    if not style_data:
        return flask.Response(response='empty style',
                              mimetype='text/plain',
                              status=404)
    return flask.Response(response=style_data,
                          mimetype='application/json',
                          status=200)


@app.route('/styles/<key>/parser', methods=['GET'])
def style_parser(key):
    """Generate a JS function to style a feature"""
    style_key = ndb.Key(urlsafe=key)
    if not style_key:
        return flask.Response(response='invalid key',
                              mimetype='text/plain',
                              status=404)
    style_data = style_key.get().data
    if not style_data:
        return flask.Response(response='empty style',
                              mimetype='text/plain',
                              status=404)
    style = json.loads(style_data)
    js = []
    if "type" in style and style['type'] == "displayRule":
        for rule in style['displayRules']:
            min = rule['zoomLevels']['min']
            max = rule['zoomLevels']['max']
            style_obj = {}
            # point geometries
            if 'pointOptions' in rule:
                pointOptions = rule['pointOptions']
                # TODO: support shape
                if 'icon' in pointOptions:
                    # just set a blue marker with a red star
                    style_obj['icon'] = ('https://chart.googleapis.com/chart?'
                                         'chst=d_map_xpin_letter&'
                                         'chld=pin_star|+|00FFFF|000000|FF0000')
                if 'label' in pointOptions:
                    # TODO: make the text be the marker...
                    # can use chart API for instance
                    if 'column' in pointOptions['label']:
                        style_obj['title'] = 'feature.getProperty("%s")' % (
                        pointOptions['label']['column'])
            # line geometries
            if 'lineOptions' in rule:
                style_obj.update(parse_stroke(rule['lineOptions']['stroke']))
            # polygon geometries
            if 'polygonOptions' in rule:
                polygonOptions = rule['polygonOptions']
                if 'fill' in polygonOptions:
                    if 'color' in polygonOptions['fill']:
                        style_obj['fillColor'] = polygonOptions['fill']['color']
                    if 'opacity' in polygonOptions['fill']:
                        style_obj['fillOpacity'] = polygonOptions['fill']['opacity']
                if 'stroke' in polygonOptions:
                    style_obj.update(parse_stroke(polygonOptions['stroke']))
            filters = [
                '(map.getZoom() >= %d)' % min,
                '(map.getZoom() <= %d)' % max]
            for filter in rule['filters']:
                if filter['operator'] == 'startsWith':
                    rule = (
                        'feature.getProperty("%s").lastIndexOf("%s",0) === 0' % (
                            filter['column'], filter['value']))
                elif filter['operator'] == 'endsWith':
                    rule = ('feature.getProperty("%s").length - '
                            'feature.getProperty("%s").lastIndexOf("%s") === '
                            '"%s".length' % (
                                filter['column'], filter['column'],
                                filter['value'], filter['value']))
                elif filter['operator'] == 'contains':
                    rule = 'feature.getProperty("%s").indexOf("%s") > 0' % (
                        filter['column'], filter['value'])
                else:
                    rule = 'feature.getProperty("%s") %s "%s"' % (
                        filter['column'], filter['operator'], filter['value'])
                filters.append('(%s)' % rule)
                condition = ' && '.join(filters)
            js.append("  if(%s) {\n    return %s;\n  }" % (
        condition, json.dumps(style_obj, sort_keys=True)))
    ret = [
        'var jacs = jacs || {};',
        'jacs.setStyle_%s = function(feature){\n%s\n}' % (
        style_key.id(), '\n'.join(js))]

    return flask.Response(response='\n'.join(ret),
                          mimetype='application/javascript',
                          status=200)
