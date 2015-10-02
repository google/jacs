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
    displayRules = ndb.JsonProperty()
    name = ndb.StringProperty()


class Styles(object):
    """Implements the /styles/ API enpoint.

    The styles API allows users to transition their GME layer styles to JACS.
    """
    def __init__(self, displayRules=None, name=None, key=None):
        self.style = None
        self.found = False
        if displayRules:
            if name:
                self.style = Style(displayRules=displayRules, name=name, id=name)
                self.found = True
            elif key:
                style_key = ndb.Key(urlsafe=key)
                if style_key:
                    self.found = True
                    self.style = style_key.get()
                    self.style.displayRules = displayRules
            else:
                self.style = Style()
                self.style.displayRules = displayRules
                self.style.name = self.style.key.id()
        elif name:
            self.style = Style.query(Style.name == name).get()
            self.found = True
        elif key:
            style_key = ndb.Key(urlsafe=key)
            if style_key:
                self.found = True
                self.style = style_key.get()

    def put(self):
        self.style.put()

    def to_json(self):
        return flask.json.dumps({
            'name': self.style.name,
            'id': self.style.key.urlsafe(),
            'displayRules': self.style.displayRules
        })

    def to_js(self):
        js = []
        style = self.style.displayRules
        if 'type' in style and style['type'] == 'displayRule':
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
                        style_obj['icon'] = (
                            'https://chart.googleapis.com/chart?'
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
                            style_obj['fillColor'] =  (
                                polygonOptions['fill']['color'])
                        if 'opacity' in polygonOptions['fill']:
                            style_obj['fillOpacity'] = (
                                polygonOptions['fill']['opacity'])
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
            self.style.name, '\n'.join(js))]
        return '\n'.join(ret)


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
