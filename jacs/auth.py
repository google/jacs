# Copyright 2015 Google Inc. All rights reserved.
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

from google.appengine.api import users
from google.appengine.api import oauth
import logging
import os

def authorize(action, table):

    oauth_user = None
    oauth_admin = None
    try:
        oauth_user = oauth.get_current_user('https://www.googleapis.com/auth/plus.me')
        oauth_admin = oauth.is_current_user_admin('https://www.googleapis.com/auth/plus.me')
    except oauth.OAuthRequestError, e:
        logging.debug("No valid oauth credentials were received: %s" % e)

    logging.info("Authorize user: %s, is admin: %s, action: %s, table: %s, oauth: %s, oauth_admin: %s" % (users.get_current_user(), users.is_current_user_admin(), action, table, oauth_user, oauth_admin))
    logging.info("URL: %s" % users.create_login_url('/client/auth.html'))
    
    if action == "read":
        return True 
    else:
        if users.is_current_user_admin():
            return True
        return False

def get_user(url):
    """Return the current logged in user, if logged in, otherwise None."""
    user = users.get_current_user()
    if user:
        return {
            'userid': user.nickname(),
            'admin': users.is_current_user_admin()
            }
    else:
        return {
            'url': users.create_login_url(url),
            'admin': False
            }
