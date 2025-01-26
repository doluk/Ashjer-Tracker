###############################################################################
# Imports
###############################################################################

import aiogoogle
import datetime
import json
import re
import webbrowser
import wsgiref.simple_server
import wsgiref.util

from aiogoogle import Aiogoogle
from aiogoogle.auth.utils import create_secret
from urllib.parse import quote_plus, unquote_plus
from typing import List


###############################################################################
# Classes
###############################################################################

class AuthEngine(object):
    """class to handle the oauth2 flow
    """

    def __init__(self):
        self.last_request_uri = None
        self._state = create_secret()
        self._success_message = 'The oauth2 flow has completed. You may close this window.'

    def __call__(self, environ, start_response):
        """web server gateway interface callable
        """

        start_response("200 OK", [("Content-type", "text/plain")])
        self.last_request_uri = wsgiref.util.request_uri(environ)
        return [self._success_message.encode("utf-8")]

    def make_server(self):
        """create a local webserver to serve the authentication request
        """

        wsgiref.simple_server.WSGIServer.allow_reuse_address = False
        local_server = wsgiref.simple_server.make_server('localhost', 8080, self)
        return local_server

    def get_auth_url(self, redirect_uri: str, scopes: List[str],
                     client_id: str) -> str:
        """generate the authorization url for the oauth2-request
        Parameters
        ----------
            redirect_uri: string
                the redirect uri
            scopes: list of string
                the requested scopes
            client_id: string
                the client id

        Returns
        -------
            string
                the authorization URL
        """

        scope = '+'.join(quote_plus(s) for s in scopes)
        template = ('https://accounts.google.com/o/oauth2/auth?redirect_uri={}' +
                    '&scope={}&&client_id={}&response_type=code&state={}' +
                    '&access_type=offline&prompt=select_account&include_granted_scopes=true')

        return template.format(quote_plus(redirect_uri), scope, client_id, self._state)

    def get_auth_code(self):
        """get the authorization grant from Google's response
        Returns
        -------
            string
                the auth grant

        Raises
        ------
            ValueError
                if the internal state and the returned state don't match
        """

        # security check: if states don't match, abort
        [returned_state] = re.findall(r'[&?]state=([^&]*)', self.last_request_uri)
        if self._state != returned_state:
            raise ValueError('Returned state does not match own state')

        return unquote_plus(re.findall(r'[&?]code=([^&]*)', self.last_request_uri)[0])


class CredManager:
    def __init__(self, client_cred_path: str, user_cred_path: str):
        self.client_creds = self.client_creds_from_file(client_cred_path)
        self.user_creds = self.user_creds_from_file(user_cred_path)
        self.user_cred_fp = user_cred_path
        self.client_cred_fp = client_cred_path

    def client_creds_from_file(self, fp: str) -> dict:
        """build client credentials from credential file
        Parameters
        ----------
            fp: string
                the file path

        Returns
        -------
            dictionary
                a dict containing the credentials
        """

        try:
            with open(fp) as infile:
                cred_file = json.load(infile)

            creds = {
                'client_id': cred_file['web']['client_id'],
                'client_secret': cred_file['web']['client_secret'],
                'scopes': ['https://www.googleapis.com/auth/drive'],
                'redirect_uri': 'http://localhost:8080/',
            }
        except FileNotFoundError:
            creds = dict()
        return creds

    def user_creds_from_file(self, fp: str) -> dict:
        """build user credentials from credential file
        Parameters
        ----------
            fp: string
                the file path

        Returns
        -------
            dictionary
                a dict containing the credentials
        """

        try:
            with open(fp) as infile:
                creds = json.load(infile)
        except FileNotFoundError:
            creds = dict()

        return creds

    def is_valid(self, token: dict) -> bool:
        """check if a user token is valid
        Parameters
        ----------
            token: dictionary
                the token parameters

        Returns
        -------
            bool
                whether or not the token is valid
        """

        # no access token
        if not token.get('access_token'):
            return False

        # expired?
        return not self.is_expired(token)

    def is_expired(self, token: dict) -> bool:
        """check if a user token is expired
        Parameters
        ----------
            token: dictionary
                the token parameters

        Returns
        -------
            bool
                whether or not the token is expired
        """

        # no expiry date
        expiry = token.get('expires_at')
        if not expiry:
            return False

        # past expiry date?
        expiry = datetime.datetime.strptime(expiry, '%Y-%m-%dT%H:%M:%S.%f')
        return expiry < datetime.datetime.utcnow()

    async def refresh(self):
        """refresh the access token if necessary. If an auto-refresh is impossible, let
        the user authorize the application to generate a new set of credentials
        """

        if not self.user_creds or not self.is_valid(self.user_creds):
            # automatically refresh credentials if possible
            if self.user_creds and self.is_expired(self.user_creds) and \
                    self.user_creds.get('refresh_token'):
                async with aiogoogle.auth.Oauth2Manager() as client:
                    new_token = await client.refresh(user_creds=self.user_creds,
                                                     client_creds=self.client_creds)
                    new_token = new_token[1]
                    self.user_creds['access_token'] = new_token['access_token']
                    self.user_creds['expires_at'] = (datetime.datetime.utcnow() +
                        datetime.timedelta(seconds=new_token['expires_in'] - 120)).isoformat()
            # if there are no (valid) credentials available, let the user log in
            else:
                self.user_creds = await self.oauth2_flow()
                self.user_creds['id_token'] = ''
                self.user_creds['id_token_jwt'] = ''

            # persist the credentials to json
            with open(self.user_cred_fp, 'w') as token:
                json.dump(self.user_creds, token)

    async def oauth2_flow(self) -> dict:
        """perform the oauth2 flow to obtain an access (and refresh) token
        Returns
        -------
            dictionary
                a dict containing the credentials
        """

        local_server = None
        try:
            auth_engine = AuthEngine()

            # oauth2 flow step a: identify client and get redirection URL
            local_server = auth_engine.make_server()
            auth_url = auth_engine.get_auth_url(
                self.client_creds['redirect_uri'], self.client_creds['scopes'],
                self.client_creds['client_id'])

            # oauth2 flow step b: have user authenticate
            webbrowser.open(auth_url, new=1, autoraise=True)
            local_server.handle_request()

            # oauth2 flow step c: get authorization code
            grant = auth_engine.get_auth_code()

            # oauth2 flow step d&e: use auth code to obtain access+refresh-token
            async with Aiogoogle() as aiogoogle:
                full_user_creds = await aiogoogle.oauth2.build_user_creds(
                    grant=grant, client_creds=self.client_creds
                )

            # close the local webserver
            local_server.server_close()
        except:
            if local_server:
                local_server.server_close()
            raise

        return full_user_creds