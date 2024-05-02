import requests
from typing import Optional
from requests.exceptions import HTTPError
import json
from time import time
from pydantic import BaseModel
import getpass


class OAuthToken(BaseModel):
    token_type: str
    access_token: str
    message: Optional[str] = None

    @property
    def auth(self):
        return '%s %s' % (self.token_type, self.access_token)


class NoCredentials(Exception):
    pass


class RestClient(object):
    """
    A REST client that uses pydantic models to interpret response data
    """

    auth_route = 'login'
    _token = None

    def _print(self, *args, cont=False):
        if not self._quiet:
            if cont:  # continue the same line
                print(*args, end=".. ")
            else:
                print(*args)

    def __init__(self, api_root, token=None, quiet=False, auth_route=None, save_credentials=True, verify=None):
        self._s = requests.Session()
        if verify:
            self._s.verify = verify
        self._s.headers['Accept'] = "application/json"
        self._quiet = quiet
        if auth_route:
            self.auth_route = auth_route
        if token:
            self.set_token(token)

        self._save = bool(save_credentials)
        self._creds = None

        while api_root[-1] == '/':
            api_root = api_root[:-1]  # strip trailing /

        self._api_root = api_root

    @property
    def saved(self):
        return self._save and bool(self._creds)

    def close(self):
        self._s.close()

    def set_token(self, token):
        """
        we accept:
        - a string with a plain token
        - a string with 'token_type token'
        - an OauthToken
        :param token:
        :return:
        """
        if isinstance(token, OAuthToken):
            self._token = token
            if token.message:
                print(token.message)
        elif isinstance(token, str):
            toks = token.split(' ')
            if len(toks) == 1:
                self._token = OAuthToken(token_type='bearer', access_token=token)
            elif len(toks) == 2:
                self._token = OAuthToken(token_type=toks[0], access_token=toks[1])
            else:
                raise ValueError('invalid token specification')
        else:
            raise ValueError('Invalid token type %s' % type(token))
        self._s.headers['Authorization'] = self._token.auth

    def _upd_save(self, save_credentials):
        """
        Updates the flag indicating whether we are saving credentials. None = do nothing. clears the cache if false.
        :param save_credentials:
        :return:
        """
        if save_credentials is None:
            return
        else:
            self._save = bool(save_credentials)
        if not self._save:
            self._creds = None

    def reauthenticate(self, save_credentials=None, **kwargs):
        """
        If we have saved credentials, pass them along. update the flag
        :param save_credentials:
        :param kwargs:
        :return:
        """
        if self._creds is None:
            raise NoCredentials
        data = self._creds
        data.update(kwargs)
        self._upd_save(save_credentials)
        self._post_credentials(data)

    def authenticate(self, username, password=None, save_credentials=None, **kwargs):
        """
        POSTs an OAuth2-compliant form to obtain a bearer token.
        Be sure to set the 'auth_route' property either in a subclass or manually (e.g. on init)
        :param username:
        :param password:
        :param save_credentials: whether to save credentials
        :param kwargs:
        :return:
        """
        if password is None:
            password = getpass.getpass('Enter password for user %s: ' % username)
        data = {
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        data.update(kwargs)
        self._upd_save(save_credentials)
        self._post_credentials(data)

    def _post_credentials(self, data):
        """
        Post the credentials, and save them if the request was successful and if thje save flag is set
        :param data:
        :return:
        """
        if not self._save:
            self._creds = None
        self.set_token(self.post_return_one(data, OAuthToken, self.auth_route, form=True))
        if self._save:
            self._creds = data

    def _request(self, verb, route, **kwargs):
        """
        Returns JSON-translated content
        :param verb:
        :param route:
        :param kwargs:
        :return:
        """
        url = '/'.join([self._api_root, route])
        endp = {
            'GET': self._s.get,
            'PUT': self._s.put,
            'POST': self._s.post,
            'PATCH': self._s.patch,
            'DELETE': self._s.delete
        }[verb]
        self._print('%s %s' % (verb, url), cont=True)
        t = time()
        resp = endp(url, **kwargs)
        el = time() - t
        self._print('%d [%.2f sec]' % (resp.status_code, el))
        if resp.status_code >= 400:
            raise HTTPError(resp.status_code, resp.content)
        return json.loads(resp.content)

    def _get_endpoint(self, route, *args, **params):
        url = '/'.join(map(str, filter(None, [route, *args])))
        return self._request('GET', url, params=params)

    def get_raw(self, *args, **kwargs):
        return self._get_endpoint(*args, **kwargs)

    def get_one(self, model, *args, **kwargs):
        if issubclass(model, BaseModel):
            return model(**self._get_endpoint(*args, **kwargs))
        else:
            return model(self._get_endpoint(*args, **kwargs))

    def get_many(self, model, *args, **kwargs):
        if issubclass(model, BaseModel):
            return [model(**k) for k in self._get_endpoint(*args, **kwargs)]
        else:
            return [model(k) for k in self._get_endpoint(*args, **kwargs)]

    def _post(self, postdata, route, *args, form=False, **params):
        url = '/'.join(map(str, [route, *args]))
        if form:
            return self._request('POST', url, data=postdata, params=params)
        else:
            return self._request('POST', url, json=postdata, params=params)

    def post_return_one(self, postdata, model, *args, **kwargs):
        if issubclass(model, BaseModel):
            return model(**self._post(postdata, *args, **kwargs))
        else:
            return model(self._post(postdata, *args, **kwargs))

    def post_return_many(self, postdata, model, *args, **kwargs):
        if issubclass(model, BaseModel):
            return [model(**k) for k in self._post(postdata, *args, **kwargs)]
        else:
            return [model(k) for k in self._post(postdata, *args, **kwargs)]

    def put(self, putdata, model, *args, form=False, **kwargs):
        url = '/'.join(map(str, args))
        if form:
            response = self._request('PUT', url, data=putdata, params=kwargs)
        else:
            response = self._request('PUT', url, json=putdata, params=kwargs)
        if issubclass(model, BaseModel):
            return model(**response)
        else:
            return model(response)

    def delete(self, *args, **kwargs):
        url = '/'.join(map(str, args))
        return self._request('DELETE', url, params=kwargs)
