import requests
from requests.exceptions import HTTPError
import json
from time import time
from pydantic import BaseModel


class OAuthToken(BaseModel):
    token_type: str
    access_token: str

    @property
    def auth(self):
        return '%s %s' % (self.token_type, self.access_token)


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

    def __init__(self, api_root, token=None, quiet=False, auth_route=None):
        self._s = requests.Session()
        self._s.headers['Accept'] = "application/json"
        self._quiet = quiet
        if auth_route:
            self.auth_route = auth_route
        if token:
            self.set_token(token)

        while api_root[-1] == '/':
            api_root = api_root[:-1]  # strip trailing /

        self._api_root = api_root

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
        elif isinstance(token, str):
            toks = token.split(' ')
            if len(toks) == 1:
                self._token = OAuthToken(token_type='bearer', access_token=token)
            elif len(toks) == 2:
                self._token = OAuthToken(token_type=toks[0], access_token=toks[1])
            else:
                raise ValueError('invalid token specification')
        else:
            raise ValueError('Invalid token type')
        self._s.headers['Authorization'] = self._token.auth

    def authenticate(self, username, password, **kwargs):
        """
        POSTs an OAuth2-compliant form to obtain a bearer token.
        Be sure to set the 'auth_route' property either in a subclass or manually (e.g. on init)
        :param username:
        :param password:
        :param kwargs:
        :return:
        """
        data = {
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        data.update(kwargs)
        self.set_token(self.post_return_one(data, OAuthToken, self.auth_route, form=True))

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
            raise HTTPError(resp.status_code, resp)
        return json.loads(resp.content)

    def _get_endpoint(self, route, *args, **params):
        url = '/'.join([route, *args])
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

    def _post(self, postdata, route, form=False, *args, **params):
        url = '/'.join([route, *args])
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

    def put(self, putdata, model, form=False, *args, **kwargs):
        url = '/'.join(args)
        if form:
            response = self._request('PUT', url, data=putdata, params=kwargs)
        else:
            response = self._request('PUT', url, json=putdata, params=kwargs)
        if issubclass(model, BaseModel):
            return model(**response)
        else:
            return model(response)

    def delete(self, *args, **kwargs):
        url = '/'.join(args)
        return self._request('DELETE', url, params=kwargs)
