import requests
from requests.structures import CaseInsensitiveDict
import json
from time import time
from pydantic import BaseModel


class OAuthToken(BaseModel):
    token_type: str
    access_token: str


class HttpError(Exception):
    """
    Escalate HTTP errors
    """
    pass


class RestClient(object):
    """
    A REST client that uses pydantic models to interpret response data
    """

    auth_route = 'login'

    def _print(self, *args, cont=False):
        if not self._quiet:
            if cont:  # continue the same line
                print(*args, end=".. ")
            else:
                print(*args)

    def __init__(self, api_root, token=None, quiet=False):
        self._s = requests.Session()
        self._quiet = quiet
        self._token = token

        while api_root[-1] == '/':
            api_root = api_root[:-1]  # strip trailing /

        self._api_root = api_root

    def authenticate(self, username, password, **kwargs):
        """
        POSTs an OAuth2-compliant form to obtain a bearer token.
        Be sure to set the 'auth_route' property either in a subclass or manually
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
        self._token = self.post_return_one(data, OAuthToken, self.auth_route, form=True)

    @property
    def headers(self):
        h = CaseInsensitiveDict()
        h["Accept"] = "application/json"
        if self._token is not None:
            if isinstance(self._token, OAuthToken):
                _token = self._token.dict()
                ttype = _token.get('token_type', None)
                tok = _token.get('access_token')
                if ttype == 'bearer':
                    auth = "Bearer %s" % tok
                else:
                    # TODO: make this support other kinds of authentication
                    raise TypeError('Unknown token type %s' % ttype)
            elif isinstance(self._token, str):
                auth = "Bearer %s" % self._token
            else:
                raise TypeError('Unrecognized token type %s' % type(self._token))
            h["Authorization"] = auth
        return h

    def _request(self, verb, route, **kwargs):
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
        resp = endp(url, headers=self.headers, **kwargs)
        el = time() - t
        self._print('%d [%.2f sec]' % (resp.status_code, el))
        if resp.status_code >= 400:
            raise HttpError(resp.status_code, resp.content)
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
