import requests
import json
from time import time
from antelope_core.models import ResponseModel, OriginMeta


class HttpError(Exception):
    """
    More than one origin
    """
    pass


class XdbRequester(object):
    def _print(self, *args, cont=False):
        if not self._quiet:
            if cont:  # continue the same line
                print(*args, end=".. ")
            else:
                print(*args)

    def __init__(self, api_root, origin, quiet=False):
        self._s = requests.Session()
        self._quiet = quiet

        if api_root[-1] == '/':
            api_root = api_root[:-1]

        self._root = '/'.join([api_root, origin])
        self._orgs = sorted((OriginMeta(**k) for k in self._get_endpoint()), key=lambda x: len(x.origin))

        self._origin = origin

    @property
    def origin(self):
        return self._origin

    @property
    def origins(self):
        """
        Returns OriginMeta data-- this should probably include config information !
        :return:
        """
        for org in self._orgs:
            yield org

    def _get_endpoint(self, *args, **params):
        url = '/'.join([self._root, *args])
        self._print('Fetching %s' % url, cont=True)
        t = time()
        resp = self._s.get(url, params=params)
        el = time() - t
        self._print('%d [%.2f sec]' % (resp.status_code, el))
        if resp.status_code >= 400:
            raise HttpError(resp.status_code, resp.content)
        return json.loads(resp.content)

    def get_one(self, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._get_endpoint(*args, **kwargs))
        else:
            return model(self._get_endpoint(*args, **kwargs))

    def get_many(self, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._get_endpoint(*args, **kwargs)]
        else:
            return [model(k) for k in self._get_endpoint(*args, **kwargs)]
