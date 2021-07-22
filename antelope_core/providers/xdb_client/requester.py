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

    def __init__(self, api_root, origin=None, quiet=False):
        self._s = requests.Session()
        self._quiet = quiet

        if api_root[-1] == '/':
            api_root = api_root[:-1]

        if origin:
            self._org = '/'.join([api_root, origin])
            self._origins = sorted((OriginMeta(**k) for k in self._get_endpoint(self._org)),
                                   key=lambda x: len(x.origin))
        else:
            self._org = api_root
            self._origins = sorted((OriginMeta(**k) for origin in self._get_endpoint(api_root, 'origins')
                                    for k in self._get_endpoint(api_root, origin)),
                                   key=lambda x: x.origin)

        self._qdb = '/'.join([api_root, 'qdb'])

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
        for org in self._origins:
            yield org

    def _get_endpoint(self, base, *args, **params):
        url = '/'.join([base, *args])
        self._print('GET %s' % url, cont=True)
        t = time()
        resp = self._s.get(url, params=params)
        el = time() - t
        self._print('%d [%.2f sec]' % (resp.status_code, el))
        if resp.status_code >= 400:
            raise HttpError(resp.status_code, resp.content)
        return json.loads(resp.content)

    def get_raw(self, *args, **kwargs):
        return self._get_endpoint(self._org, *args, **kwargs)

    def get_one(self, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._get_endpoint(self._org, *args, **kwargs))
        else:
            return model(self._get_endpoint(self._org, *args, **kwargs))

    def get_many(self, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._get_endpoint(self._org, *args, **kwargs)]
        else:
            return [model(k) for k in self._get_endpoint(self._org, *args, **kwargs)]

    def qdb_get_one(self, model, *args, **kwargs):
            return model(**self._get_endpoint(self._qdb, *args, **kwargs))

    def qdb_get_many(self, model, *args, **kwargs):
            return [model(**k) for k in self._get_endpoint(self._qdb, *args, **kwargs)]

    def _post_qdb(self, postdata, *args, **params):
        url = '/'.join([self._qdb, *args])
        self._print('POST %s', cont=True)
        t = time()
        resp = self._s.post(url, json=postdata, params=params)
        el = time() - t
        self._print('%d [%.2f sec]' % (resp.status_code, el))
        if resp.status_code >= 400:
            raise HttpError(resp.status_code, resp.content)
        return json.loads(resp.content)

    def post_return_one(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._post_qdb(postdata, *args, **kwargs))
        else:
            return model(self._post_qdb(postdata, *args, **kwargs))

    def post_return_many(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._post_qdb(postdata, *args, **kwargs)]
        else:
            return [model(k) for k in self._post_qdb(postdata, *args, **kwargs)]

