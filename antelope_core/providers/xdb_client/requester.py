from antelope_core.models import ResponseModel, OriginMeta
from .rest_client import RestClient


class XdbRequester(RestClient):
    def __init__(self, api_root, origin=None, token=None, quiet=False):
        super(XdbRequester, self).__init__(api_root, token=token, quiet=quiet)

        # I don't understand what's going on here
        if origin:
            self._org = origin  # '/'.join([api_root, origin])  # we prepend the API_ROOT now in the parent class
            self._origins = sorted((OriginMeta(**k) for k in self._get_endpoint(self._org)),
                                   key=lambda x: len(x.origin))
        else:
            # this does not seem valid, given all the commentary about "prepend the API_ROOT"
            self._org = api_root
            self._origins = sorted((OriginMeta(**k) for origin in self._get_endpoint(api_root, 'origins')
                                    for k in self._get_endpoint(api_root, origin)),
                                   key=lambda x: x.origin)

        self._qdb = 'qdb'  # '/'.join([api_root, 'qdb'])  # we prepend the API_ROOT now in the parent class

    @property
    def origin(self):
        return self._org

    @property
    def origins(self):
        """
        Returns OriginMeta data-- this should probably include config information !
        :return:
        """
        for org in self._origins:
            yield org

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
        return self._post(postdata, self._qdb, *args, **params)

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

