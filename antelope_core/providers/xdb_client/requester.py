from antelope.models import ResponseModel, OriginMeta
from .rest_client import RestClient


class XdbRequester(RestClient):
    """
    A RestClient that encapsulates translating the HTTP responses to Pydantic models
     -get_one
     -get_many
     -qdb_get_one
     -qdb_get_many
     -post_return_one
     -post_return_many
    """
    def __init__(self, api_root, ref=None, token=None, quiet=False):
        super(XdbRequester, self).__init__(api_root, token=token, quiet=quiet)

        if ref:
            # we make a list of all the endpoint's origins that match our ref
            self._org = ref  # '/'.join([api_root, origin])  # we prepend the API_ROOT now in the parent class
            self._origins = sorted((OriginMeta(**k) for k in self._get_endpoint(self._org)),
                                   key=lambda x: len(x.origin))
        else:
            # we make a list of all the endpoint's origins- user just has to supply origin as an argument
            self._org = ''
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
        This generates OriginMeta cached from the server for origins granted by our token
        Returns OriginMeta data-- this should probably include config information !
        :return:
        """
        for org in self._origins:
            yield org

    @property
    def is_lcia_engine(self):
        return any(k.is_lcia_engine for k in self._origins)

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
            return model(**self._post(postdata, self._org, *args, **kwargs))
        else:
            return model(self._post(postdata, self._org, *args, **kwargs))

    def qdb_post_return_one(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._post_qdb(postdata, *args, **kwargs))
        else:
            return model(self._post_qdb(postdata, *args, **kwargs))

    def post_return_many(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._post(postdata, self._org, *args, **kwargs)]
        else:
            return [model(k) for k in self._post(postdata, self._org, *args, **kwargs)]

    def qdb_post_return_many(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._post_qdb(postdata, *args, **kwargs)]
        else:
            return [model(k) for k in self._post_qdb(postdata, *args, **kwargs)]
