from antelope.models import ResponseModel, OriginMeta
from .rest_client import RestClient


class XdbRequester(RestClient):
    """
    A RestClient that encapsulates translating the HTTP responses to Pydantic models.  A token is structurally required
    but is still None default (will just give 401 unauthorized).  On initialization, queries API_root/origins to
    determine the set of origins the query knows.

    ref is optional and if supplied, the origin will be automatically prefixed to every query. Escape this behavior
    by using origin_x routes to supply origin explicitly.
    (if None, a client will be expected to specify origin in every query

    {{What *is* the difference between .origin and .ref? anyway? why the absurd machinations around catalog_names?
       ans: something (perhaps superstitious) about provenance- it's about mapping the ref to a 'physical' source }}

    pydantic operations
    use specified ref as origin
     -get_one
     -get_many
     -post_return_one
     -post_return_many
    user supplies origin as a positional parameter
     -origin_get_one
     -origin_get_many
     -origin_post_return_one
     -origin_post_return_many
    """
    def __init__(self, api_root, ref=None, token=None, quiet=False, **kwargs):
        super(XdbRequester, self).__init__(api_root, token=token, quiet=quiet, **kwargs)
        # retrieve origins authorized by this token
        self._origins = sorted((OriginMeta(**k) for k in self._get_endpoint('origins')),
                               key=lambda x: x.origin)
        self._org = ref  # '/'.join([api_root, origin])  # we prepend the API_ROOT now in the parent class

    @property
    def origin(self):
        return self._org

    @property
    def origins(self):
        """
        generates OriginMeta objects obtained by the server when the token was first authenticated
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

    def origin_get_one(self, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._get_endpoint(*args, **kwargs))
        else:
            return model(self._get_endpoint(*args, **kwargs))

    def origin_get_many(self, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._get_endpoint(*args, **kwargs)]
        else:
            return [model(k) for k in self._get_endpoint(*args, **kwargs)]

    def post_return_one(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._post(postdata, self._org, *args, **kwargs))
        else:
            return model(self._post(postdata, self._org, *args, **kwargs))

    def origin_post_return_one(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return model(**self._post(postdata, *args, **kwargs))
        else:
            return model(self._post(postdata, *args, **kwargs))

    def post_return_many(self, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._post(postdata, self._org, *args, **kwargs)]
        else:
            return [model(k) for k in self._post(postdata, self._org, *args, **kwargs)]

    def origin_post_return_many(self, origin, postdata, model, *args, **kwargs):
        if issubclass(model, ResponseModel):
            return [model(**k) for k in self._post(postdata, origin, *args, **kwargs)]
        else:
            return [model(k) for k in self._post(postdata, origin, *args, **kwargs)]
