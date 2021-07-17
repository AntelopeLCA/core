from antelope import BaseEntity, CatalogRef
from antelope_core.models import Entity


class XdbReferenceRequired(Exception):
    """
    Straight-up entities have no capabilities
    """
    pass


class XdbEntity(BaseEntity):

    is_entity = True

    def __init__(self, model, local):
        """
        Must supply the pydantic model that comes out of the query, and also the archive that stores the ref
        :param model:
        :param local:
        """
        assert issubclass(type(model), Entity), 'model is not a Pydantic Entity (%s)' % type(model)
        self._model = model
        self._local = local

    @property
    def reference_entity(self):
        raise XdbReferenceRequired

    @property
    def entity_type(self):
        return self._model.entity_type

    @property
    def origin(self):
        return self._model.origin

    @property
    def external_ref(self):
        return self._model.entity_id

    def properties(self):
        for k in self._model.properties:
            yield k

    def make_ref(self, query):
        if self._local[self.external_ref]:
            return self._local[self.external_ref]
        ref = CatalogRef.from_query(self.external_ref, query, self.entity_type, **self._model.properties)
        self._local.add(ref)
        return ref
