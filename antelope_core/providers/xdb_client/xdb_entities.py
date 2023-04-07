from antelope import BaseEntity, CatalogRef
from antelope.models import Entity, FlowEntity, ReferenceExchange


class XdbReferenceRequired(Exception):
    """
    Straight-up entities have no capabilities
    """
    pass


class XdbEntity(BaseEntity):

    is_entity = True  # haha, all lies!

    def __init__(self, model, local):
        """
        XdbEntity is an ephemeral object, basically a way of storing pydantic models PRIOR to their getting made into
        refs (which is done by this class's make_ref() process)

        XdbEntities are instantiated by the client on any occasion the client receives info on entities back from
        the remote server: in XdbClient._fetch(), and in processes() flows() quantities() and anything to do with
        exchanges.

        The return objects are immediately used as arguments for BasicQuery.make_ref() or CatalogQuery.make_ref(),
        either of which calls this class's make_ref() with the query as argument.  Then the make_ref() is responsible
        for constructing the fully-featured reference object that is stored in the local archive.

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
        args = {k: v for k, v in self._model.properties.items()}
        if self.entity_type == 'quantity' and 'referenceUnit' in args:
            args['reference_entity'] = args.pop('referenceUnit')
        elif self.entity_type == 'flow':
            if 'referenceQuantity' in args:
                args['reference_entity'] = query.get(args.pop('referenceQuantity'))
            if isinstance(self._model, FlowEntity):
                args['context'] = self._model.context
                args['locale'] = self._model.locale
        elif self.entity_type == 'process':
            if 'referenceExchange' in args:
                # we cannot synthesize RxRefs prior to the existence of the ProcessRef. sorry.
                args['referenceExchange'] = [ReferenceExchange(**k) for k in args.pop('referenceExchange')]

        ref = CatalogRef.from_query(self.external_ref, query, self.entity_type, **args)
        self._local.add(ref)
        return ref
