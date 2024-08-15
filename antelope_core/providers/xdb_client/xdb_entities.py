from antelope import BaseEntity, CatalogRef, EntityNotFound
from antelope.models import Entity, FlowEntity, ReferenceExchange

import re


class XdbReferenceRequired(Exception):
    """
    Straight-up entities have no capabilities
    """
    pass


class XdbEntity(BaseEntity):

    is_entity = True  # haha, all lies!

    def __init__(self, model):
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
        """
        assert issubclass(type(model), Entity), 'model is not a Pydantic Entity (%s)' % type(model)
        self._model = model
        self._ref = None

    @property
    def ref(self):
        if self._ref is None:
            raise XdbReferenceRequired
        return self._ref

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

    def __setitem__(self, key, value):
        if self._ref:
            self._ref[key] = value
        self._model.properties[key] = value

    def __getitem__(self, item):
        if self._ref:
            return self._ref[item]
        return self._model.properties[item]

    def make_ref(self, query):
        if self._ref is not None:
            return self._ref

        args = {k: v for k, v in self._model.properties.items()}
        if self.entity_type == 'quantity' and 'unit' in args:
            args['reference_entity'] = args.pop('unit')
        elif self.entity_type == 'flow':
            if 'referenceQuantity' in args:
                rq = args.pop('referenceQuantity')
                try:
                    args['reference_entity'] = query.get_canonical(rq)
                except EntityNotFound:
                    args['reference_entity'] = query.get(rq)
            if isinstance(self._model, FlowEntity):
                args['context'] = self._model.context
                args['locale'] = self._model.locale
                # query._tm.add_context(self._model.context, origin=self._model.origin)  # this is somewhat cursed
        elif self.entity_type == 'process':
            if 'referenceExchange' in args:
                # we cannot synthesize RxRefs prior to the existence of the ProcessRef. sorry.
                rxs = args.pop('referenceExchange')
                try:
                    args['referenceExchange'] = [ReferenceExchange(**k) for k in rxs]
                except TypeError:
                    print(self.link)
                    print(rxs)
                    raise

        if self.origin != query.origin:
            args['masquerade'] = self.origin

        ref = CatalogRef.from_query(self.external_ref, query, self.entity_type, **args)
        if ref.entity_type == 'flow':
            if any(bool(re.search('carbon.dioxide', k, flags=re.I)) for k in ref.synonyms):
                print('%s ***** CO2' % ref.link)
                ref.is_co2 = True

        self._ref = ref
        return ref

    def has_lcia_engine(self):
        if self._ref is not None:
            return self._ref.has_lcia_engine()
        return False
