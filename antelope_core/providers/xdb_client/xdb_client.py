"""
Client for the xdb Antelope server

The concept here is to pass received requests straight into the Pydantic models, and then use those for easy
(though manual) deserialization into EntityRefs.
"""
from synonym_dict import LowerDict

from antelope import EntityNotFound
from antelope_core.archives import LcArchive, InterfaceError
from antelope_core.models import Context as ContextModel, Entity
from antelope_core.catalog_query import READONLY_INTERFACE_TYPES
from antelope_core.contexts import NullContext, Context as CoreContext

from .requester import XdbRequester, HttpError
from .implementation import XdbImplementation
from .xdb_entities import XdbEntity

class XdbTermManager(object):
    def __init__(self, requester):
        """
        The key question here: should I at least cache remote contexts? for now let us cache nothing

        Except we DO need to cache contexts, or at least create them on the fly (and if we are going to create
        them, we should cache them) because exterior exchanges need to terminate properly in contexts.

        SO: we will establish the condition that Xdb MUST only use contexts' canonical names, thereby escaping the
        need for synonym disambiguation in the client.
        :param requester:
        """
        self._requester = requester
        self._contexts = LowerDict()
        self._flows = set()
        self._quantities = set()

    @property
    def is_lcia_engine(self, org=None):
        valid_orgs = [k for k in self._requester.origins if 'quantity' in k.interfaces]
        if org is None:
            valid_orgs = list(filter(lambda x: x.origin.startswith(org), valid_orgs))
        return any(k.is_lcia_engine for k in valid_orgs)

    def _fetch_context_model(self, item):
        return self._requester.get_one(ContextModel, 'contexts', item)

    def _build_context(self, context_model):
        if context_model.name == 'None':
            c_actual = NullContext  # maintain singleton status of NullContext
        else:
            if context_model.parent is None or context_model.parent == '':
                parent = None
            else:
                parent = self.get_context(context_model.parent)
            c_actual = CoreContext(context_model.name, sense=context_model.sense, parent=parent)
            c_actual.add_origin(self._requester.origin)  # here we are masquerading all origins back to the requester origin
        self._contexts[c_actual.name] = c_actual
        return c_actual

    def add_flow(self, flow, **kwargs):
        self._flows.add(flow)

    def add_quantity(self, quantity):
        self._quantities.add(quantity)

    def __getitem__(self, item):
        try:
            return self._contexts[item]
        except KeyError:
            try:
                c_model = self._fetch_context_model(item)
            except HttpError as e:
                if e.args[0] == 404:
                    self._contexts[item] = None
                    return None
                else:
                    raise
            c_actual = self._build_context(c_model)
            self._contexts[item] = c_actual
            return c_actual

    def is_context(self, item):
        """
        The only place this is used is in collision checking- therefore this only needs to check if the name is
        known *locally* as a context (why do an http query for literally every new entity?)
        :param item:
        :return:
        """
        try:
            cx = self._contexts[item]
            if cx is not None:
                return True
            else:
                return False
        except KeyError:
            return False

    def get_context(self, item):
        return self.__getitem__(item) or NullContext

    def get_canonical(self, item):
        """
        again, to avoid premature optimization, the initial policy is no caching anything
        :param item:
        :return:
        """
        try:
            return self._requester.get_one(Entity, 'quantities', item)
        except HttpError as e:
            if e.args[0] == 404:
                raise EntityNotFound(item)
            else:
                raise

    def synonyms(self, term):
        return self._requester.get_many(str, 'synonyms', term=term)

    def contexts(self, **kwargs):
        c_models = self._requester.get_many(ContextModel, 'contexts', **kwargs)
        for c in c_models:
            if c.name in self._contexts:
                yield self._contexts[c.name]
            else:
                yield self._build_context(c)


class XdbClient(LcArchive):
    def __init__(self, source, ref=None):
        self._requester = XdbRequester(source, ref)
        if ref is None:
            ref = 'qdb'
        super(XdbClient, self).__init__(source, ref=ref, term_manager=XdbTermManager(self._requester))

    @property
    def r(self):
        return self._requester

    def make_interface(self, iface):
        if iface in READONLY_INTERFACE_TYPES:
            return XdbImplementation(self)
        raise InterfaceError(iface)

    def _fetch(self, entity, **kwargs):
        return self.query.make_ref(XdbEntity(self._requester.get_one(Entity, entity), self))
