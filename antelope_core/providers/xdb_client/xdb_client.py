"""
Client for the xdb Antelope server

The concept here is to pass received requests straight into the Pydantic models, and then use those for easy
(though manual) deserialization into EntityRefs.
"""
from antelope.models import Context as ContextModel, Entity
from antelope_core.archives import LcArchive, InterfaceError
from antelope_core.catalog_query import READONLY_INTERFACE_TYPES
from antelope_core.contexts import ContextManager, NullContext

from .requester import XdbRequester
from .implementation import XdbImplementation
from .xdb_entities import XdbEntity

from requests.exceptions import HTTPError


class XdbTermManager(object):
    def __init__(self, requester: XdbRequester):
        """
        The key question here: should I at least cache remote contexts? for now let us cache nothing

        Except we DO need to cache contexts, or at least create them on the fly (and if we are going to create
        them, we should cache them) because exterior exchanges need to terminate properly in contexts.

        SO: we will establish the condition that Xdb MUST only use contexts' canonical names, thereby escaping the
        need for synonym disambiguation in the client.
        :param requester:
        """
        self._requester = requester
        self._cm = ContextManager()
        self._bad_contexts = set()
        self._flows = set()
        self._quantities = set()

    def update_requester(self, requester):
        self._requester = requester

    @property
    def is_lcia_engine(self):
        return self._requester.is_lcia_engine

    def _fetch_context_model(self, item):
        return self._requester.get_one(ContextModel, 'contexts', item)

    def _build_context(self, context_model, *args):
        if context_model.name == 'None':
            c_actual = NullContext  # maintain singleton status of NullContext
        else:
            if context_model.parent is None or context_model.parent == '':
                parent = None
            else:
                parent = self.get_context(context_model.parent)
            c_actual = self._cm.new_entry(context_model.name, *args, parent=parent)
            if parent is not None and parent.sense is None and context_model.sense is not None:
                c_actual.sense = context_model.sense
            c_actual.add_origin(self._requester.origin)  # here we are masquerading all origins back to the requester origin
        return c_actual

    def add_flow(self, flow, **kwargs):
        self._flows.add(flow)

    def add_quantity(self, quantity):
        self._quantities.add(quantity)

    def __getitem__(self, item):
        if isinstance(item, list):
            item = tuple(item)
        if item in self._bad_contexts:
            return None
        try:
            return self._cm[item]
        except KeyError:
            try:
                c_model = self._fetch_context_model(item)
            except HTTPError as e:
                if e.args[0] == 404:
                    self._bad_contexts.add(item)
                    return None
                else:
                    raise
            c_actual = self._build_context(c_model, item)
            # this escapes the PROTECTED_TERM restrictions- which is OK because our CM is captive
            self._cm.add_synonym(c_actual.name, item)
            return c_actual

    def is_context(self, item):
        """
        The only place this is used is in collision checking- therefore this only needs to check if the name is
        known *locally* as a context (why do an http query for literally every new entity?)
        :param item:
        :return:
        """
        try:
            cx = self._cm[item]
        except KeyError:
            return False
        return cx is not None

    def get_context(self, item):
        return self.__getitem__(item) or NullContext

    '''
    def get_canonical(self, item):
        """
        again, to avoid premature optimization, the initial policy is no caching anything
        :param item:
        :return:
        """
        try:
            return self._requester.get_one(Entity, 'quantities', item)
        except HTTPError as e:
            if e.args[0] == 404:
                raise EntityNotFound(item)
            else:
                raise e
    '''

    def synonyms(self, term):
        return self._requester.get_many(str, 'synonyms', term=term)

    def contexts(self, **kwargs):
        c_models = self._requester.get_many(ContextModel, 'contexts', **kwargs)
        for c in c_models:
            if c.name in self._cm:
                yield self._cm[c.name]
            else:
                yield self._build_context(c)


class XdbClient(LcArchive):
    """
    An XdbClient accesses xdb at a named URL using an access token.
    """
    def __init__(self, source, ref=None, token=None):
        self._requester = XdbRequester(source, ref, token=token)
        if ref is None:
            ref = 'qdb'
        super(XdbClient, self).__init__(source, ref=ref, term_manager=XdbTermManager(self._requester))

    def refresh_token(self, new_token):
        self._requester.set_token(new_token)

    def refresh_auth(self, new_source, new_token):
        self._requester = XdbRequester(new_source, new_token)
        self.tm.update_requester(self._requester)

    @property
    def r(self):
        return self._requester

    def make_interface(self, iface):
        if iface in READONLY_INTERFACE_TYPES:
            return XdbImplementation(self)
        raise InterfaceError(iface)

    def _fetch(self, key, **kwargs):
        ref = self.query.make_ref(XdbEntity(self._requester.get_one(Entity, key), self))
        if key not in self._entities:
            self._entities[key] = ref
        return ref
