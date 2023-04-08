"""
Query Interface -- used to operate catalog refs
"""

from antelope import (IndexInterface, BackgroundInterface, ExchangeInterface, QuantityInterface, EntityNotFound, UnknownOrigin)
#                      ForegroundInterface,
#                      IndexRequired, PropertyExists,
#                      )
from antelope.refs.exchange_ref import RxRef

INTERFACE_TYPES = ('basic', 'index', 'exchange', 'background', 'quantity', 'foreground')
READONLY_INTERFACE_TYPES = {'basic', 'index', 'exchange', 'background', 'quantity'}


def zap_inventory(interface, warn=False):
    if interface == 'inventory':
        if warn:
            print('# # # # # # # # # **** Warning: use exchange over inventory ***** # # # # # # # # #')
            raise AttributeError
        return 'exchange'
    return interface


class NoCatalog(Exception):
    pass


class BackgroundSetup(Exception):
    pass


class BadInterfaceSpec(Exception):
    pass


class CatalogQuery(IndexInterface, BackgroundInterface, ExchangeInterface, QuantityInterface):  # , ForegroundInterface):
    """
    A CatalogQuery is a class that performs any supported query against a supplied catalog.
    Supported queries are defined in the lcatools.interfaces, which are all abstract.
    Implementations also subclass the abstract classes.

    This reduces code duplication (all the catalog needs to do is provide interfaces) and ensures consistent signatures.

    The arguments to a query should always be text strings, not entities.  When in doubt, use the external_ref.

    The EXCEPTION is the bg_lcia routine, which works best (auto-loads characterization factors) if the query quantity
    is a catalog ref.

    The catalog's resolver performs fuzzy matching, meaning that a generic query (such as 'local.ecoinvent') will return
    both exact resources and resources with greater semantic specificity (such as 'local.ecoinvent.3.2.apos').
    All queries accept the "strict=" keyword: set to True to only accept exact matches.
    """
    _recursing = False
    _dbg = False

    def on_debug(self):
        self._dbg = True

    def off_debug(self):
        self._dbg = False

    def _debug(self, *args):
        if self._dbg:
            print(self.__class__.__name__, *args)

    def __init__(self, origin, catalog=None, debug=False):
        self._origin = origin
        self._catalog = catalog
        self._dbg = debug

        self._iface_cache = dict()

    def __str__(self):
        if self._catalog:
            root = 'catalog_root=%s' % self._catalog.root
        else:
            root = 'no catalog'
        if self._dbg:
            root += ', DEBUG ON'
        return '%s(%s, %s)' % (self.__class__.__name__, self._origin, root)

    def __repr__(self):
        return self.__str__()

    @property
    def origin(self):
        return self._origin

    @property
    def _tm(self):
        return self._catalog.lcia_engine

    '''
    def is_elementary(self, context):
        """
        Stopgap used to expose access to a catalog's Qdb; in the future, flows will no longer exist and is_elementary
        will be a trivial function of an exchange asking whether its termination is a context or not.
        :param context:
        :return: bool
        """
        return self._tm[context.fullname].elementary
    '''

    def cascade(self, origin):
        """
        Generate a new query for the specified origin.
        Enables the query to follow the origins of foreign objects found locally.
        :param origin:
        :return:
        """
        return self._grounded_query(origin)

    def _grounded_query(self, origin):
        if origin is None or origin == self._origin:
            return self
        return self._catalog.query(origin)

    '''
    def __str__(self):
        return '%s for %s (catalog: %s)' % (self.__class__.__name__, self.origin, self._catalog.root)
    '''
    def _setup_background(self, bi):
        self._debug('Setting up background interface')
        try:
            bi.setup_bm(self)
        except AttributeError:
            raise BackgroundSetup('Failed to configure background')

    def _iface(self, itype, strict=False):
        self._debug('Origin: %s' % self.origin)
        if self._catalog is None:
            raise NoCatalog
        if itype in self._iface_cache:
            self._debug('Returning cached iface')
            yield self._iface_cache[itype]
        for i in self._catalog.gen_interfaces(self._origin, itype, strict=strict):
            if itype == 'background':  # all our background implementations must provide setup_bm(query)
                self._setup_background(i)

            self._debug('yielding %s' % i)
            self._iface_cache[itype] = i  # only cache the most recent iface
            yield i

    def _perform_query(self, itype, attrname, exc, *args, strict=False, **kwargs):
        if itype is None:
            raise BadInterfaceSpec(itype, attrname)  # itype = 'basic'  # fetch, get properties, uuid, reference

        self._debug('Performing %s query, iface %s' % (attrname, itype))
        message = 'itype %s required for attribute %s' % (itype, attrname)
        try:
            for iface in self._iface(itype, strict=strict):
                try:
                    self._debug('Attempting %s query on iface %s' % (attrname, iface))
                    result = getattr(iface, attrname)(*args, **kwargs)
                    message = '(%s) %s' % (itype, attrname)  # implementation found
                except exc:  # allow nonimplementations to pass silently
                    continue
                if result is not None:  # successful query must return something
                    return result
        except NotImplementedError:
            pass

        raise exc('%s: %s | %s' % (self.origin, message, args))

    def resolve(self, itype=INTERFACE_TYPES, strict=False):
        """
        Secure access to all known resources but do not answer any query
        :param itype: default: all interfaces
        :param strict: [False]
        :return:
        """
        for k in self._iface(itype, strict=strict):
            yield k

    def get(self, eid, **kwargs):
        """
        Retrieve entity by external Id. This will take any interface and should keep trying until it finds a match.
        It first matches canonical entities, because that is the point of canonical entities.
        :param eid: an external Id
        :return:
        """
        try:
            return self._tm.get_canonical(eid)
        except EntityNotFound:
            entity = self._perform_query('basic', 'get', EntityNotFound, eid, **kwargs)
            return self.make_ref(entity)

    def get_reference(self, external_ref):
        ref = self._perform_query('basic', 'get_reference', EntityNotFound, external_ref)
        # quantity: unit
        # flow: quantity
        # process: list
        # fragment: fragment
        # [context: context]
        if ref is None:
            deref = None
        elif isinstance(ref, list):
            deref = [RxRef(self.make_ref(x.process), self.make_ref(x.flow), x.direction, x.comment) for x in ref]
        elif isinstance(ref, str):
            deref = ref
        elif ref.entity_type == 'unit':
            deref = ref.unitstring
        else:
            deref = self.make_ref(ref)
        return deref

    '''
    LCIA Support
    get_canonical(quantity)
    catch get_canonical calls to return the query from the local Qdb; fetch if absent and load its characterizations
    (using super ==> _perform_query)
    '''
    def get_context(self, term, **kwargs):
        cx = super(CatalogQuery, self).get_context(term, **kwargs)
        return self._tm[cx]

    def get_canonical(self, quantity, **kwargs):
        try:
            # print('Gone canonical')
            q_can = self._tm.get_canonical(quantity)
        except EntityNotFound:
            if hasattr(quantity, 'entity_type') and quantity.entity_type == 'quantity':
                print('Missing canonical quantity-- adding to LciaDb')
                self._catalog.register_entity_ref(quantity)
                return self._tm.get_canonical(quantity)
                # print('Retrieving canonical %s' % q_can)
            else:
                raise
        return q_can

    def characterize(self, flowable, ref_quantity, query_quantity, value, context=None, location='GLO', **kwargs):
        """
        This is an Xdb innovation: we do not need or want an implementation-specific characterize routine-- just like
        with make_ref, the point of the catalog query is to localize all characterizations to the LciaEngine.

        We simply duplicate the characterize() code from the core QuantityImplementation
        :param flowable:
        :param ref_quantity:
        :param query_quantity:
        :param value:
        :param context:
        :param location:
        :param kwargs:
        :return:
        """
        rq = self.get_canonical(ref_quantity)
        qq = self.get_canonical(query_quantity)
        origin = kwargs.pop('origin', self.origin)
        print('@@@ going characterization-commando')
        return self._tm.add_characterization(flowable, rq, qq, value, context=context, location=location,
                                             origin=origin, **kwargs)

    def clear_seen_characterizations(self, quantity):
        """
        An ugly hack to deal with the absolutely terrible way we are working around our slow-ass Qdb implementation
        the proper solution is for qdb lookup to be local,  fast and correct, so as to not require caching at all.
        :param quantity:
        :return:
        """
        for i in self._iface_cache.values():
            if i._archive:
                for f in i._archive.entities_by_type('flow'):
                    k = [cf for cf in f._chars_seen.keys() if cf[0] is quantity]
                    for cf in k:
                        f.pop_char(*cf)
                    if f._query_ref:
                        k = [cf for cf in f._query_ref._chars_seen.keys() if cf[0] is quantity]
                        for cf in k:
                            f._query_ref.pop_char(*cf)

    def make_ref(self, entity):
        if isinstance(entity, list):
            return [self.make_ref(k) for k in entity]
        if entity is None:
            return None
        if entity.entity_type == 'fragment':
            # TODO: create a new ForegroundQuery to eliminate the need for this hack
            return entity  # don't make references for fragments just now
        if entity.is_entity:
            try:
                e_ref = entity.make_ref(self._grounded_query(entity.origin))
            except UnknownOrigin:
                e_ref = entity.make_ref(self)
        else:
            e_ref = entity  # already a ref
        if entity.entity_type == 'quantity':
            ''' # astonishingly, we don't want this - register but not return
            # print('Going canonical')
            # astonishing because it's not true. 
            Well. not exactly true.
            
            CatalogQueries should return canonical quantities. that is the point of the catalog.  The reason we didn't
            want this was because we were using the catalog to access origin-specific data to re-serve it.  On the
            server side, we thought we would want to keep track of all this- for veracity of the data, for provenance,
            etc.  But in point of fact, there is NO CIRCUMSTANCE under which a user benefits from having 
            origin-specific  versions of "mass" or "area".
            
            True, the data won't match the source.  but we will still RECOGNIZE the source because we will register the 
            quantity terms with the term manager.  Which we WEREN"T doing before.
            
            A corollary of this is that CatalogQuery.get() should get_canonical FIRST
            '''
            try:
                return self._tm.get_canonical(entity.link)
            except EntityNotFound:
                try:
                    _ = self._tm.get_canonical(entity)
                except EntityNotFound:
                    self._catalog.register_entity_ref(e_ref)
                    return self._tm.get_canonical(entity)
                # print('@@@ Canonical quantity missing link-- adding direct to qm')  # I predict this never occurs
                # in fact, it occurred several times immediately
                return self._tm.add_quantity(entity)  # this will be identical to _ unless there is a unit conflict
        else:
            return e_ref
