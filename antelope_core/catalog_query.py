"""
Query Interface -- used to operate catalog refs
"""
import logging

from antelope import (BasicInterface, IndexInterface, BackgroundInterface, ExchangeInterface, QuantityInterface,
                      EntityNotFound, UnknownOrigin,
                      ExchangeRef, comp_dir, CatalogRef)

from antelope.interfaces.ibasic import BasicRequired

from antelope.refs import FlowRef, RxRef

from antelope.models import LciaResult as LciaResultModel, Characterization as CharacterizationModel

from .lcia_results import LciaResult, MixedComponents
from .characterizations import QRResult, Characterization
from .contexts import NullContext
from .implementations.quantity import CO2QuantityConversion

from .fragment_flows import FragmentFlow, frag_flow_lcia

from synonym_dict import InconsistentLineage

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


class CatalogQuery(BasicInterface, IndexInterface, BackgroundInterface, ExchangeInterface, QuantityInterface):
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

    def purge_cache_with(self, archive):
        for i, v in list(self._iface_cache.items()):
            if v.is_me(archive):
                self._iface_cache.pop(i)

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
        If not found locally, the current query is used instead
        :param origin:
        :return:
        """
        return self._grounded_query(origin)

    def _grounded_query(self, origin):
        if origin is None or origin == self._origin:
            return self
        return self._catalog.query(origin, cache=False)

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
        if self._catalog is None:
            raise NoCatalog
        returned = set()
        if itype in self._iface_cache:
            self._debug('Returning cached iface')
            i = self._iface_cache[itype]
            yield i
            returned.add(i._archive)
        for i in self._catalog.gen_interfaces(self._origin, itype, strict=strict):
            if i._archive in returned:
                continue
            if itype == 'background':  # all our background implementations must provide setup_bm(query)
                self._setup_background(i)

            self._debug('yielding %s' % i)
            if itype not in self._iface_cache:  # only cache the first iface
                if i.origin == self.origin:  # only cache iface if it is a strict match
                    self._iface_cache[itype] = i
            yield i

    def _perform_query(self, itype, attrname, exc, *args, strict=False, **kwargs):
        if itype is None:
            raise BadInterfaceSpec(itype, attrname)  # itype = 'basic'  # fetch, get properties, uuid, reference

        self._debug('Performing %s query, origin %s, iface %s' % (attrname, self.origin, itype))
        message = 'itype %s required for attribute %s' % (itype, attrname)
        props = []
        run = 0
        try:
            for iface in self._iface(itype, strict=strict):
                run += 1
                try:
                    self._debug('Attempting %s query on iface %s' % (attrname, iface))
                    result = getattr(iface, attrname)(*args, **kwargs)
                    message = '(%s) %s' % (itype, attrname)  # implementation found
                except exc:  
                    message = '(%s) %s except %s' % (itype, attrname, exc.__name__)
                    continue
                except NotImplementedError:  # allow nonimplementations to pass silently
                    message = '(%s) %s not implemented' % (itype, attrname)
                    continue
                if result is None:  # successful query must return something
                    message = '(%s) %s null' % (itype, attrname)
                else:
                    if attrname == 'properties':
                        for k in result:
                            if k not in props:
                                props.append(k)
                    else:
                        return result
            if attrname == 'get_context':
                if run:
                    return NullContext

        except AttributeError as e:
            message = '(%s) Attribute error %s' % (attrname, e.args)
        if len(props) > 0:
            return props

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
            entity = self._perform_query('basic', 'get', EntityNotFound, eid, **kwargs)
            return self.make_ref(entity)
        except EntityNotFound:
            return self._tm.get_canonical(eid)

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
            deref = [RxRef(self.make_ref(x.process), self.make_ref(x.flow), x.direction, x.comment, x.value)
                     for x in ref]
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
    '''
    def get_context(self, term, **kwargs):
        cx = super(CatalogQuery, self).get_context(term, **kwargs)
        return self._tm[cx]
    '''

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

    def synonyms(self, item, **kwargs):
        """
        Potentially controversial? include canonical as well as provincial synonyms for catalog queries??
        :param item:
        :param kwargs:
        :return:
        """
        rtn_set = set()
        for i in self._tm.synonyms(item):
            if i not in rtn_set:
                rtn_set.add(i)
                yield i
        for i in super(CatalogQuery, self).synonyms(item, **kwargs):
            if i not in rtn_set:
                rtn_set.add(i)
                yield i

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
        # print('@@@ going characterization-commando')
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
        if entity.is_entity:
            try:
                e_ref = entity.make_ref(self._grounded_query(entity.origin))
            except UnknownOrigin:
                e_ref = entity.make_ref(self)
        else:
            e_ref = entity  # already a ref
        if entity.entity_type == 'quantity':

            '''# question of whether to put this test before or after the get_canonical() attempts
            if before- we allow non-canonical "mass" to accumulate in the workspace
            if after- we catch 'mass' and 'freight' (and any LCIA methods we already know locally)
            and only return shit we don't recognize, but lose connection to remote sources
            
            prefer before: we may want to query core quantities like "net calorific value" for flow characterizations
            '''
            if entity.has_lcia_engine():  # we don't need a canonical version if it runs itself
                return e_ref

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
            
            2024-08-10 we are officially abandoning the following corollary: just send back canonical
            A corollary of this is that CatalogQuery.get() should get_canonical FIRST
            '''
            try:
                return self._tm.get_canonical(entity)  # returns the existing canonical unless there is a unit conflict
            except EntityNotFound:
                self._catalog.register_entity_ref(e_ref)
                return self._tm.get_canonical(entity)
        else:
            return e_ref

    '''
    de-reference Characterization factor models
    '''

    def _resolve_cf(self, cf: CharacterizationModel) -> Characterization:
        """

        :param cf:
        :return:
        """
        rq = self.get_canonical(cf.ref_quantity)
        qq = self.get_canonical(cf.query_quantity)
        try:
            cx = self._tm[tuple(cf.context)]
        except InconsistentLineage:
            cx = NullContext
        c = Characterization(cf.flowable, rq, qq, cx, origin=cf.origin)
        for k, v in cf.value.items():
            c[k] = v
        return c

    def factors(self, quantity, flowable=None, context=None, **kwargs):
        for cf in super(CatalogQuery, self).factors(quantity, flowable=flowable, context=context, **kwargs):
            if isinstance(cf, CharacterizationModel):
                yield self._resolve_cf(cf)
            else:
                yield cf

    '''
    de-reference LciaResult models
    '''

    def _result_from_model(self, process_ref, quantity, res_m: LciaResultModel):
        """
        Constructs a Detailed LCIA result from a background LCIA query, when we don't have a list of exchanges
        :param process_ref:
        :param quantity:
        :param res_m:
        :return:
        """
        try:
            quantity = self._tm.get_canonical(res_m.quantity.external_ref)
        except EntityNotFound:
            quantity = CatalogRef.from_json(res_m.quantity.serialize())  # dumbest thing ever- (the de/re/deserialize)

        res = LciaResult(quantity, scenario=res_m.scenario, scale=res_m.scale)
        process = self.get(process_ref)
        for c in res_m.components:
            try:
                entity = self.cascade(c.origin).get(c.entity_id)
            except (UnknownOrigin, EntityNotFound):
                entity = c.component
            res.add_component(c.component, entity)
            for d in c.details:
                try:
                    value = d.result / d.factor.value
                except ZeroDivisionError:
                    value = 0.0
                cx = self._tm[tuple(d.factor.context)]
                try:
                    rq = self.get_canonical(d.exchange.quantity_ref)
                except EntityNotFound:
                    rq = self.get(d.exchange.quantity_ref)
                try:
                    # OK - here we are making redundant orphan FlowRefs, one per detail, to avoid excess API calls.
                    flow_ref = FlowRef(d.exchange.external_ref, self.cascade(d.exchange.origin),
                                       name=d.exchange.name, reference_entity=rq, context=cx)
                except UnknownOrigin:
                    flow_ref = FlowRef(d.exchange.external_ref, self, masquerade=d.exchange.origin,
                                       name=d.exchange.name, reference_entity=rq, context=cx)
                ex_dir = d.exchange.direction
                '''
                if ex_dir is None:
                    print('Bad context: %s' % list(cx))
                    print('using "Output" direction')
                    ex_dir = 'Output'
                '''
                if cx.sense:
                    if ex_dir != comp_dir(cx.sense):
                        value *= -1  # DetailedLciaResult will negate the result internally
                ex = ExchangeRef(process, flow_ref,
                                 ex_dir,
                                 termination=cx, value=value)
                cf = QRResult(d.factor.flowable, rq, quantity, cx,
                              d.factor.locale, d.factor.origin, d.factor.value)
                if ex.flow.quell_co2:
                    co2_cf = CO2QuantityConversion(cf)
                    res.add_score(c.component, ex, co2_cf)
                else:
                    res.add_score(c.component, ex, cf)
        for s in res_m.summaries:
            try:
                entity = self.cascade(s.origin).get(s.entity_id)
            except (UnknownOrigin, EntityNotFound):
                entity = s.component
            res.add_summary(s.component, entity, s.node_weight, s.unit_score)
        if len(res) == 0:
            res.add_summary('Total', 'total', 1.0, res_m.total)
        else:
            if res.total() != res_m.total:
                net = res_m.total - res.total()
                try:
                    res.add_summary('net result', 'net result', 1.0, net)
                except MixedComponents:
                    logging.warning('Inconsistent Agg LCIA result (net %g of %g)' % (net, res_m.total))
        return res

    def _cycle_through_ress(self, ress, process, query_qty):
        if isinstance(ress, list):
            conv = []
            for res in ress:
                if isinstance(res, LciaResultModel):
                    conv.append(self._result_from_model(process_ref=process, quantity=query_qty, res_m=res))
                else:
                    conv.append(res)
            return conv
        else:
            if isinstance(ress, LciaResultModel):
                return self._result_from_model(process_ref=process, quantity=query_qty, res_m=ress)
            else:
                return ress

    def contrib_lcia(self, process, quantity=None, ref_flow=None, **kwargs):
        p = self.get(process)
        r = p.reference(ref_flow)
        ffs = FragmentFlow.from_process_inventory(self, p, r.flow, **kwargs)
        res = frag_flow_lcia(ffs, quantity)
        return res.contrib()

    def sys_lcia(self, process, query_qty, observed=None, ref_flow=None, **kwargs):
        """
        Reimplement this to detect pydantic LciaResult models and de-reference them
        :param process:
        :param query_qty:
        :param observed:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        ress = super(CatalogQuery, self).sys_lcia(process, query_qty, observed=observed, ref_flow=ref_flow, **kwargs)
        return self._cycle_through_ress(ress, process, query_qty)

    def bg_lcia(self, process, query_qty=None, ref_flow=None, **kwargs):
        """
        returns an LciaResult object, aggregated as appropriate depending on the interface's privacy level.
        This can only be implemented at the query level because it requires access to lci()
        :param process: must have a background interface
        :param query_qty: an operable quantity_ref, or catalog default may be used if omitted
        :param ref_flow:
        :param kwargs:
        :return:
        """
        try:
            ress = super(CatalogQuery, self).bg_lcia(process, query_qty=query_qty, ref_flow=ref_flow, **kwargs)
        except BasicRequired:
            p_ref = self.get(process)
            if p_ref.is_entity:
                raise NotImplementedError  # we can't proceed
            lci = p_ref.lci(ref_flow=ref_flow)
            # aggregation
            ress = query_qty.do_lcia(lci, **kwargs)
        return self._cycle_through_ress(ress, process, query_qty)
