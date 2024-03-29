"""
Each archive now has a TermManager which interprets query arguments as synonyms for canonical flows and contexts.  This
can also be upgraded to an LciaEngine, which extends the synonymization strategy to quantities as well
"""
from antelope import (QuantityInterface, NoFactorsFound, ConversionReferenceMismatch, EntityNotFound, FlowInterface,
                      convert, ConversionError, QuantityRequired, RefQuantityRequired)

from antelope.flows.flow import QuelledCO2

from .basic import BasicImplementation
from ..characterizations import QRResult, LocaleMismatch
from ..contexts import NullContext
from ..lcia_results import LciaResult
from ..entities.quantities import new_quantity
from ..entities.flows import new_flow


class UnknownRefQuantity(Exception):
    pass


class QuantityConversion(object):
    """
    A stack of Quantity Relation results that are composed sequentially in order to render a flow-quantity conversion.
    The first QRR added should report the query quantity (numerator) in terms of some reference quantity (denominator);
    then each subsequent QRR should include the prior ref quantity as the query quantity.

    QuantityConversion implements the interface of a QRResult (flowable, ref, query, context, value, locale, origin),
    'context' is a cache of the [canonical] context used as query input; 'locale' is a '/' join of all
    found geographies; 'flowable' 'query' and 'origin' take from the first QR Result; 'ref' takes the last; value is
    computed as the product of all contained QRResults.

    For instance, a Quantity conversion from moles of CH4 to GWP 100 might include first the GWP conversion and then
    the mol conversion:
    QuantityConversion(QRResult('methane', 'kg', 'kg CO2eq', 'emissions to air', 'GLO', 'ipcc.2007', 25.0),
                       QRResult('methane', 'mol', 'kg', None, 'GLO', 'local.qdb', 0.016))
    giving the resulting value of 0.4.

    The QuantityConversion needs information to be fully defined: the query quantity and the query context, both of
    which should be canonical.  The canonical context is especially needed to test directionality for LCIA.

    For the context initially submitted, consult the exchange.
    """
    @classmethod
    def null(cls, flowable, rq, qq, context, locale, origin):
        qrr = QRResult(flowable, rq, qq, context or NullContext, locale, origin, 0.0)
        return cls(qrr)

    @classmethod
    def copy(cls, conv):
        return cls(*conv.results, query=conv.query, context=conv.context)

    def __init__(self, *args, query=None, context=NullContext):
        self._query = query  # this is just a stub to give ref conversion machinery something to grab hold of
        self._context = context
        self._results = []
        for arg in args:
            self.add_result(arg)

    def __hash__(self):
        return hash((self.flowable, self.ref, self.query, self.context, self.locale))

    def __eq__(self, other):
        try:
            return (self.flowable == other.flowable and self.query == other.query and self.context == other.context
                    and self.locale == other.locale and self.value == other.value)
        except AttributeError:
            return False

    def __bool__(self):
        if self.value != 0.0:
            return True
        return False

    def invert(self):
        inv_qrr = type(self)(query=self.ref)
        for res in self._results[::-1]:
            inv_qrr.add_inverted_result(res)
        return inv_qrr

    def flatten(self, origin=None):
        if len(self._results) == 0:
            return self
        if origin is None:
            origin = self._results[0].origin
        return QRResult(self.flowable, self.ref, self.query, self.context, self.locale, origin, self.value)

    def add_result(self, qrr):
        if isinstance(qrr, QRResult):
            if qrr.query is None or qrr.ref is None:
                raise ValueError('Both ref and query quantity must be defined')
            if len(self._results) > 0:
                if self.flowable != qrr.flowable:  # don't think we care
                    # raise FlowableMismatch('%s != %s' % (self.flowable, qrr.flowable))
                    print('Flowable changed: %s -> %s' % (self.flowable, qrr.flowable))
                if self.ref != qrr.query:
                    raise ConversionReferenceMismatch('%s != %s' % (self.ref, qrr.query))
            self._results.append(qrr)
        else:
            raise TypeError('Must supply a QRResult')

    @property
    def qualitative(self):
        return isinstance(self.value, str)

    @property
    def query(self):
        if self._query is None:
            if len(self._results) > 0:
                return self._results[0].query
        return self._query

    @property
    def ref(self):
        if len(self._results) == 0:
            return self._query
        return self._results[-1].ref

    @property
    def flowable(self):
        if len(self._results) == 0:
            return None
        return self._results[0].flowable

    @property
    def context(self):
        for qrr in reversed(self._results):
            if qrr.context is not None:
                return qrr.context
        if self._context is not None:
            return self._context
        return NullContext

    @property
    def locale(self):
        locs = []
        for res in self._results:
            if res.locale not in locs:
                locs.append(res.locale)
        return '/'.join(locs)

    @property
    def origin(self):
        if len(self._results) > 0:
            return self._results[0].origin
        return ''

    @property
    def results(self):
        for res in self._results:
            yield res

    def seen(self, q):
        for res in self._results:
            if q == res.ref:
                return True
        return False

    @staticmethod
    def _invert_qrr(qrr):
        """
        swaps the ref and query quantities and inverts the value
        :param qrr:
        :return:
        """
        return QRResult(qrr.flowable, qrr.query, qrr.ref, qrr.context, qrr.locale, qrr.origin, 1.0 / qrr.value)

    def add_inverted_result(self, qrri):
        self.add_result(self._invert_qrr(qrri))

    @property
    def value(self):
        val = 1.0
        for res in self._results:
            if isinstance(res.value, str):
                return res.value  # qualitative value
            val *= res.value
        return val

    def __getitem__(self, item):
        return self._results[item]

    def __str__(self):
        if self.qualitative:
            return '%s [%s] %s: %s [%s] (%s)' % (self.flowable, self.context, self.query, self.value,
                                                 self._results[-1].locale, self._results[-1].origin)
        if len(self._results) == 0:
            return str(self.query)
        conv = ' x '.join(['%g %s/%s' % (res.value, res.query.unit, res.ref.unit) for res in self._results])
        return '%s; %s: %s [%s] (%s)' % (self.flowable, self.context,
                                         conv, self._results[-1].locale,
                                         self._results[-1].origin)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__str__())

    # TODO: add serialization, other outputs


class QuantityConversionError(object):
    def __init__(self, qrresult, ref_quantity):
        self._qrr = qrresult
        self._ref = ref_quantity

    @property
    def ref(self):
        return self._ref

    @property
    def query(self):
        return self._qrr.query

    @property
    def fail(self):
        return self._qrr.ref

    @property
    def flowable(self):
        return self._qrr.flowable

    @property
    def context(self):
        return self._qrr.context

    @property
    def locale(self):
        return self._qrr.locale

    @property
    def value(self):
        return None

    def __repr__(self):
        return '%s(%s; %s %s =X=> %s)' % (self.__class__.__name__, self.flowable, self.context, self._qrr.ref, self._ref)


def do_lcia(quantity, inventory, locale=None, group=None, dist=2, **kwargs):
    """
    Successively implement the quantity relation over an iterable of exchanges.

    man, WHAT is the qdb DOING with all those LOC? (ans: seemingly a lot)

    :param quantity:
    :param inventory: An iterable of exchange-like entries, having flow, direction, value, termination.  Currently
      also uses process.external_ref for hashing purposes, but that could conceivably be abandoned.
    :param locale: ['GLO']
    :param group: How to group scores.  Should be a lambda that operates on inventory items. Default x -> x.process
    :param dist: [2] controls how strictly to interpret exchange context.
      0 - exact context matches only;
      1 - match child contexts (code default)
      2 - match parent contexts [this default]
      3 - match any ancestor context, including NullContext
    :param kwargs:
    :return:
    """
    res = LciaResult(quantity)
    if group is None:
        group = lambda _x: _x.process
    for x in inventory:
        xt = x.type
        if xt == 'reference':  # in ('cutoff', 'reference'):
            res.add_cutoff(x)
            continue
        elif xt == 'self':
            continue
        qrr = x.flow.lookup_cf(quantity, x.termination, locale, dist=dist, **kwargs)
        if isinstance(qrr, QuantityConversion):
            if qrr.value == 0:
                res.add_zero(x)
            else:
                res.add_score(group(x), x, qrr)
        elif isinstance(qrr, QuantityConversionError):
            res.add_error(x, qrr)
        elif isinstance(qrr, QuelledCO2):
            res.add_zero(x)
        elif qrr is None:
            res.add_cutoff(x)
        else:
            raise TypeError('Unknown qrr type %s' % qrr)
    e = len(list(res.errors()))
    if e:
        print('%s: %d CF errors encountered' % (quantity, e))
    return res


class NoConversion(Exception):
    pass


def try_convert(flowable, rq, qq, context, locale):

    if hasattr(qq, 'is_lcia_method') and qq.is_lcia_method:
        raise NoConversion
    else:
        try:
            fac = convert(qq, from_unit=rq.unit)
            return QRResult(flowable, rq, qq, context or NullContext, locale, qq.origin, fac)
        except (KeyError, ConversionError):
            pass

    if hasattr(rq, 'is_lcia_method') and rq.is_lcia_method:
        raise NoConversion
    else:
        try:
            fac = convert(rq, to=qq.unit)
            return QRResult(flowable, rq, qq, context or NullContext, locale, rq.origin, fac)
        except (KeyError, ConversionError):
            pass
    raise NoConversion


class QuantityImplementation(BasicImplementation, QuantityInterface):

    def new_quantity(self, name, ref_unit=None, **kwargs):
        """

        :param name:
        :param ref_unit:
        :param kwargs:
        :return:
        """
        q = new_quantity(name, ref_unit, **kwargs)
        self._archive.add(q)
        return q

    def new_flow(self, name, ref_quantity=None, **kwargs):
        """

        :param name:
        :param ref_quantity: defaults to "Number of items"
        :param context: [None] pending context refactor
        :param kwargs:
        :return:
        """
        if ref_quantity is None:
            ref_quantity = 'Number of items'
        try:
            ref_q = self.get_canonical(ref_quantity)
        except EntityNotFound:
            raise UnknownRefQuantity(ref_quantity)
        f = new_flow(name, ref_q, **kwargs)
        self._archive.add_entity_and_children(f)
        return self.get(f.link)

    """
    Uses the archive's term manager to index cfs, by way of the canonical quantities
    """
    def quantities(self, **kwargs):
        for q_e in self._archive.search('quantity', **kwargs):
            yield q_e

    def get_canonical(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity from a qdb; else raises EntityNotFound
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        try:
            return self._archive.tm.get_canonical(quantity)
        except EntityNotFound:
            if isinstance(quantity, str):
                q = self._archive.retrieve_or_fetch_entity(quantity)
                if q is None:
                    raise
                return self._archive.tm.get_canonical(q.external_ref)
            elif hasattr(quantity, 'entity_type') and quantity.entity_type == 'quantity':
                # this is reimplementing CatalogQuery.get_canonical() -> catalog.register_entity_ref()
                if quantity.is_entity:
                    raise TypeError('Supplied argument is an entity')
                self._archive.add(quantity)
                return self._archive.tm.get_canonical(quantity)
            else:
                raise

    def factors(self, quantity, flowable=None, context=None, dist=0):
        q = self.get_canonical(quantity)  # get_canonical AUDIT
        for cf in self._archive.tm.factors_for_quantity(q, flowable=flowable, context=context, dist=dist):
            yield cf

    def characterize(self, flowable, ref_quantity, query_quantity, value, context=None, location='GLO', origin=None,
                     **kwargs):
        """
        We gotta be able to do this
        :param flowable: string
        :param ref_quantity: string
        :param query_quantity: string
        :param value: float
        :param context: string
        :param location: string
        :param origin: [self.origin]
        :param kwargs: overwrite=False
        :return:
        """
        rq = self.get_canonical(ref_quantity)
        qq = self.get_canonical(query_quantity)
        if qq.origin != self._archive.ref:
            self._archive.add_entity_and_children(qq)  # catches EntityExists
        if origin is None:
            origin = self.origin
        return self._archive.tm.add_characterization(flowable, rq, qq, value, context=context, location=location,
                                                     origin=origin, **kwargs)

    def _ref_qty_conversion(self, target_quantity, flowable, compartment, conv, locale, _reverse=True):
        """
        Transforms a CF into a quantity conversion with the proper ref quantity. Does it recursively! watch with terror.
        :param target_quantity: conversion target
        :param flowable:
        :param compartment:
        :param conv: An existing QuantityConversion chain, whose ref we must turn into target_quantity
        :param locale:
        :return: the incoming conv, augmented
        """
        if target_quantity is None:
            raise ConversionReferenceMismatch('Cannot convert to None')
        found_quantity = self.get_canonical(conv.ref)  # this is still necessary because CFs are stored with native ref quantities
        if found_quantity != target_quantity:
            # zero look for conversions
            try:
                qr_result = try_convert(flowable, target_quantity, found_quantity, compartment, locale)
                conv.add_result(qr_result)
                return conv
            except NoConversion:
                pass

            # first look for forward matches
            cfs_fwd = [cf for cf in self._archive.tm.factors_for_flowable(flowable, quantity=found_quantity,
                                                                          context=compartment, dist=3)
                       if not conv.seen(cf.ref_quantity)]
            for cf in cfs_fwd:
                new_conv = QuantityConversion.copy(conv)
                new_conv.add_result(cf.query(locale))
                try:
                    return self._ref_qty_conversion(target_quantity, flowable, compartment, new_conv, locale,
                                                    _reverse=_reverse)
                except ConversionReferenceMismatch:
                    continue

            # then look for reverse matches... but... only once
            if _reverse:
                new_conv = QuantityConversion.copy(conv)
                try:
                    rev_conv = self._ref_qty_conversion(found_quantity, flowable, compartment,
                                                        QuantityConversion(query=target_quantity), locale,
                                                        _reverse=False)

                    for res in rev_conv.invert().results:
                        new_conv.add_result(res)
                    if new_conv.ref == target_quantity:
                        return new_conv
                except QuantityRequired:  # if we don't have a reverse qty interface, we can't look for reverse cfs
                    pass

            raise ConversionReferenceMismatch('Flow %s\nfrom %s\nto %s' % (flowable,
                                                                           conv.ref,
                                                                           target_quantity))
        return conv

    def _quantity_engine(self, fb, rq, qq, cx, locale='GLO',
                         **kwargs):
        """
        This is the main "comprehensive" engine for performing characterizations.

        :param fb: a string that is synonymous with a known flowable.
        :param rq: a canonical ref_quantity or None
        :param qq: a canonical query_quantity or None
        :param cx: a known context or None
        :param locale: ['GLO']
        :param kwargs:
         dist: CLookup distance (0=exact 1=subcompartments 2=parent 3=all parents)
        :return: 3-tuple of lists of QRResults objects: qr_results, qr_mismatch, qr_geog
         qr_results: valid conversions from query quantity to ref quantity
         qr_geog: valid conversions which had a broader spatial scope than specified, if at least one narrow result
         qr_mismatch: conversions from query quantity to a different quantity that could not be further converted

        """
        # TODO: port qdb functionality: detect unity conversions; quell biogenic co2; integrate convert() -- DONE??

        qr_results = []
        qr_mismatch = []
        qr_geog = []

        # if we are not doing LCIA, jump straight to unit conversion
        if qq is not None:
            if not qq.is_lcia_method:
                res = QuantityConversion(query=qq, context=cx)
                try:
                    qr_results.append(self._ref_qty_conversion(rq, fb, cx, res, locale))
                except ConversionReferenceMismatch:
                    res.add_result(QRResult(fb, qq, qq, cx, locale, qq.origin, 1.0))
                    qr_mismatch.append(QuantityConversionError(res, rq))
                except LocaleMismatch as e:
                    locales = e.args[0]
                    for loc in locales:
                        res = QuantityConversion(query=qq, context=cx)
                        qr_geog.append(self._ref_qty_conversion(rq, fb, cx, res, loc))

                return qr_results, qr_geog, qr_mismatch

        for cf in self._archive.tm.factors_for_flowable(fb, quantity=qq, context=cx, **kwargs):
            res = QuantityConversion(cf.query(locale), query=qq, context=cx)
            try:
                qr_results.append(self._ref_qty_conversion(rq, fb, cx, res, locale))
            except ConversionReferenceMismatch:
                qr_mismatch.append(QuantityConversionError(res, rq))
            except LocaleMismatch as e:
                locales = e.args[0]
                for loc in locales:
                    qr_geog.append(self._ref_qty_conversion(rq, fb, cx, res, loc))

        ''' # leaving this OUT- we should only do forward and reverse matching for ref quantity conversion, not qq
        Leaving it back in because it breaks a unit test
        and backing it out again because of minor _ref_qty_conversion refactor
        for cf in self._archive.tm.factors_for_flowable(fb, quantity=rq, context=cx, **kwargs):
            res = QuantityConversion(cf.query(locale))
            try:
                qr_results.append(self._ref_qty_conversion(qq, fb, cx, res, locale).invert())
            except ConversionReferenceMismatch:
                pass  # qr_mismatch.append(res.invert())  We shouldn't be surprised that there is no reverse conversion
        ##'''

        if len(qr_results + qr_geog + qr_mismatch) == 0:
            raise NoFactorsFound

        if len(qr_results) > 1:
            _geog = [k for k in filter(lambda x: x[0].locale != locale, qr_results)]
            qr_results = [k for k in filter(lambda x: x[0].locale == locale, qr_results)]
            qr_geog += _geog

        return qr_results, qr_geog, qr_mismatch

    def _get_flowable_info(self, flow, ref_quantity, context):
        """
        We need all three defined at the end. So if all given, we take em and look em up.
        Basically we take what we get for flow, unless we're missing ref_quantity or context.
        If flow is not entity_type='flow', we try to fetch a flow and if that fails we return what we were given.
        If we're given a flow and/or an external ref that looks up, then flowable, ref qty, and context are taken from
        it (unless they were provided)
        If a context query is specified and no canonical context is found, NullContext is returned (matches only
        NullContext characterizations).
        Otherwise, None is returned (matches everything).
        :param flow:
        :param ref_quantity:
        :param context:
        :return: flowable, canonical ref_quantity, canonical context or None
        """
        if isinstance(flow, str):
            try:
                flow = self.get(flow)  # assume it's a ref
            except EntityNotFound:
                pass

            # if f is not None:  # lookup succeeded
            #     flow = f

        if isinstance(flow, FlowInterface):
            flowable = flow.link
            if ref_quantity is None:
                ref_quantity = flow.reference_entity
                if ref_quantity is None:
                    print('a FlowInterface? %s\n%s' % (flow, flow.__class__.__name__))
                    print('No ref quantity? %s' % flow.link)

            if context is None:
                context = flow.context or None
        else:
            if hasattr(flow, 'link'):
                flowable = flow.link
            else:
                flowable = str(flow)

        if ref_quantity is None:
            raise RefQuantityRequired

        rq = self.get_canonical(ref_quantity)
        cx = self._archive.tm[context]  # will fall back to find_matching_context if tm is an LciaEngine
        if cx is None and context is not None:  # lcia_engine now returns NullContext, but TermManager does not
            cx = NullContext
        return flowable, rq, cx

    def quantity_conversions(self, flow, query_quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        """
        Return a comprehensive set of conversion results for the provided inputs.  This method catches errors and
        returns a null result if no factors are found.

        This function is a wrapper to handle inputs.

        :param flow: a string that is synonymous with a flowable characterized by the query quantity
        :param query_quantity: convert to this quantity
        :param ref_quantity: [None] convert for 1 unit of this quantity
        :param context: [None] a string synonym for a context / "archetype"? (<== locale-specific?)
        :param locale: handled by CF; default 'GLO'
        :param kwargs:
         dist: CLookup distance (0=exact 1=subcompartments 2=parent compartment 3=all parents)
        :return: a 3-tuple of lists of QuantityConversion objects:
         [valid conversions],
         [geographic proxy conversions],
         [mismatched ref unit conversions]
        """
        flowable, rq, cx = self._get_flowable_info(flow, ref_quantity, context)
        if query_quantity is None:
            qq = None
        else:
            qq = self.get_canonical(query_quantity)
            if qq is None:
                raise EntityNotFound(qq)

        if qq == rq:  # is?
            return [QRResult(flowable, rq, qq, context or NullContext, locale, qq.origin, 1.0)], [], []

        try:
            return self._quantity_engine(flowable, rq, qq, cx, locale=locale, **kwargs)
        except NoFactorsFound:
            if qq is None:
                return [], [], []
            else:
                return [QuantityConversion.null(flowable, rq, qq, cx, locale, self.origin)], [], []

    def _quantity_relation(self, fb, rq, qq, cx, locale='GLO',
                           strategy=None, allow_proxy=True, **kwargs):
        """
        Reports the first / best result of a quantity conversion.  Returns a single QRResult interface
        (QuantityConversion result) that converts unit of the reference quantity into the query quantity for the given
        fb, context, and locale (default 'GLO').
        If the locale is not found, this would be a great place to run a spatial best-match algorithm.

        :param fb:
        :param rq:
        :param qq:
        :param cx:
        :param locale:
        :param strategy: approach for resolving multiple-CF in dist>0.  ('highest' | 'lowest' | 'average' | ...? )
          None = return first result
        :param allow_proxy: [True] in the event of 0 exact results but >0 geographic proxies, return a geographic
          proxy without error.
        :param kwargs:
        :return: a QRResult object or interface
        """
        try:
            qr_results, qr_geog, qr_mismatch = self._quantity_engine(fb, rq, qq, cx,
                                                                     locale=locale, **kwargs)
        except NoFactorsFound:
            qr_results, qr_geog, qr_mismatch = [QuantityConversion.null(fb, rq, qq, cx, locale, self.origin)], [], []

        if len(qr_results) == 0 and len(qr_geog) > 0:
            if allow_proxy:
                qr_results += qr_geog
            else:
                locs = ', '.join([qr.locale for qr in qr_geog])
                print('allow_proxy to show values with locales: %s' % locs)

        if len(qr_results) > 1:
            # this is obviously punting
            if strategy == 'first':
                best = qr_results[0]
            elif strategy is None or strategy == 'highest':  # highest CF is most conservative
                val = max(v.value for v in qr_results)
                best = next(v for v in qr_results if v.value == val)
            elif strategy == 'lowest':
                val = min(v.value for v in qr_results)
                best = next(v for v in qr_results if v.value == val)
            # elif strategy == 'average':
            #     return sum(v.value for v in qr_results) / len(qr_results)
            else:
                raise ValueError('Unknown strategy %s' % strategy)
        elif len(qr_results) == 1:
            best = qr_results[0]
        else:
            if len(qr_mismatch) == 0:
                raise NoFactorsFound

            best = None
        return best, qr_mismatch

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, locale='GLO',
                          strategy=None, allow_proxy=True, **kwargs):

        fb, rq, cx = self._get_flowable_info(flowable, ref_quantity, context)  # call only for exception handling
        qq = self.get_canonical(query_quantity)

        if qq == rq:  # is?
            return QRResult(flowable, rq, qq, context or NullContext, locale, qq.origin, 1.0)

        result, mismatch = self._quantity_relation(fb, rq, qq, cx, locale=locale,
                                                   strategy=strategy, allow_proxy=allow_proxy, **kwargs)
        if result is None:
            if len(mismatch) > 0:
                '''
                for k in mismatch:
                    print('Conversion failure: Flowable: %s\nfrom: %s %s\nto: %s %s' % (fb, k.fail, k.fail.link,
                                                                                        rq, rq.link))
                '''

                raise ConversionReferenceMismatch(mismatch[0])

            else:
                raise AssertionError('Something went wrong')
        return result

    def cf(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        """
        Should Always return a number and catch errors
        :param flow:
        :param quantity:
        :param ref_quantity: [None] taken from flow.reference_entity if flow is entity or locally known external_ref
        :param context: [None] taken from flow.reference_entity if flow is entity or locally known external_ref
        :param locale:
        :param kwargs: allow_proxy [False], strategy ['first'] -> passed to quantity_relation
        :return: the value of the QRResult found by the quantity_relation
        """
        try:
            qr = self.quantity_relation(flow, ref_quantity, quantity, context=context, locale=locale, **kwargs)
            return qr.value
        except ConversionReferenceMismatch:
            return 0.0

    def flat_qr(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        val = self.quantity_relation(flow, ref_quantity, quantity, context, locale=locale, **kwargs)
        return val.flatten(origin=self.origin)

    def profile(self, flow, ref_quantity=None, context=None, complete=False, **kwargs):
        """
        Generate characterizations for the named flow or flowable.  The positional argument is first used to retrieve
        a flow, and if successful, the reference quantity and context are taken for that flow.  Otherwise, the
        positional argument is interpreted as a flowable synonym and used to generate CFs, optionally filtered by
        context.  In that case, if no ref quantity is given then the CFs are returned as-reported; if a ref quantity is
        given then a ref quantity conversion is attempted and the resulting QRResult objects are returned.

        This whole interface desperately needs testing.

        :param flow:
        :param ref_quantity: [None]
        :param context: [None]
        :param complete: [False] if True, report all results including errors and geographic proxies
        :param kwargs:
        :return:
        """
        qrr, qrg, qrm = self.quantity_conversions(flow, None, ref_quantity, context, **kwargs)

        for r in qrr:
            yield r

        if complete:
            for r in qrg + qrm:
                yield r

    def norm(self, quantity, region=None, **kwargs):
        """

        :param quantity:
        :param region:
        :param kwargs:
        :return:
        """
        q = self.get_canonical(quantity)
        n = q.get('normalisationFactors', [])
        if len(n) == 0:
            return 0.0
        ix = 0
        if region is not None:
            try:
                ix = next(i for i, k in enumerate(q.get('normSets', [])) if k == region)
            except StopIteration:
                pass
        return n[ix]

    def do_lcia(self, quantity, inventory, locale='GLO', group=None, dist=2, **kwargs):
        """
        This is *almost* static. Could be moved into interface, except that it requires LciaResult (which is core).

        Successively implement the quantity relation over an iterable of exchanges.

        :param quantity:
        :param inventory: An iterable of exchange-like entries, having flow, direction, value, termination.  Currently
          also uses process.external_ref for hashing purposes, but that could conceivably be abandoned.
        :param locale: ['GLO']
        :param group: How to group scores.  Should be a lambda that operates on inventory items. Default x -> x.process
        :param dist: [2] controls how strictly to interpret exchange context.
          0 - exact context matches only;
          1 - match child contexts (code default)
          2 - match parent contexts [this default]
          3 - match any ancestor context, including Null
        :param kwargs:
        :return:
        """
        q = self.get_canonical(quantity)
        return do_lcia(q, inventory, locale=locale, group=group, dist=dist, **kwargs)

    def lcia(self, process, ref_flow, quantity_ref, **kwargs):
        """
        Implementation of foreground LCIA -- moved from LcCatalog
        :param process:
        :param ref_flow:
        :param quantity_ref:
        :param kwargs:
        :return:
        """
        p = self._archive.retrieve_or_fetch_entity(process)
        return do_lcia(quantity_ref, p.inventory(ref_flow=ref_flow),
                       locale=p['SpatialScope'])
