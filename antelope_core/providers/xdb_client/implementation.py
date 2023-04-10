# from collections import defaultdict
import json
from antelope import IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface
from antelope import ExchangeRef, RxRef, EntityNotFound, comp_dir
from antelope.models import (OriginCount, Entity, FlowEntity, Exchange, ReferenceExchange, UnallocatedExchange,
                             DetailedLciaResult, AllocatedExchange, Characterization as CharacterizationModel,
                             ExchangeValues, DirectedFlow)

from antelope_core.implementations import BasicImplementation
from antelope_core.lcia_results import LciaResult
from antelope_core.characterizations import Characterization, QRResult

from .xdb_entities import XdbEntity
from requests.exceptions import HTTPError


class BadClientRequest(Exception):
    pass


class RemoteExchange(Exchange):
    @property
    def is_reference(self):
        return self.type == 'reference'


class RemoteExchangeValues(ExchangeValues):
    @property
    def is_reference(self):
        return self.type == 'reference'


def _ref(obj):
    """
    URL-ize input argument
    :param obj:
    :return:
    """
    if hasattr(obj, 'external_ref'):
        return obj.external_ref
    return str(obj)


class XdbImplementation(BasicImplementation, IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface):
    """
    The implementation is very thin, so pile everything into one class
    """
    def setup_bm(self, query):
        return True

    def get_reference(self, key):
        p = self.get(key)
        if p.entity_type == 'process':
            rs = self._archive.r.get_many(ReferenceExchange, _ref(key), 'references')
            return [RxRef(p, self.get(r.flow.external_ref), r.direction, comment=r.comment) for r in rs]
        elif p.entity_type == 'flow':
            return self._archive.r.get_one(Entity, _ref(key), 'reference')
        elif p.entity_type == 'quantity':
            return self._archive.r.get_one(str, _ref(key), 'reference')
        else:
            raise TypeError(p.entity_type)

    def properties(self, external_ref, **kwargs):
        return self._archive.r.get_many(str, _ref(external_ref), 'properties')

    def get_item(self, external_ref, item):
        try:
            return self._archive.r.get_raw(_ref(external_ref), 'doc', item)
        except HTTPError as e:
            if e.args[0] == 404:
                raise KeyError(external_ref, item)
            raise e

    def get_uuid(self, external_ref):
        """
        Stopgap: don't support UUIDs
        :param external_ref:
        :return:
        """
        return self._archive.r.get_raw(_ref(external_ref), 'uuid')

    '''
    Index routes
    '''
    def count(self, entity_type, **kwargs):
        """
        Naturally the first route is problematic- because we allow incompletely-specified origins.
        We should sum them.
        :param entity_type:
        :param kwargs:
        :return:
        """
        return sum(k.count[entity_type] for k in self._archive.r.get_many(OriginCount, 'count'))

    def processes(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(Entity, 'processes', **llargs)]

    def flows(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(FlowEntity, 'flows', **llargs)]

    def quantities(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(Entity, 'quantities', **llargs)]

    def contexts(self, **kwargs):
        return self._archive.tm.contexts(**kwargs)

    def get_context(self, term, **kwargs):
        if isinstance(term, list) or isinstance(term, tuple):
            return self._archive.tm.get_context(term[-1])
        return self._archive.tm.get_context(term)

    def targets(self, flow, direction=None, **kwargs):
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(Entity, _ref(flow), 'targets')]

    '''
    Exchange routes
    '''
    def _resolve_ex(self, ex):
        self.get_canonical(ex.flow.quantity_ref)
        ex.flow = XdbEntity(FlowEntity.from_exchange_model(ex), self._archive)  # must get turned into a ref with make_ref

        if ex.type == 'context':
            ex.termination = self.get_context(ex.context)
        elif ex.type == 'cutoff':
            ex.termination = None
        return ex

    def _resolve_exv(self, exv: ExchangeValues):
        exv = self._resolve_ex(exv)
        if 'null' in exv.values:
            exv.values[None] = exv.values.pop('null')
        return exv

    def exchanges(self, process, **kwargs):
        """
        Client code (process_ref.ProcessRef) already turns them into ExchangeRefs
        :param process:
        :param kwargs:
        :return:
        """
        return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(RemoteExchange, _ref(process), 'exchanges'))

    def exchange_values(self, process, flow, direction=None, termination=None, reference=None, **kwargs):
        """

        :param process:
        :param flow:
        :param direction:
        :param termination:
        :param reference:
        :param kwargs:
        :return:
        """
        return list(self._resolve_exv(exv) for exv in self._archive.r.get_many(RemoteExchangeValues, _ref(process),
                                                                               'exchanges', _ref(flow)))

    def inventory(self, node, ref_flow=None, scenario=None, **kwargs):
        """
        Client code (process_ref.ProcessRef) already turns them into ExchangeRefs
        :param node:
        :param ref_flow: if node is a process, optionally provide its reference flow
        :param scenario: if node is a fragment, optionally provide a scenario- as string or tuple
        :param kwargs:
        :return:
        """
        if ref_flow and scenario:
            raise BadClientRequest('cannot specify both ref_flow and scenario')
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex)
                        for ex in self._archive.r.get_many(AllocatedExchange, _ref(node), _ref(ref_flow), 'inventory'))
        elif scenario:
            return list(self._resolve_ex(ex)
                        for ex in self._archive.r.get_many(AllocatedExchange, _ref(node), 'inventory',
                                                           scenario=scenario))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(node),
                                                                                'inventory'))

    def dependencies(self, process, ref_flow=None, **kwargs):
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                _ref(ref_flow), 'dependencies'))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                'dependencies'))

    def emissions(self, process, ref_flow=None, **kwargs):
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                _ref(ref_flow), 'emissions'))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                'emissions'))

    def cutoffs(self, process, ref_flow=None, **kwargs):
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                _ref(ref_flow), 'cutoffs'))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                'cutoffs'))

    def lci(self, process, ref_flow=None, **kwargs):
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex)
                        for ex in self._archive.r.get_many(AllocatedExchange, _ref(process), _ref(ref_flow), 'lci'))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process), 'lci'))

    def _to_exch_ref(self, x):
        """
        We can't resolve references from here! something is fucked
        :param x:
        :return:
        """
        pass

    def sys_lci(self, demand, **kwargs):
        dmd = [UnallocatedExchange.from_inv(x).dict() for x in demand]
        return list(self._resolve_ex(ex)  # DWR! THESE ARE NOT OPERATIONAL EXCHANGES YET!!!
                    for ex in self._archive.r.post_return_many(dmd, UnallocatedExchange, 'sys_lci', **kwargs))

    '''
    qdb routes
    '''
    def get_canonical(self, quantity, **kwargs):
        """

        :param quantity:
        :param kwargs:
        :return:
        """
        return self._archive.retrieve_or_fetch_entity(quantity, **kwargs)

    def _resolve_cf(self, cf: CharacterizationModel) -> Characterization:
        """

        :param cf:
        :return:
        """
        rq = self.get_canonical(cf.ref_quantity)
        qq = self.get_canonical(cf.query_quantity)
        cx = self.get_context(cf.context)
        c = Characterization(cf.flowable, rq, qq, cx, origin=cf.origin)
        for k, v in cf.value.items():
            c[k] = v
        return c

    def factors(self, quantity, flowable=None, context=None, **kwargs):
        """
        We need to construct operable characterizations with quantities that are recognized by the LciaEngine- in other
        words, with refs from our archive
        :param quantity:
        :param flowable:
        :param context: not implemented at the API
        :param kwargs:
        :return:
        """
        if flowable:
            facs = self._archive.r.get_many(CharacterizationModel, quantity, 'factors', flowable)
        else:
            facs = self._archive.r.get_many(CharacterizationModel, quantity, 'factors')
        return list(self._resolve_cf(cf) for cf in facs)

    def cf(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        """
        We still want to retain the ability to ask the remote server for CFs, even if we may prefer to get that
        info locally for local flows
        :param flow:
        :param quantity:
        :param ref_quantity: NOT USED
        :param context:
        :param locale:
        :param kwargs:
        :return:
        """
        try:
            return self._archive.r.get_one(float, _ref(flow), 'cf', _ref(quantity), context=context, locale=locale)
        except HTTPError:
            return 0.0

    @staticmethod
    def _result_from_exchanges(quantity, exch_map, res_m: DetailedLciaResult):
        """
        Constructs a detailed LCIA result using details provided by the backend server, populated with exchanges
        that we provided via POST.

        :param quantity:
        :param exch_map:
        :param res_m:
        :return:
        """
        res = LciaResult(quantity, scenario=res_m.scenario, scale=res_m.scale)
        nodes = set(v.process for v in exch_map.values())
        for c in res_m.components:
            try:
                node = next(v for v in nodes if v.external_ref == c.entity_id)
            except StopIteration:
                node = c.entity_id
            for d in c.details:
                key = (d.exchange.external_ref, tuple(d.exchange.context))
                ex = exch_map[key]
                val = d.result / d.factor.value
                if val != ex.value:
                    print('%s: value mismatch %g vs %g' % (key, val, ex.value))
                cf = QRResult(d.factor.flowable, ex.flow.reference_entity, quantity, ex.termination,
                              d.factor.locale, d.factor.origin, d.factor.value)
                res.add_score(c.component, ex, cf)
            for s in c.summaries:
                res.add_summary(c.component, node, s.node_weight, s.unit_score)
        return res

    def _result_from_model(self, process_ref, quantity, res_m: DetailedLciaResult):
        """
        Constructs a Detailed LCIA result from a background LCIA query, when we don't have a list of exchanges
        :param process_ref:
        :param quantity:
        :param res_m:
        :return:
        """
        res = LciaResult(quantity, scenario=res_m.scenario, scale=res_m.scale)
        process = self.get(process_ref)
        res.add_component(process_ref, entity=process)
        for c in res_m.components:
            for d in c.details:
                value = d.result / d.factor.value
                cx = self.get_context(d.exchange.context)
                ex = ExchangeRef(process, self.get(d.exchange.external_ref), comp_dir(cx.sense),
                                 termination=cx, value=value)
                rq = self.get_canonical(d.exchange.quantity_ref)
                cf = QRResult(d.factor.flowable, rq, quantity, cx,
                              d.factor.locale, d.factor.origin, d.factor.value)
                res.add_score(c.component, ex, cf)
            for s in c.summaries:
                res.add_summary(c.component, process, s.node_weight, s.unit_score)
        return res

    def do_lcia(self, quantity, inventory, locale='GLO', **kwargs):
        """

        :param quantity:
        :param inventory:
        :param locale:
        :param kwargs:
        :return:
        """
        exchanges = [UnallocatedExchange.from_inv(x).dict() for x in inventory]
        exch_map = {(x.flow.external_ref, x.term_ref): x for x in inventory}

        ress = self._archive.r.qdb_post_return_many(exchanges, DetailedLciaResult, _ref(quantity), 'do_lcia')
        return [self._result_from_exchanges(quantity, exch_map, res) for res in ress]
        if len(res_out) == 1:
            return res_out[0]
        return res_out

    def bg_lcia(self, process, query_qty, observed=None, ref_flow=None, **kwargs):
        """
        We want to override the interface implementation and send a simple request to the backend
        :param process:
        :param query_qty:
        :param observed:
        :param ref_flow:
        :param kwargs: locale, quell_biogenic_co2
        :return:
        """
        obs_flows = ()
        try:
            if observed:
                obs_flows = [DirectedFlow.from_exchange(x).dict() for x in observed]
            if len(obs_flows) > 0:
                if ref_flow:
                    ress = self._archive.r.post_return_many(obs_flows, DetailedLciaResult, _ref(process), _ref(ref_flow),
                                                            'lcia', _ref(query_qty), **kwargs)
                else:
                    ress = self._archive.r.post_return_many(obs_flows, DetailedLciaResult, _ref(process),
                                                            'lcia', _ref(query_qty), **kwargs)
            else:
                if ref_flow:
                    ress = self._archive.r.get_many(DetailedLciaResult, _ref(process), _ref(ref_flow),
                                                    'lcia', _ref(query_qty), **kwargs)
                else:
                    ress = self._archive.r.get_many(DetailedLciaResult, _ref(process),
                                                    'lcia', _ref(query_qty), **kwargs)
        except HTTPError as e:
            if e.args[0] == 404:
                content = json.loads(e.args[1])
                raise EntityNotFound(content['detail'])
            else:
                raise
        return [self._result_from_model(process, query_qty, res) for res in ress]
