from collections import defaultdict

from antelope import IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface
from antelope import comp_dir, ExchangeRef, RxRef
from antelope_core.implementations import BasicImplementation
from antelope_core.models import (OriginCount, Entity, FlowEntity, Exchange, ReferenceExchange, UnallocatedExchange,
                                  DetailedLciaResult, AllocatedExchange)
from antelope_core.lcia_results import LciaResult
from antelope_core.characterizations import QRResult

from .xdb_entities import XdbEntity


class BadClientRequest(Exception):
    pass


class RemoteExchange(Exchange):
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
    def get_reference(self, key):
        rs = self._archive.r.get_one(list, _ref(key), 'references')
        if isinstance(rs[0], str):
            return rs[0]  # quantity - unitstring
        elif 'entity_id' in rs[0]:
            return self.get(rs[0]['entity_id'])
        else:
            p = self.get(key)
            return [RxRef(p, self.get(r.flow), r.direction, comment=r.comment) for r in rs]

    def properties(self, external_ref, **kwargs):
        return self._archive.r.get_many(str, _ref(external_ref), 'properties')

    def get_item(self, external_ref, item):
        return self._archive.r.get_raw(_ref(external_ref), 'doc', item)

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
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(Entity, 'processes', **kwargs)]

    def flows(self, **kwargs):
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(FlowEntity, 'flows', **kwargs)]

    def quantities(self, **kwargs):
        return [XdbEntity(k, self._archive) for k in self._archive.r.get_many(Entity, 'quantities', **kwargs)]

    def contexts(self, **kwargs):
        return self._archive.tm.contexts(**kwargs)

    def get_context(self, term, **kwargs):
        if isinstance(term, list):
            return self._archive.tm.get_context(term[-1])
        return self._archive.tm.get_context(term)

    def targets(self, flow, direction=None, **kwargs):
        tgts = self._archive.r.get_many(ReferenceExchange, _ref(flow), 'targets')
        return [RxRef(self.get(tgt.process), self.get(tgt.flow), tgt.direction, tgt.comment) for tgt in tgts]

    '''
    Exchange routes
    '''
    def _resolve_ex(self, ex):
        ex.flow = XdbEntity(FlowEntity.from_exchange_model(ex), self._archive)  # must get turned into a ref with make_ref
        if ex.type == 'context':
            ex.termination = self.get_context(ex.context)
        elif ex.type == 'cutoff':
            ex.termination = None
        return ex

    def exchanges(self, process, **kwargs):
        """
        Client code already turns them into ExchangeRefs
        :param process:
        :param kwargs:
        :return:
        """
        return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(RemoteExchange, _ref(process), 'exchanges'))

    def inventory(self, node, ref_flow=None, scenario=None, **kwargs):
        """
        Client code already turns them into ExchangeRefs
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
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(node), 'inventory'))

    def lci(self, process, ref_flow=None, **kwargs):
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex)
                        for ex in self._archive.r.get_many(AllocatedExchange, _ref(process), _ref(ref_flow), 'lci'))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process), 'lci'))

    '''
    qdb routes
    '''
    def get_canonical(self, quantity, **kwargs):
        """

        :param quantity:
        :param kwargs:
        :return:
        """
        return self._archive.r.qdb_get_one(Entity, _ref(quantity))

    def _result_from_model(self, quantity, exch_map, res_m: DetailedLciaResult):
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

        ress = self._archive.r.post_return_many(exchanges, DetailedLciaResult, _ref(quantity), 'do_lcia')
        return [self._result_from_model(quantity, exch_map, res) for res in ress]

