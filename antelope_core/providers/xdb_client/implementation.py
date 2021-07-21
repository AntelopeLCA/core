from antelope import IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface
from antelope import ExchangeRef, RxRef
from antelope_core.implementations import BasicImplementation
from antelope_core.models import (OriginCount, Entity, FlowEntity, Exchange, ReferenceExchange, UnallocatedExchange,
                                  DetailedLciaResult, AllocatedExchange)

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
        raise AttributeError('noooooo! %s' % key)
        rs = self._archive.r.get_one(list, _ref(key), 'references')
        if isinstance(rs[0], str):
            return rs[0]  # quantity - unitstring
        elif 'entity_id' in rs[0]:
            return self.get(rs[0]['entity_id'])
        else:
            p = self.get(key)
            return [RxRef(p, self.get(r.flow), r.direction, comment=r.comment) for r in rs]

    def properties(self, external_ref, **kwargs):
        return self._archive.r.get_many(str, 'properties', _ref(external_ref))

    def get_item(self, external_ref, item):
        return self._archive.r.get_raw(_ref(external_ref), 'doc', item)

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
            ex.termination = self.get_context(ex.termination)
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
    def do_lcia(self, quantity, inventory, locale='GLO', **kwargs):
        """

        :param quantity:
        :param inventory:
        :param locale:
        :param kwargs:
        :return:
        """
        exchanges = [UnallocatedExchange.from_inv(x) for x in inventory]
        return self._archive.r.post_return_many(exchanges, DetailedLciaResult, _ref(quantity), 'do_lcia')

