from antelope import IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface
from antelope import ExchangeRef, RxRef
from antelope_core.implementations import BasicImplementation
from antelope_core.models import (OriginCount, Entity, FlowEntity, Exchange, ReferenceExchange, UnallocatedExchange,
                                  DetailedLciaResult)

from .xdb_entities import XdbEntity


class RemoteExchange(Exchange):
    @property
    def is_reference(self):
        return self.type == 'reference'


class XdbImplementation(BasicImplementation, IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface):
    """
    The implementation is very thin, so pile everything into one class
    """
    def get_reference(self, key):
        rs = self._archive.r.get_one(list, key, 'references')
        if isinstance(rs[0], str):
            return rs[0]  # quantity - unitstring
        elif 'entity_id' in rs[0]:
            return self.get(rs[0]['entity_id'])
        else:
            p = self.get(key)
            return [RxRef(p, self.get(r.flow), r.direction, comment=r.comment) for r in rs]

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
        tgts = self._archive.r.get_many(ReferenceExchange, flow, 'targets')
        return [RxRef(self.get(tgt.process), self.get(tgt.flow), tgt.direction, tgt.comment) for tgt in tgts]

    '''
    Exchange routes
    '''
    def _resolve_ex(self, ex):
        ex.flow = self.get(ex.flow)
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
        return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(RemoteExchange, process, 'exchanges'))

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
        return self._archive.r.post_return_many(exchanges, DetailedLciaResult, quantity, 'do_lcia')

