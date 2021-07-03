from antelope import IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface
from antelope import ExchangeRef, RxRef
from antelope_core.implementations import BasicImplementation
from antelope_core.models import *

from .xdb_entities import XdbEntity


class XdbImplementation(BasicImplementation, IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface):
    """
    The implementation is very thin, so pile everything into one class
    """
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
    def _resolve_term(self, ex):
        if ex.type == 'context':
            ex.termination = self.get_context(ex.termination)
        elif ex.type == 'cutoff':
            ex.termination = None
        return ex.termination

    def exchanges(self, process, **kwargs):
        """
        Client code already turns them into ExchangeRefs
        :param process:
        :param kwargs:
        :return:
        """
        return list(self._resolve_term(ex) for ex in self._archive.r.get_many(Exchange, process, 'exchanges'))
