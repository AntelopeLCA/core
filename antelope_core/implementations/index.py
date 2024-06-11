from collections import defaultdict

from antelope import IndexInterface, comp_dir, CONTEXT_STATUS_
from .basic import BasicImplementation
from ..contexts import NullContext
from ..entities import MetaQuantityUnit


class IndexImplementation(BasicImplementation, IndexInterface):
    """
    A CatalogInterface provides basic-level semantic data about entities

    Only requires the abstract ArchiveImplementation

    Attribute requirements for the archive:
     - everything required by BasicImplementation
     - entities_by_type()
     - count_by_type()
     - search(), including implicitly _narrow_search()
    """
    def __init__(self, *args, cutoffs=False, **kwargs):
        super(IndexImplementation, self).__init__(*args, **kwargs)
        if not hasattr(self._archive, 'ti'):
            self.re_index(cutoffs=cutoffs)

    @property
    def _terminations(self):
        return self._archive.ti

    def re_index(self, cutoffs=False):
        self._archive.ti = defaultdict(set)
        self._index_terminations(cutoffs=cutoffs)

    def _index_terminations(self, cutoffs=False):
        """
        This can't be done on add because new processes may get stored before their references are setup.
        This should only be run if the archive is local
        :param cutoffs: [False] if true, terminations include all cutoff [null-termination] exchanges, not just
          references
         :NOTE: until context refactor there is no consistent usage of terminations, so we will skip this
        :return:
        """
        for p in self._archive.entities_by_type('process'):
            if cutoffs and CONTEXT_STATUS_ != 'compat':
                for x in p.exchanges():
                    if x.termination is None:
                        self._terminations[x.flow.external_ref].add((x.direction, p))
            else:
                for rx in p.reference_entity:
                    self._terminations[rx.flow.external_ref].add((rx.direction, p))
    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """
    def count(self, entity_type, **kwargs):
        return self._archive.count_by_type(entity_type)

    def processes(self, literal=False, **kwargs):
        for p in self._archive.search('process', **kwargs):
            yield p

    def flows(self, **kwargs):
        for f in self._archive.search('flow', **kwargs):
            yield f

    def quantities(self, unit=None, **kwargs):
        for q in self._archive.search('quantity', **kwargs):
            if unit is not None:
                if q.unit != unit:
                    continue
            yield q

    def lcia_methods(self, **kwargs):
        """
        Generate LCIA Methods-- which are quantities that have defined indicators
        :param kwargs:
        :return:
        """
        indicator = kwargs.pop('Indicator', '')
        for i in filter(lambda x: x.has_property('Indicator'), self.quantities(Indicator=indicator, **kwargs)):
            yield i

    def lcia(self, **kwargs):
        """
        Generate LCIA Methodologies or method collections- these are specified by having the singleton MetaQuantityUnit
        (and/or the unitstring '0') as a unit. They should *not* have indicators.
        They have a property 'impactCategories' which is a list of included lcia_methods
        :param kwargs:
        :return:
        """
        for q in self.quantities():
            if q.reference_entity is MetaQuantityUnit:
                yield q

    def targets(self, flow_ref, direction=None, **kwargs):
        """
        Generate processes in the archive that terminate a given exchange i.e. - have the same flow and a complementary
        direction.  If refs_only is specified, only report processes that terminate the exchange with a reference
        exchange.
        :param flow_ref: flow, exchange, or flow's external key
        :param direction: [None] filter
        :return:
        """
        if hasattr(flow_ref, 'entity_type'):
            if flow_ref.entity_type == 'flow':
                flow_ref = flow_ref.external_ref
            elif flow_ref.entity_type == 'exchange':
                if direction is None:
                    direction = flow_ref.direction
                flow_ref = flow_ref.flow.external_ref

        cdir = comp_dir(direction)
        if not isinstance(flow_ref, str) and not isinstance(flow_ref, int):  # NSUUID archives can use integer ids
            flow_ref = flow_ref.external_ref
        for x in self._terminations[flow_ref]:  # defaultdict, so no KeyError
            if direction is None:
                yield x[1]
            else:
                if cdir == x[0]:
                    yield x[1]

    def flowables(self, **kwargs):
        return self._archive.tm.flowables(**kwargs)

    def contexts(self, **kwargs):
        return self._archive.tm.contexts(**kwargs)

    '''
    def get_context(self, term, **kwargs):
        """
        I think this needs to be moved into the quantity interface
        :param term:
        :param kwargs:
        :return:
        """
        cx = self._archive.tm[term]
        if cx is None:
            return NullContext
        return cx
    '''

    '''
    def mix(self, flow_ref, direction):
        if not isinstance(flow_ref, str):
            flow_ref = flow_ref.external_ref
        terms = [t for t in self.terminate(flow_ref, direction=direction)]
        flow = self[flow_ref]
        p = LcProcess.new('Market for %s' % flow['Name'], Comment='Auto-generated')
        p.add_exchange(flow, comp_dir(direction), value=float(len(terms)))
        p.add_reference(flow, comp_dir(direction))
        for t in terms:
            p.add_exchange(flow, direction, value=1.0, termination=t.external_ref)
        return p
    '''

    '''
    def terminate(self, flow, direction=None, **kwargs):
        for p in self._archive.terminate(flow, direction=direction, **kwargs):
            yield self.make_ref(p)

    def originate(self, flow, direction=None, **kwargs):
        for p in self._archive.originate(flow, direction=direction, **kwargs):
            yield self.make_ref(p)

    def mix(self, flow, direction, **kwargs):
        return self._archive.mix(flow, direction, **kwargs)
    '''
    def unmatched_flows(self, flows, **kwargs):
        """
        Takes in a list of flows and generates a sublist of flows that were not recognized as synonyms to any local
        flows.

        Uses TermManager.get_flowable which tries the best of flow.synonyms, flow.name, str(flow)

        :param flows:
        :param kwargs:
        :return:
        """
        for flow in flows:
            try:
                self._archive.tm.get_flowable(flow)
            except KeyError:
                yield flow
