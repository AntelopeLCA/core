"""
This object replaces the LciaResult types spelled out in Antelope-- instead, it serializes to an LCIA result directly.

"""
from antelope import comp_dir  # , CatalogRef, ExchangeRef
from .exchanges import ExchangeValue, DissipationExchange
from .autorange import AutoRange
from numbers import Number
from math import isclose
from collections import defaultdict
from antelope import CatalogRef
import logging


q_contrib = CatalogRef(origin='local.qdb', external_ref='contribution_share', entity_type='quantity',
                       reference_entity='share',
                       Name='Contribution Analysis', uuid='365e6e6c-80d8-45b4-8675-f6acdaa47ea9')

q_percent = CatalogRef(origin='local.qdb', external_ref='contribution_percent', entity_type='quantity',
                       reference_entity='percent',
                       Name='Contribution Analysis (percent)', uuid='88bf6be4-6cbf-4b7a-89f5-3ced3ed59ff6')


from antelope.models import SummaryLciaScore, DisaggregatedLciaScore, LciaDetail
# from lcatools.interfaces import to_uuid


class InconsistentQuantity(Exception):
    pass


class InconsistentScenario(Exception):
    pass


class DuplicateResult(Exception):
    pass


class InconsistentScores(Exception):
    pass


class InconsistentSummaries(Exception):
    pass


def number(val):
    try:
        return '%10.3g' % val
    except TypeError:
        return '%10.10s' % '----'


class StringEntity(object):
    """
    To be used in cases where a component's entity is a plain string, to still allow aggregation
    """
    def __init__(self, name):
        self.name = str(name)

    def __getitem__(self, item):
        return item

    def get(self, item, default=None):
        if default:
            return default
        return self.__getitem__(item)

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if hasattr(other, 'name'):
            return self.name == other.name
        return self.name == str(other)

    def __hash__(self):
        return hash(self.name)


class DetailedLciaResult(object):
    """
    Contains exchange, factor, result
    """
    def __init__(self, lc_result, exchange, qrresult):
        """

        :param lc_result:
        :param exchange:
        :param qrresult: meets the QRResult spec: has properties 'flowable', 'ref', 'query', 'context', 'locale',
        'origin', 'value'
        'ref' has to have property 'unit'
        'context' has to *be* a context (with 'sense')
        """
        if exchange.flow.unit != qrresult.ref.unit and qrresult.value != 0.0:
            print('%s: Inconsistent qty\nexch: %s\nqrr:  %s' % (self.__class__.__name__, exchange.flow.reference_entity, qrresult))
            #  raise InconsistentQuantity('%s\n%s' % (exchange.flow.reference_entity, qrresult))
        self._exchange = exchange
        self._qr = qrresult
        self._lc = lc_result

    @property
    def exchange(self):
        return self._exchange

    @property
    def factor(self):
        return self._qr

    @property
    def _dirn_adjust(self):
        if self._qr.context.sense is None:
            return 1.0
        elif comp_dir(self._qr.context.sense) == self._exchange.direction:
            return 1.0
        return -1.0

    @property
    def is_null(self):
        return self.result == 0

    @property
    def flow(self):
        return self._exchange.flow

    @property
    def name(self):
        return '; '.join(filter(None, (self.flowable, self.context)))

    @property
    def direction(self):
        return self._exchange.direction

    @property
    def flowable(self):
        return str(self._qr.flowable)

    @property
    def context(self):
        if self.exchange.termination is not None:
            return str(self.exchange.termination)
        return str(self._qr.context)

    @property
    def content(self):
        if isinstance(self._exchange, DissipationExchange):
            return self._exchange.content()
        return None

    @property
    def value(self):
        if self._exchange.value is None:
            return 0.0
        return self._exchange.value * self._lc.scale

    @property
    def result(self):
        if self._qr.value is None:
            return 0.0
        return self.value * self._dirn_adjust * self._qr.value

    def __hash__(self):
        return hash((self._exchange.process.external_ref, self._exchange.direction, self._qr.flowable, self._qr.context))

    def __eq__(self, other):
        if not isinstance(other, DetailedLciaResult):
            return False
        return (self.exchange.process.external_ref == other.exchange.process.external_ref and
                self.flowable == other.flowable and
                self.direction[0] == other.direction[0] and
                self.context == other.context)

    def __str__(self):
        if self._dirn_adjust == -1:
            dirn_mod = '*'
        else:
            dirn_mod = ' '

        if self._qr.value is None:
            cf = '(CF error)'
        else:
            cf = number(self._qr.value * self._lc.autorange)
        return '%s%s = %-s  x %-s [%s] %s, %s' % (dirn_mod,
                                                  number(self.result * self._lc.autorange),
                                                  cf,
                                                  number(self.value),
                                                  self._qr.locale,
                                                  self.flowable,
                                                  self.context)

    def serialize(self):
        return LciaDetail.from_detailed_lcia_result(self)


class SummaryLciaResult(object):
    """
    A container for a separately computed Lcia result, *especially* for the results of a fragment LCIA.
    Includes a node weight and a unit score-- its cumulative result is the product of these.
    If the unit score is a number, the summary is static.

    However, the unit_score can itself _be_ an LciaResult, making the datatype recursive.

    This has __add__ functionality, for merging repeated instances of the same fragment during traversal
    """
    def __init__(self, lc_result, entity, node_weight, unit_score):
        """
        :param lc_result: who "owns" you. scale report by their scale.
        entity_id must either have get_uuid() or be hashable
        :param entity: a hashable identifier
        :param node_weight: stand-in for exchange value
        :param unit_score: stand-in for factor value
        """
        if isinstance(entity, str):
            entity = StringEntity(entity)
        self.entity = entity
        self._node_weight = node_weight
        if isinstance(unit_score, Number):
            self._static_value = unit_score
            self._internal_result = None
        else:
            self._static_value = None
            self._internal_result = unit_score

        self._lc = lc_result

    def update_parent(self, lc):
        self._lc = lc

    @property
    def name(self):
        if isinstance(self.entity, tuple):
            return '; '.join(str(k) for k in self.entity)
        try:
            return self.entity.name
        except AttributeError:
            return str(self.entity)

    @property
    def id(self):
        try:
            return self.entity.fragment.external_ref
        except AttributeError:
            try:
                return self.entity.external_ref
            except AttributeError:
                return str(self.entity)

    @property
    def origin(self):
        try:
            return self.entity.fragment.origin
        except AttributeError:
            try:
                return self.entity.origin
            except AttributeError:
                return 'None'

    @property
    def static(self):
        return self._static_value is not None

    @property
    def is_null(self):
        if self.static:
            if self._static_value == 0:
                return True
            return False
        return self._internal_result.is_null

    @property
    def node_weight(self):
        return self._node_weight * self._lc.scale

    @property
    def unit_score(self):
        if self.static:
            return self._static_value
        else:
            return self._internal_result.total()

    @property
    def internal_result(self):
        if self.static:
            return self._static_value
        else:
            return self._internal_result

    @property
    def cumulative_result(self):
        return self.node_weight * self.unit_score

    @property
    def result(self):
        """
        must workalike with both AggregateLciaScores and DetailedLciaResults
        :return:
        """
        return self.cumulative_result

    def components(self):
        if self.static:
            yield self
        else:
            for x in self._internal_result.components():
                yield x

    def __hash__(self):
        return hash(self.entity)

    def __eq__(self, other):
        if not isinstance(other, SummaryLciaResult):
            return False
        return self.entity == other.entity

    def __str__(self):
        return 'S%s = %-s x %-s | %s' % (number(self.cumulative_result * self._lc.autorange), number(self.node_weight),
                                         number(self.unit_score * self._lc.autorange),
                                         self.entity)

    def show(self, **kwargs):
        if self.static:
            print('%s' % self)
        else:
            self._internal_result.show(**kwargs)

    def show_detailed_result(self, **kwargs):
        if self.static:
            self.show(**kwargs)
        else:
            self._internal_result.show_components(**kwargs)

    def flatten(self):
        if self.static:
            return self
        return self._internal_result.flatten(_apply_scale=self.node_weight)

    def __add__(self, other):
        """
        Add two summary LCIA results together.  This only works under the following circumstances:
         * different instances of the same entity are being added (e.g. two instances of the same flow).
           In this case, the two summaries' entities must compare as equal and their unit scores must be equal.
           The node weights are added.  Scale is ignored (scale is inherited from the primary summary)

         * Two static-valued summaries are added together.  In this case, either the scores must be equal (in which case
           the node weights are summed) or the node weights must be equal, and the unit scores are summed.

        This is the sort of garbage that should be unittested.

        :param other:
        :return:
        """
        if not isinstance(other, SummaryLciaResult):
            raise TypeError('Can only add SummaryLciaResults together')
        if self._lc is not other._lc:
            raise InconsistentSummaries('These summaries do not belong to the same LciaResult')
        if self.static:
            if other.static:
                # either the node weights or the unit scores must be equal
                if self.unit_score == other.unit_score:  # make node weights add preferentially
                    unit_score = self.unit_score
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                elif self.node_weight == other.node_weight:
                    _node_weight = self._node_weight
                    unit_score = self._static_value + other.unit_score
                else:
                    # in conflicts, prefer unit node weights if they exist (i.e. in aggregations)
                    if self.node_weight == 1.0 or other.node_weight == 1.0:  # we still need this for grouping by stage
                        _node_weight = 1.0
                        unit_score = self.cumulative_result + other.cumulative_result
                    else:
                        # if these are richly detailed, it is wrong to add them
                        raise InconsistentScores('These summaries do not add together:\n%s\n%s' % (self, other))
            else:
                if self.unit_score == other.unit_score:
                    unit_score = other._internal_result
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                else:
                    raise InconsistentScores('These summaries have different unit scores')
        elif self.unit_score == other.unit_score:
            unit_score = self._internal_result
            if other.static:
                _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
            elif self._internal_result is other._internal_result:
                # this only works because terminations cache unit scores
                # just sum the node weights, ignoring our local scaling factor (DWR!)
                if self.entity == other.entity:
                    # WARNING: FragmentFlow equality does not include magnitude or node weight
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                else:
                    print("entities do not match\n self: %s\nother: %s" % (self.entity, other.entity))
                    raise InconsistentSummaries
            else:
                """
                This situation is cropping up in the CalRecycle model but it appears to be kosher. I propose the 
                following test: if the two summaries are both nonstatic and (a) have the same set of components and (b) 
                have the same unit scores, then treat them as the same.
                """
                if set(k.entity for k in self.components()) == set(k.entity for k in other.components()):
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                else:
                    raise InconsistentSummaries('Components differ between non-static summaries')
        else:
            print('\n%s' % self)
            print(other)
            raise InconsistentSummaries('At least one not static, and unit scores do not match')
        return SummaryLciaResult(self._lc, self.entity, _node_weight, unit_score)

    def serialize(self, detailed=False):
        """
        If detailed is True, this should return DisaggregatedLciaScores
        If detailed is False, this should return SummaryLciaScores
        :param detailed:
        :return:
        """
        if detailed:
            if not self.static:
                f = self.flatten()  # will yield a list of aggregate lcia scores with one component each
                details = [d.serialize() for p in f.components() for d in p.details()]
                return DisaggregatedLciaScore(component=self.name, result=self.cumulative_result,
                                              node_weight=self.node_weight, unit_score=self.unit_score,
                                              origin=self.origin, entity_id=self.id,
                                              details=details)
        return SummaryLciaScore(component=self.name, result=self.cumulative_result,
                                node_weight=self.node_weight, unit_score=self.unit_score,
                                origin=self.origin, entity_id=self.id)


class SummaryLciaMissing(SummaryLciaResult):
    @property
    def name(self):
        return 'Missing %s' % self.entity

    is_null = False

    def __str__(self):
        return '%s = %-s x (MISSING)  | %s' % (
            number(self.cumulative_result * self._lc.autorange), number(self.node_weight),
            self.entity)

    def serialize(self, detailed=False):
        print('serialize SummaryLciaMissing - suspected broken')
        if detailed:
            return DisaggregatedLciaScore(component=self.name, result=self.cumulative_result,
                                          node_weight=self.node_weight, unit_score=self.unit_score,
                                          origin=self.origin, entity_id=self.id,
                                          details=[])
        return SummaryLciaScore(component=self.name, result=self.cumulative_result,
                                node_weight=self.node_weight, unit_score=self.unit_score,
                                origin=self.origin, entity_id=self.id)


class AggregateLciaResult(object):
    """
    contains an entityId which could be a process or a fragment (but presents as a process, i.e. with exchanges)
    The Aggregate score is constructed from individual LCIA Details (exchange value x characterization factor)
    """

    def __init__(self, lc_result, entity):
        if isinstance(entity, str):
            entity = StringEntity(entity)
        self.entity = entity
        self._lc = lc_result
        self.LciaDetails = []  # what exactly was having unique membership protecting us from??

    def update_parent(self, lc_result):
        self._lc = lc_result

    @property
    def name(self):
        if isinstance(self.entity, tuple):
            return '; '.join(str(k) for k in self.entity)
        try:
            return self.entity.name
        except AttributeError:
            return str(self.entity)

    @property
    def cumulative_result(self):
        if len(self.LciaDetails) == 0:
            return 0.0
        return sum([i.result for i in self.LciaDetails])

    @property
    def is_null(self):
        for i in self.LciaDetails:
            if not i.is_null:
                return False
        return True

    '''
    def _augment_entity_contents(self, other):
        """
        when duplicate fragmentflows are aggregated, their node weights and magnitudes should be added together
        :return:
        """
        self.entity = self.entity + other
    '''

    def add_detailed_result(self, exchange, qrresult):
        d = DetailedLciaResult(self._lc, exchange, qrresult)
        '''
        if d in self.LciaDetails:  # process.uuid, direction, flow.uuid are the same
            if factor[location] != 0:
                other = next(k for k in self.LciaDetails if k == d)
                raise DuplicateResult('exchange: %s\n  factor: %s\nlocation: %s\nconflicts with %s' %
                                      (exchange, factor, location, other.factor))
            else:
                # do nothing
                return
        '''
        self.LciaDetails.append(d)

    def show(self, **kwargs):
        self.show_detailed_result(**kwargs)

    def details(self):
        """
        generator of nonzero detailed results
        :return:
        """
        for d in self.LciaDetails:
            if d.result != 0:
                yield d

    def show_detailed_result(self, key=lambda x: x.result, show_all=False, count=None, threshold=None):
        residual = 0.0
        resid_c = 0
        rev = bool(self.cumulative_result > 0)  # we need to reverse the reverse-sort if results are negative
        for d in sorted(self.LciaDetails, key=key, reverse=rev):
            if d.result != 0 or show_all:
                if count is not None and count <= 0 and not show_all:
                    residual += d.result
                    resid_c += 1
                    continue
                if threshold is not None and abs(d.result) < abs(threshold) and not show_all:
                    residual += d.result
                    resid_c += 1
                    continue
                print('%s' % d)
                if count is not None:
                    count -= 1
        if residual != 0.0:
            print(' %s   remainder (%d items)' % (number(residual), resid_c))
        # print('=' * 60)
        # print('             Total score: %g ' % self.cumulative_result)

    def __str__(self):
        return 'A%s  %s' % (number(self.cumulative_result * self._lc.autorange), self.entity)

    def serialize(self, detailed=False):
        """
        If detailed is True, this should return DisaggregatedLciaScores
        If detailed is False, this should return SummaryLciaScores
        :param detailed:
        :return:
        """
        j = {
            'result': self.cumulative_result,
            'component': self.name
        }
        if hasattr(self.entity, 'external_ref'):
            j['entity_id'] = self.entity.external_ref
            j['origin'] = self.entity.origin
        else:
            j['origin'] = 'None'
            j['entity_id'] = str(self.entity)

        if detailed:
            j['details'] = [p.serialize() for p in self.LciaDetails]
            return DisaggregatedLciaScore(**j)

        j['node_weight'] = 1.0
        j['unit_score'] = j['result']
        return SummaryLciaScore(**j)


def show_lcia(lcia_results):
    """
    Takes in a dict of uuids to lcia results, and summarizes them in a neat table
    :param lcia_results:
    :return:
    """
    print('LCIA Results\n%s' % ('-' * 60))
    for r in lcia_results.values():
        print('%10.5g %s' % (r.total(), r.quantity))


class MixedComponents(Exception):
    """
    We are now suddenly deciding that an LciaResult may not contain a mixture of AggregateLciaScores and SummaryLciaScores
    """
    pass


class LciaResult(object):
    """
    An LCIA result object contains a collection of LCIA results for a related set of entities, called components.  Each
     component is an AggregateLciaResult, which itself is a collection of either detailed LCIA results or summary scores.

    Each component which is a FragmentFlow represents a specific traversal scenario and is thus static.

    Each component which is a process will contain actual exchanges and factors, which are scenario-sensitive, and
     so is (theoretically) dynamic. This is not yet useful in practice.  LCIA Results are in sharp need of testing /
     refactoring.
    """
    _type = None

    def _check_type(self, t):
        if self._type is None:
            self._type = t
            return
        if t != self._type:
            self.show_components()
            raise MixedComponents(self._type, t)

    def __init__(self, quantity, scenario=None, private=False, scale=1.0, autorange=False):
        """
        If private, the LciaResult will not return any unaggregated results
        :param quantity:
        :param scenario:
        :param private:
        """
        self.quantity = quantity
        self.scenario = scenario
        self._scale = scale
        self._LciaScores = dict()
        self._cutoffs = []
        self._errors = []
        self._zeros = []

        self._private = private
        self._autorange = None
        self._failed = []
        if autorange:
            self.set_autorange()

    @property
    def has_summaries(self):
        return any(isinstance(c, SummaryLciaResult) for c in self._LciaScores.values())

    @property
    def is_null(self):
        for i in self._LciaScores.values():
            if not i.is_null:
                return False
        return True

    def set_autorange(self, value=None, **kwargs):
        """
        Update the AutoRange object. Should be done before results are presented, if auto-ranging is in use.

        Auto-ranging affects the following outputs:
         * any show() or printed string
         * the results of contrib_new()
         * a_total()

        No other outputs are affected.

        :param value:
        :return:
        """
        if value:
            self._autorange = AutoRange(value, **kwargs)
        elif value is False:
            self.unset_autorange()
        else:
            self._autorange = AutoRange(self.span, **kwargs)

    def unset_autorange(self):
        self._autorange = None

    @property
    def autorange(self):
        if self._autorange is None:
            return 1.0
        else:
            return self._autorange.scale

    @property
    def unit(self):
        if self._autorange is not None:
            return self._autorange.adj_unit(self.quantity.unit)
        else:
            return self.quantity.unit

    @property
    def scale(self):
        return self._scale

    def scale_result(self, scale):
        """
        why is this a function?
        """
        self._scale *= scale

    def _match_key(self, item):
        """
        if item is a known key, return its match
        if item is a seen entity, return its component
        :param item:
        :return:
        """
        if item in self._LciaScores:
            yield self._LciaScores[item]
        else:
            for k, v in self._LciaScores.items():
                if item == v.entity:
                    yield v
                # elif str(v.entity).startswith(str(item)):
                #     yield v
                # elif str(k).startswith(str(item)):
                #     yield v

    def __getitem__(self, item):
        try:
            return next(self._match_key(item))
        except StopIteration:
            raise KeyError('%s' % item)

    def __len__(self):
        return len(self._LciaScores)

    '''
    def __getitem__(self, item):
        if isinstance(item, int):
            return
    '''
    def contrib(self, percent=False, count=None):
        """
        Convert the LCIA result to a result with a unitary value.  If the score is an Aggregated score, it will first
        be aggregated by flow name
        :param percent: [False] if True, the score will add up to 100
        :param count: [None] number of distinct categories to report (plus remainder)
        :return:
        """
        if percent:
            q = q_percent
            check = 100.0
        else:
            q = q_contrib
            check = 1.0

        norm = self.total()
        contrib = LciaResult(q, scenario=self.scenario)
        residual = 0.0
        resid_c = 0
        rev = bool(self.total() > 0)   # we need to reverse the reverse-sort if results are negative

        if self.has_summaries:
            for k, c in sorted(self._LciaScores.items(), key=lambda x: x[1].cumulative_result, reverse=rev):
                if count is not None:
                    if count <= 0:
                        residual += c.cumulative_result
                        resid_c += 1
                        continue
                    else:
                        count -= 1
                contrib.add_summary(k, c.entity, check, c.cumulative_result / norm)
        else:
            flat = self.flatten()  # this ensures meaningful components
            agg = flat.aggregate(key=str)
            for k, c in sorted(agg._LciaScores.items(), key=lambda x: x[1].cumulative_result, reverse=rev):
                if count is not None:
                    if count <= 0:
                        residual += c.cumulative_result
                        resid_c += 1
                        continue
                    else:
                        count -= 1
                contrib.add_summary(k, c.entity, check, c.cumulative_result / norm)

        if residual != 0.0:
            k = 'remainder (%d items)' % resid_c
            contrib.add_summary(k, k, check, residual / norm)

        if not isclose(check, contrib.total(), rel_tol=1e-6):
            raise ValueError('Total %g does not match target %g !' % (contrib.total(), check))
        return contrib

    def aggregate(self, key=lambda x: x.get('StageName'), entity_id=None):
        """
        returns a new LciaResult object in which the components of the original LciaResult object are aggregated into
        static values according to a key.  The key is a lambda expression that is applied to each AggregateLciaResult
        component's entity property (components where the lambda fails will all be grouped together).

        The special key '*' will aggregate all components together.  'entity_id' argument is required in this case to
        provide a distinguishing key for the result (falls back to "aggregated result").

        :param key: default: lambda x: x.fragment['StageName'] -- assuming the payload is a FragmentFlow
        :param entity_id: a descriptive string for the entity, to allow the aggregation to be distinguished in
         subsequent aggregations.  Use 'None' at your peril
        :return:
        """
        agg_result = LciaResult(self.quantity, scenario=self.scenario, private=self._private, scale=self._scale)
        if key == '*':
            if entity_id is None:
                entity_id = 'aggregated result'
            agg_result.add_summary(entity_id, entity_id, 1.0, self.total())
        else:
            for k, v in self._LciaScores.items():
                keystring = k
                try:
                    keystring = key(v.entity)
                finally:
                    # use keystring AS entity
                    agg_result.add_summary(keystring, keystring, 1.0, v.cumulative_result)
        return agg_result

    def show_agg(self, **kwargs):
        self.aggregate(**kwargs).show_components()  # deliberately don't return anything- or should return grouped?

    def flatten(self, _apply_scale=1.0):
        """
        Return a new LciaResult in which all groupings have been replaced by a set of AggregatedLciaScores, one
         per elementary flow.
        Performs some inline testing via equality assertions, but this still *really needs* unit testing, especially
        around matters of internal scale versus applied scale
        :param: _apply_scale: [1.0] apply a node weighting to the components
        :return:
        """
        flat = LciaResult(self.quantity, scenario=self.scenario, private=self._private, scale=1.0)
        recurse = []  # store flattened summary scores to handle later
        totals = defaultdict(list)
        for k, c in self._LciaScores.items():
            if isinstance(c, SummaryLciaResult):
                if c.static:
                    flat.add_summary(k, c.entity, c.node_weight * _apply_scale, c.unit_score)
                else:
                    recurse.append(c.flatten())
            else:
                for d in c.details():
                    totals[d.factor, d.exchange.direction, d.exchange.termination].append(d.exchange)

        for r in recurse:
            for k in r.keys():
                c = r[k]
                if isinstance(c, SummaryLciaResult):
                    # guaranteed to be static since r is a flattened LciaResult
                    if not c.static:
                        raise InconsistentSummaries(c)
                    try:
                        flat.add_summary(k, c.entity, c.node_weight * _apply_scale, c.unit_score)  # TODO: apply internal scale??
                    except InconsistentScores:
                        print('for key %s' % k)
                        raise
                else:
                    for d in c.details():
                        totals[d.factor, d.exchange.direction, d.exchange.termination].append(d.exchange)

        for k, l in totals.items():
            factor, dirn, term = k
            for x in l:
                name = '; '.join([x.flow.name, factor.context.name])
                if factor.context.name == 'None':
                    logging.warning('XXX None context found\n%s\n%s\n%s' % (x.flow.external_ref, x, factor))
                flat.add_component(name)
                exch = ExchangeValue(x.process, x.flow, dirn,
                                     value=x.value * _apply_scale * self.scale,
                                     termination=term)
                flat.add_score(name, exch, factor)

        scaled_total = self.total() * _apply_scale
        if not isclose(scaled_total, flat.total(), rel_tol=1e-10):  # this is in lieu of testing, obviously
            print(' LciaResult: %10.4g' % scaled_total)
            print('Flat result: %10.4g' % flat.total())
            print('Difference: %10.4g @ %10.4g' % (flat.total() - scaled_total, _apply_scale))
            if not isclose(scaled_total, flat.total(), rel_tol=1e-6):
                raise ValueError('Total differs by greater than 1e-6! (applied scaling=%10.4g)' % _apply_scale)
        return flat

    @property
    def is_private(self):
        return self._private

    def total(self):
        return sum([i.cumulative_result for i in self._LciaScores.values()])

    def a_total(self):
        """
        I don't want any logic in total()
        :return:
        """
        return self.total() * self.autorange

    @property
    def span(self):
        return sum([abs(i.cumulative_result) for i in self._LciaScores.values()])

    def range(self):
        _pos = _neg = 0.0
        for v in self._LciaScores.values():
            val = v.cumulative_result
            if val > 0:
                _pos += val
            elif val < 0:
                _neg += val
        return _neg, _pos

    def add_component(self, key, entity=None):
        self._check_type('component')
        if entity is None:
            entity = key
        if key not in self._LciaScores.keys():
            self._LciaScores[key] = AggregateLciaResult(self, entity)

    def add_score(self, key, exchange, qrresult):
        if qrresult.query != self.quantity:
            raise InconsistentQuantity('%s\nqrresult.quantity: %s\nself.quantity: %s' % (qrresult,
                                                                                         qrresult.query,
                                                                                         self.quantity))
        if key not in self._LciaScores.keys():
            self.add_component(key)
        self._LciaScores[key].add_detailed_result(exchange, qrresult)

    def add_summary(self, key, entity, node_weight, unit_score):
        self._check_type('summary')
        summary = SummaryLciaResult(self, entity, node_weight, unit_score)
        if key in self._LciaScores.keys():
            # raise DuplicateResult('Key %s is already present' % key)
            '''
            tgt = self._LciaScores[key]
            if isinstance(unit_score, LciaResult):
                uss = unit_score.total()
            else:
                uss = unit_score
            print('Key %s [%s] (%10.4g x %10.4g) adding %s (%10.4g x %10.4g)' % (key,
                                                                                 tgt.entity,
                                                                                 tgt.node_weight, tgt.unit_score,
                                                                                 entity,
                                                                                 node_weight, uss))
            '''
            try:
                self._LciaScores[key] += summary
            except TypeError:  # AggregateLciaResult doesn't know how to add-- summary takes over
                summary += self._LciaScores[key]
                self._LciaScores[key] = summary
            except InconsistentSummaries:
                self._failed.append(summary)
        else:
            self._LciaScores[key] = summary

    def add_missing(self, key, missing, node_weight):
        summary = SummaryLciaMissing(self, missing, node_weight, 0.0)
        if key in self._LciaScores.keys():
            self._LciaScores[key] += summary
        else:
            self._LciaScores[key] = summary

    @property
    def failed_summaries(self):
        """
        A list of Summary results that failed to be added to an existing summary. This is mainly diagnostic and should
        be removed soon.
        Note the difiference from self.errors(), which is meant to store input exchanges that could not be converted
        to the query quantity during LCIA.
        :return:
        """
        return self._failed

    def add_cutoff(self, exchange):
        self._cutoffs.append(exchange)

    def cutoffs(self):
        """
        Generates exchanges for which no factor was found during LCIA.
        :return:
        """
        for x in self._cutoffs:
            yield x

    def add_error(self, x, qr):
        self._errors.append(DetailedLciaResult(self, x, qr))

    def errors(self):
        """
        generates exchanges that could not be converted to the target quantity due to a conversion error.
        Note the difference from self.failed_summaries, which reports summary scores that could not be added.
        :return:
        """
        for x in self._errors:
            yield x

    def add_zero(self, x):
        self._zeros.append(x)

    def zeros(self):
        for x in self._zeros:
            yield x

    def details(self):
        for c in self.components():
            for d in c.details():
                yield d

    def keys(self):
        if self._private:
            for k in ():
                yield k
        else:
            for k in self._LciaScores.keys():
                yield k

    def components(self):
        if not self._private:
            for v in self._LciaScores.values():
                yield v

    def component_entities(self):
        if self._private:
            return [None]
        return [k.entity for k in self._LciaScores.values()]

    def _header(self):
        print('%s %s' % (self.quantity, self.unit))
        if self._autorange:
            self.set_autorange()  # update AutoRange object
            print('Auto-ranging: x %g' % self.autorange)
        print('-' * 60)
        if self._scale != 1.0:
            print('%10.4gx %s' % (self._scale, 'scale'))

    def show(self):
        self._header()
        print('%s' % self)

    def show_components(self, percent=False, count=100, threshold=None):
        """

        :param percent:
        :param count: [None] a maximum number of components to print
        :param threshold: [None] an absolute value below which scores are aggregated
        :return:
        """
        self._header()
        residual = 0.0
        resid_c = 0

        if not self._private:
            rev = bool(self.total() > 0)  # we need to reverse the reverse-sort if results are negative
            for v in sorted(self._LciaScores.values(), key=lambda x: x.cumulative_result, reverse=rev):
                if count is not None and count <= 0:
                    residual += v.cumulative_result
                    resid_c += 1
                    continue
                elif threshold is not None and abs(v.cumulative_result) < abs(threshold):
                    residual += v.cumulative_result
                    resid_c += 1
                    continue
                if percent:
                    pct = (v.cumulative_result / self.total()) * 100
                    pfx = '%5.2f %% ' % pct
                else:
                    pfx = ''
                print('%s%s' % (pfx, v))
                if count is not None:
                    count -= 1
            if residual != 0.0:
                if percent:
                    pct = (residual / self.total()) * 100
                    pfx = '%5.2f %% ' % pct
                else:
                    pfx = ''
                print('%s %s  remainder (%d items)' % (pfx, number(residual), resid_c))
            print('==========')
        if percent:
            print('%5.2f %% %s' % (100, self))
        else:
            print('%s' % self)

    def show_details(self, key=None, count=100, threshold=None):
        """
        Sorting by parts is not ideal but it will have to do.
        :param key:
        :param count:
        :param threshold:
        :return:
        """
        self._header()
        if not self._private:
            if key is None:
                rev = bool(self.total() > 0)  # we need to reverse the reverse-sort if results are negative
                for e in sorted(self._LciaScores.keys(),
                                key=lambda x: self._LciaScores[x].cumulative_result,
                                reverse=rev):
                    try:
                        print('\n%s:' % self._LciaScores[e].entity)
                    except TypeError:
                        print('\n%s:' % str(self._LciaScores[e].entity))
                    self._LciaScores[e].show_detailed_result(count=count, threshold=threshold)
            else:
                self._LciaScores[key].show_detailed_result(count=count, threshold=threshold)
        print('%s' % self)

    def __add__(self, other):
        if self.quantity != other.quantity:
            raise InconsistentQuantity
        if self.scenario != other.scenario:
            raise InconsistentScenario
        s = LciaResult(self.quantity, self.scenario)
        for k, v in self._LciaScores.items():
            s._LciaScores[k] = v
            v.update_parent(s)
        for k, v in other._LciaScores.items():
            if k in s._LciaScores:
                if v.entity is s._LciaScores[k].entity:
                    s._LciaScores[k] += v  # this is not implemented yet
                else:
                    s._LciaScores['_%s' % k] = v
            else:
                s._LciaScores[k] = v
        return s

    def __str__(self):
        err = ''
        if len(self._errors) > 0:
            err = '\n[%d Flow Conversion Errors]' % len(self._errors)
        return '%s %s%s' % (number(self.total()), self.quantity, err)

    def terminal_nodes(self, key=lambda x: x.link):
        aggs, scores = self._terminal_nodes()
        l = LciaResult(self.quantity, scenario=self.scenario)  #, private=self._private, scale=self._scale)  # not sure about these

        # PROPOSED: a way for fragment entities to appear like LciaSummary (node weight x score) - build summaries
        _sum_l = LciaResult(self.quantity, scenario=self.scenario)

        for ent, agg in aggs.items():
            if hasattr(ent, 'entity_type'):
                k = key(ent)
                if ent.entity_type == 'fragment':
                    if isinstance(agg, SummaryLciaResult):
                        l.add_summary(k, ent, scores[ent], agg.cumulative_result)
                    else:  # direct foreground emission: aggregate to the computation, then group with the parent
                        ''' here we have a quandary. if direct-emission fragments are attached to a parent with 
                        its own native score (i.e taps to a background process i.e. our use case), then we can
                        aggregate those direct emissions and then scale them to add with the native score by inspecting
                        its node weight.
                        but if the direct emissions are simply in the foreground, then the parent will have no native
                        summary and no accumulated node weight. so we are stuck with node weight = 1.0 for these
                        '''
                        _use = ent.reference_entity
                        if _use is None:
                            print(ent.link)
                            raise ZeroDivisionError('direct foreground emissions must have a parent')
                        _use_k = key(_use)
                        if _use in aggs:
                            # parent already known; defer our frags to add later
                            _sum_l.add_summary(_use_k, _use, 1.0, scores[ent] * agg.cumulative_result)
                        else:
                            # just add it straight away
                            l.add_summary(_use_k, _use, 1.0, scores[ent] * agg.cumulative_result)

                elif ent.entity_type == 'process':
                    l.add_summary(k, ent, scores[ent], agg.cumulative_result)
                else:
                    raise TypeError(ent)
            else:
                k = str(ent)
                l.add_summary(k, ent, scores[ent], agg.cumulative_result)

        # now add in deferred emissions, scaling by the parent's node weight to accumulate to the parent's unit score
        _sum_l.show_components()
        for summ_c in _sum_l.components():
            r_c = l[summ_c.entity]
            print('+++ %s' % r_c)
            l.add_summary(key(summ_c.entity), summ_c.entity, r_c.node_weight, summ_c.cumulative_result / r_c.node_weight)
            print('--- %s' % r_c)

        return l

    def _terminal_nodes(self, weight=1.0):
        """
        Recursive function to flatten out an LCIA result by node rather than flow
        returns two mappings: entity to accumulated node weight (aggs), entity to "scores" where "scores" is different
        for AggregateLciaScores and SummaryLciaResults:
         - aggregated scores return the score value, summary scores return accumulated node weights
        :param weight: the recursive node weight
        :return: mapping of node to component, mapping of node to accumulated weight (upstream weight already included)
        "node" can be either an entity (process or fragment) or a string
        """
        '''
        Further explanation:
         we are dis-aggregating our own internal score and re-aggregating it by terminal nodes.  aggs is a dictionary
         that maps the terminal node to the component, and scores maps the terminal node to the cumulative *weight* of 
         the terminal node.  Why we called it 'scores' is unclear.
         Procedure: we go through our components and for each:
          - If it is an AggregateLciaResult (i.e. a true LCIA computation), then it becomes a terminal node
          - If it is a static Summary, then we can't disaggregate and it also becomes a terminal node
          - If it is a dynamic summary, we recurse on it, and we take its terminal nodes and parse them out.
        For this to work, each terminal node must have the same unit score. 
        
        If we find non-matching scores for the same terminal node, we suffix the name and make a new component.
        This could happen if: two different fragments use the same process, but one is derived from an lci() and the 
        other is from a sys_lci() with a subtracted flow.  So naturally their scores are different.. No easy way to
        deal with that.... we just incrementally create new keys ("hunt for a distinct name...")
        '''
        aggs = dict()
        scores = defaultdict(float)
        for c in self.components():
            if isinstance(c, AggregateLciaResult):
                # base case
                if c.entity in aggs:
                    if aggs[c.entity].cumulative_result != c.cumulative_result:
                        raise KeyError(c)
                else:
                    aggs[c.entity] = c
                scores[c.entity] += weight
            else:  # Summary
                rec_weight = weight * c.node_weight
                if c.static:
                    if c.entity in aggs:
                        if aggs[c.entity].cumulative_result != c.cumulative_result:
                            raise KeyError(c)
                    else:
                        aggs[c.entity] = c
                    scores[c.entity] += rec_weight
                else:
                    rec_aggs, rec_scores = c._internal_result._terminal_nodes(weight=rec_weight)
                    for k, v in rec_aggs.items():
                        if v.cumulative_result == 0:
                            continue
                        if k in aggs:
                            if aggs[k].cumulative_result != v.cumulative_result:
                                # hunt for a distinct name to give a node with this score
                                ''' 
                                our current node matches an existing node in our mapping, but the scores don't match
                                we simply create a new node. but- we may have done that already- so we start with 'A'
                                and advance until we (a) find an existing key whose score matches or (b) find a new
                                key that hasn't been used yet
                                '''
                                idx = ord('A')
                                name = str(k)
                                while name in aggs:  # existing key
                                    if aggs[name].cumulative_result == v.cumulative_result:  # whose score matches
                                        break  # found one
                                    name = '.'.join([str(k), chr(idx)])
                                    idx += 1

                                    if idx > ord('Z'):  # we've run out of names- implausibly-
                                        raise InconsistentScores(c, k)  # something is wrong if we get to this point (?)

                                k = name
                                aggs[k] = v
                        else:
                            aggs[k] = v
                        scores[k] += rec_scores[k]
        return aggs, scores

    # charts
    def contrib_query(self, stages=None):
        """
        returns a list of scores
        :param stages: [None] a list of stages to query, or None to return all components.
         Specify '*' to return a 1-item list containing just the total.

        :return:
        """
        if stages == '*':
            return [self.total()]
        elif stages is None:
            stages = self.component_entities()

        data = []
        for c in stages:
            try:
                data.append(self._LciaScores[c].cumulative_result)
            except KeyError:
                data.append(0)
        if not isclose(sum(data), self.total(), rel_tol=1e-6):
            print('Contributions do not equal total [ratio: %.10f]' % (sum(data) / self.total()))
        return data

    def contrib_new(self, *args, autorange=None):
        """
        re-implement contrib query with a better spec.

        Queries are specified as entries from self.keys(). One way to get the keys to be more legible is to first
        perform an aggregation using self.aggregate().

        The current __getitem__ method, which uses a fuzzy match (self._match_keys()) is not currently used.

        :param args: A sequential list of components to query.  The special component '*' can be used to select the
        balance of results.
        :param autorange: [None] do not alter autorange settings.  [True / False]: activate or deactivate auto-ranging.
        :return: a 2-tuple: results, balance where results is a list having the same length as the number of arguments,
         and balance is a float reporting the remainder.  sum(results, balance) == self.total().  If '*' is specified as
         one of the queries, balance will always be 0.
        """
        if autorange is not None:
            self.set_autorange(autorange)
        elif self._autorange is not None:
            self.set_autorange()

        bal_idx = None
        results = []
        for i, query in enumerate(args):
            if query == '*':
                bal_idx = i  # save for later
                results.append(0.0)
            else:
                try:
                    results.append(self._LciaScores[query].cumulative_result * self.autorange)
                except KeyError:
                    results.append(0.0)

        balance = self.total() * self.autorange - sum(results)

        if bal_idx is not None:
            results[bal_idx] = balance
            return results, 0.0
        else:
            return results, balance

    def serialize_components(self, detailed=False):
        """
        If detailed is True, this should return DisaggregatedLciaScores
        If detailed is False, this should return SummaryLciaScores
        :param detailed:
        :return:
        """
        rev = bool(self.total() > 0)  # we need to reverse the reverse-sort if results are negative
        return [c.serialize(detailed=detailed) for c in sorted(self.components(), key=lambda x: x.cumulative_result,
                                                               reverse=rev)]
