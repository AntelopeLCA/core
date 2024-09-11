# from collections import defaultdict
import json
from antelope import (IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface, NoFactorsFound,
                      ItemNotFound)
from antelope import RxRef, EntityNotFound
from antelope.refs.base import NoUuid
from antelope.models import (OriginCount, Entity, FlowEntity, Exchange, ReferenceExchange, ReferenceValue,
                             UnallocatedExchange,
                             LciaResult as LciaResultModel, AllocatedExchange,
                             Characterization as CharacterizationModel,
                             ExchangeValues, DirectedFlow, FlowFactors)

from antelope_core.implementations import BasicImplementation, ConfigureImplementation
from antelope_core.lcia_results import LciaResult
from antelope_core.characterizations import QRResult
from .xdb_entities import XdbReferenceRequired


from requests.exceptions import HTTPError


class BadClientRequest(Exception):
    pass


class XdbConfigureImplementation(ConfigureImplementation):
    def apply_config(self, config, **kwargs):
        r_config = self._archive.r.get_raw('config')
        # no other config needs to be run for now
        config.update(r_config)
        super(XdbConfigureImplementation, self).apply_config(config, **kwargs)


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
    URL-ize input argument... add underscores as brackets if the ref contains slashes
    :param obj:
    :return:
    """
    if hasattr(obj, 'external_ref'):
        ref = str(obj.external_ref)
    else:
        ref = str(obj)
    if ref.find('/') >= 0:
        return '_/%s/_' % ref
    else:
        return ref


class XdbImplementation(BasicImplementation, IndexInterface, ExchangeInterface, QuantityInterface, BackgroundInterface):
    """
    The implementation is very thin, so pile everything into one class
    """
    def setup_bm(self, query):
        return True

    def get(self, external_ref, origin=None, **kwargs):
        return self._archive.retrieve_or_fetch_entity(external_ref, origin=origin, **kwargs)

    def get_reference(self, key):
        p = self.get(key)
        if p.entity_type == 'process':
            try:
                rs = p.ref.get(p.ref.reference_field)
                if rs is None:
                    raise XdbReferenceRequired
                return [RxRef(p, self._archive.get_or_make(r.flow), r.direction, comment=r.comment)
                        for r in rs]
            except XdbReferenceRequired:
                rs = self._archive.r.get_many(ReferenceValue, _ref(key), 'references')
                return [RxRef(p, self._archive.get_or_make(r.flow), r.direction, comment=r.comment, value=r.value)
                        for r in rs]
        elif p.entity_type == 'flow':
            ref_q = self._archive.r.get_one(Entity, _ref(key), 'reference')
            return self._archive.get_or_make(ref_q)
        elif p.entity_type == 'quantity':
            return self._archive.r.get_one(str, _ref(key), 'reference')
        else:
            raise TypeError(p.entity_type)

    def properties(self, external_ref, **kwargs):
        the_ref = self.get(external_ref, **kwargs)
        props = self._archive.r.get_one(dict, _ref(external_ref), 'properties')
        for k, v in props.items():
            the_ref[k] = v
            yield k

    def get_item(self, external_ref, item):
        try:
            return self._archive.r.get_raw(_ref(external_ref), 'doc', item)
        except HTTPError as e:
            if e.args[0] == 404:
                raise ItemNotFound(external_ref, item)
            raise e

    def get_uuid(self, external_ref):
        """
        Stopgap: don't support UUIDs
        :param external_ref:
        :return:
        """
        try:
            return self._archive.r.get_raw(_ref(external_ref), 'uuid')
        except HTTPError as e:
            if e.args[0] == 404:
                return NoUuid
            raise

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
        return [self._archive.get_or_make(k) for k in self._archive.r.get_many(Entity, 'processes', **llargs)]

    def flows(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [self._archive.get_or_make(k) for k in self._archive.r.get_many(FlowEntity, 'flows', **llargs)]

    def quantities(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [self._archive.get_or_make(k) for k in self._archive.r.get_many(Entity, 'quantities', **llargs)]

    def lcia(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [self._archive.get_or_make(k) for k in self._archive.r.get_many(Entity, 'lcia', **llargs)]

    def lcia_methods(self, **kwargs):
        llargs = {k.lower(): v for k, v in kwargs.items()}
        return [self._archive.get_or_make(k) for k in self._archive.r.get_many(Entity, 'lcia_methods', **llargs)]

    def contexts(self, **kwargs):
        return self._archive.tm.contexts(**kwargs)

    def get_context(self, term, **kwargs):
        if isinstance(term, list) or isinstance(term, tuple):
            return self._archive.tm.get_context(term[-1])
        return self._archive.tm.get_context(term)

    def targets(self, flow, direction=None, **kwargs):
        return [self._archive.get_or_make(k) for k in self._archive.r.get_many(Entity, _ref(flow), 'targets')]

    '''
    Exchange routes
    '''
    def _resolve_ex(self, ex):
        # self.get_canonical(ex.flow.quantity_ref)  # wtf was this for
        ex.process = self._archive.get(ex.process)
        ex.flow = self._archive.get_or_make(FlowEntity.from_exchange_model(ex))  # must get turned into a ref with make_ref

        if ex.type == 'context':
            ex.termination = self.get_context(ex.context)
        elif ex.type == 'cutoff':
            ex.termination = None
        return ex

    def _resolve_exv(self, exv: ExchangeValues):
        exv = self._resolve_ex(exv)
        if 'None' in exv.values:
            exv.values[None] = exv.values.pop('None')
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
        exvs = list(self._resolve_exv(exv) for exv in self._archive.r.get_many(RemoteExchangeValues, _ref(process),
                                                                               'exchanges', _ref(flow)))
        if direction:
            exvs = list(filter(lambda x: x.direction == direction, exvs))
        if reference:
            exvs = list(filter(lambda x: x.is_reference == reference, exvs))
        return exvs

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
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(UnallocatedExchange, _ref(node),
                                                                                'inventory'))

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None, **kwargs):
        if direction is None:
            if ref_flow is None:
                return self._archive.r.get_one(float, 'exchange_relation', _ref(process), _ref(exch_flow))
            else:
                return self._archive.r.get_one(float, 'exchange_relation', _ref(process), _ref(ref_flow),
                                               _ref(exch_flow))
        else:
            if ref_flow is None:
                return self._archive.r.get_one(float, 'exchange_relation', _ref(process), _ref(exch_flow), direction)
            else:
                return self._archive.r.get_one(float, 'exchange_relation', _ref(process), _ref(ref_flow),
                                               _ref(exch_flow), direction)

    def dependencies(self, process, ref_flow=None, **kwargs):
        if ref_flow:
            # process inventory
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                _ref(ref_flow), 'dependencies'))
        else:
            return list(self._resolve_ex(ex) for ex in self._archive.r.get_many(AllocatedExchange, _ref(process),
                                                                                'dependencies'))

    def consumers(self, process, ref_flow=None, **kwargs):
        """
        This returns reference exchanges for activities that consume the named reference exchange
        the puzzle here is how to generate the RxRefs-- I guess we should just do it here
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        if ref_flow:
            return list(self._resolve_ex(rx) for rx in self._archive.r.get_many(ReferenceExchange,
                                                                                _ref(process), _ref(ref_flow),
                                                                                'consumers'))
        else:
            return list(self._resolve_ex(rx) for rx in self._archive.r.get_many(ReferenceExchange,
                                                                                _ref(process),
                                                                                'consumers'))

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
        return list(facs)

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
            return self._archive.r.get_one(float, _ref(flow), 'cf', _ref(quantity), context=context, locale=locale,
                                           ref_quantity=ref_quantity)
        except HTTPError as e:
            if e.args[0] == 404:
                return 0.0
            else:
                raise

    def characterize(self, flowable, ref_quantity, query_quantity, value, context=None, location='GLO', **kwargs):
        if context:
            context = self.get_context(context).as_list()
        if not isinstance(value, dict):
            value = {location: value}
        cf = CharacterizationModel(origin=self.origin, flowable=flowable, ref_quantity=_ref(ref_quantity),
                                   query_quantity=_ref(query_quantity), context=context,
                                   value=value)
        return self._archive.r.post_return_one(cf.model_dump(), CharacterizationModel, 'characterize', **kwargs)

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, locale='GLO', **kwargs):
        """
        not yet implemented
        :param flowable:
        :param ref_quantity:
        :param query_quantity:
        :param context:
        :param locale:
        :param kwargs:
        :return:
        """
        print('quantity_relation not implemented! %s, %s, %s' % (flowable, _ref(ref_quantity), _ref(query_quantity)))
        raise NoFactorsFound

    @staticmethod
    def _result_from_exchanges(quantity, exch_map, res_m: LciaResultModel):
        """
        Constructs a detailed LCIA result using details provided by the backend server, populated with exchanges
        that we provided via POST.

        :param quantity:
        :param exch_map:
        :param res_m:
        :return:
        """
        res = LciaResult(quantity, scenario=res_m.scenario, scale=res_m.scale)
        for c in res_m.components:
            for d in c.details:
                key = (d.exchange.external_ref, tuple(d.exchange.context))
                try:
                    ex = exch_map[key]
                except KeyError:
                    print('missing key %s,%s' % key)
                    continue
                val = d.result / d.factor.value
                if val != ex.value:
                    print('%s: value mismatch %g vs %g' % (key, val, ex.value))
                cf = QRResult(d.factor.flowable, ex.flow.reference_entity, quantity, ex.termination,
                              d.factor.locale, d.factor.origin, d.factor.value)
                res.add_score(c.component, ex, cf)
        if len(res_m.summaries) > 0:
            print('Ignoring spurious summaries:')
            for s in res_m.summaries:
                print(s)
        # the results here shouldn't have any summaries
        # for s in res_m.summaries:
        #     res.add_summary(s.component, node, s.node_weight, s.unit_score)
        return res

    def get_factors(self, quantity, flow_specs, **kwargs):
    # def _result_from_model(self, process_ref, quantity, res_m: LciaResultModel):
        """

        :param quantity:
        :param flow_specs:
        :param kwargs:
        :return:
        """
        specs = [fs.dict() for fs in flow_specs]
        return self._archive.r.origin_post_return_many('qdb', specs, FlowFactors,
                                                       _ref(quantity), 'flow_specs')

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

        res = self._archive.r.origin_post_return_one('qdb', exchanges, LciaResultModel, _ref(quantity), 'do_lcia')
        return self._result_from_exchanges(quantity, exch_map, res)

    def bg_lcia(self, process, query_qty=None, ref_flow=None, **kwargs):
        """

        :param process:
        :param query_qty:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        try:
            if query_qty is None:
                if ref_flow:
                    ress = self._archive.r.get_many(LciaResultModel, _ref(process), _ref(ref_flow),
                                                    'lcia', **kwargs)
                else:
                    ress = self._archive.r.get_many(LciaResultModel, _ref(process),
                                                    'lcia', **kwargs)
            else:
                if ref_flow:
                    ress = self._archive.r.get_one(LciaResultModel, _ref(process), _ref(ref_flow),
                                                   'lcia', _ref(query_qty), **kwargs)
                else:
                    ress = self._archive.r.get_one(LciaResultModel, _ref(process),
                                                   'lcia', _ref(query_qty), **kwargs)
        except HTTPError as e:
            if e.args[0] == 404:
                content = json.loads(e.args[1])
                raise EntityNotFound(content['detail'])
            else:
                raise
        return ress

    def sys_lcia(self, process, query_qty, observed=None, ref_flow=None, **kwargs):
        """
        We want to override the interface implementation and send a simple request to the backend
        :param process:
        :param query_qty:
        :param observed: iterable of DirectedFlows
        :param ref_flow:
        :param kwargs: locale, quell_biogenic_co2
        :return:
        """
        obs_flows = ()
        try:
            if observed:
                obs_flows = [k.model_dump() for k in observed]
            if len(obs_flows) > 0:
                if ref_flow:
                    ress = self._archive.r.post_return_one(obs_flows, LciaResultModel, _ref(process), _ref(ref_flow),
                                                           'lcia', _ref(query_qty), **kwargs)
                else:
                    ress = self._archive.r.post_return_one(obs_flows, LciaResultModel, _ref(process),
                                                           'lcia', _ref(query_qty), **kwargs)
            else:
                if ref_flow:
                    ress = self._archive.r.get_one(LciaResultModel, _ref(process), _ref(ref_flow),
                                                   'lcia', _ref(query_qty), **kwargs)
                else:
                    ress = self._archive.r.get_one(LciaResultModel, _ref(process),
                                                   'lcia', _ref(query_qty), **kwargs)
        except HTTPError as e:
            if e.args[0] == 404:
                content = json.loads(e.args[1])
                raise EntityNotFound(content['detail'])
            else:
                raise
        return ress  # [self._result_from_model(process, query_qty, res) for res in ress]
