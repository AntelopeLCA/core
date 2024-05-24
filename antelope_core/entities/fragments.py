import logging
import uuid

from .entities import LcEntity
from .anchors import Anchor

from antelope import comp_dir, check_direction, PropertyExists, RxRef


class InvalidParentChild(Exception):
    pass


class BalanceAlreadySet(Exception):
    pass


class CacheAlreadySet(Exception):
    pass


class ScenarioConflict(Exception):
    pass


class Fragment(LcEntity):
    """
    What can I say? THey're complicated.
    Conceptually, a fragment is a modeler's tool for keeping track of measurements of a specific material flow.

    A fragment's main purpose is to express a linkage between a parent node and a target or 'anchor' node by means
    of an observed flow.

    Fragments have the following broad functionalities:

     - they can be named. external_ref can be set. not clear why this is only for fragments, but it is meant to
     provide h-r semantic consistency / clarity during data use
     - they can be observed. a fragment has a cached and an observed exchange value. the cached value is meant to
     indicate its value upon creation, and observed is the default value returned upon quantitative queries.
     observations take the form of a scenario specification and an exchange value (along with metadata such as dqi,
     uncertainty, etc)
     - they can be anchored, to data sets or to other models.  anchor points are also specified by scenario. anchors
     can be toggled to "descend" (i.e. expand sub-fragments) or non-descend / roll-up sub-fragments
     - they have child flows. "reference flows" have no parent and supply/consume a good or service through exchange
     with a second party.  Child flows are then dependent on the reference flow. By traversing these links to their
     ends, the entire "span" of the study can be enumerated. It is the job of the traversal engine to ensure that
     traversal terminates upon reaching a background node (i.e. one with membership in a cycle) to a flat LCI.
     - they can balance. Each fragment node (the thing that is 'anchored') can compute a balance during traversal,
     assigning a designated child flow the magnitude of the balance at traversal time.  The quantity that is conserved
     is precisely the designated balance flow's reference quantity.  all flows are queried for characterization
     in computing the balance.
    * STILL TBD: how to handle "balancing" into and out of anchored subfragments
    """
    @classmethod
    def new(cls, flow, direction, **kwargs):
        """
        :param flow: entity or ref
        :param direction: "Input" or "Output" w/r/t parent (future: "balance" to be also accepted)
        :param kwargs: parent; exchange_value/observe; anchor/anchor_flow/descend; balance_flow (alt.), external_ref
        :return:
        """
        return cls(uuid.uuid4(), flow, direction, **kwargs)

    _ref_field = 'parent'

    _parent = None
    _flow = None
    _direction = None

    _new_fields = ['StageName']

    def __init__(self, the_uuid, flow, direction, parent=None, exchange_value=None, units=None, stage_name=None,
                 anchor=None, anchor_flow=None, descend=True,
                 balance_flow=False, external_ref=None, observe=True, **kwargs):

        self._flow = flow
        self._direction = check_direction(direction)
        self._evs = {1: 0.0}
        self._anchors = dict()
        self._child_flows = list()

        super(Fragment, self).__init__('fragment', external_ref, entity_uuid=the_uuid, **kwargs)
        if self._external_ref == self._uuid:
            self._external_ref = None  # reset this
        self['StageName'] = stage_name or ''

        if self.uuid is None:
            self.uuid = str(uuid.uuid4())

        if parent:
            self.set_parent(parent)

        self._balance = False
        self._balance_child = None

        if bool(balance_flow):
            if parent is None:
                logging.warning('Ignoring balance spec for reference flow')
            else:
                self.set_balance_flow()

        if self._balance:
            self._cached_ev = None
            if observe:
                logging.warning('Ignoring observe specification for balance flow')
        else:
            value = exchange_value or 1.0
            if units is not None and len(units) > 0:
                value *= self.conversion(units)

            self._cached_ev = value
            if observe:
                self._evs[1] = self._cached_ev

        if anchor:
            self._anchors[None] = Anchor(self, anchor, anchor_flow=anchor_flow, descend=descend)
        else:
            self._anchors[None] = Anchor.null(self)

    def clear_evs(self, observed=False):
        self._cached_ev = 1.0
        self._evs = {1: int(bool(observed))}

    def __hash__(self):
        """
        Fragments must use UUID as hash because the link is mutable
        :return:
        """
        if self._origin is None:
            raise AttributeError('Origin not set!')
        return hash('%s/%s' % (self.origin, self.uuid))

    @property
    def cached_ev(self):
        return self._cached_ev

    @cached_ev.setter
    def cached_ev(self, value):
        if self.cached_ev != 1.0:
            raise CacheAlreadySet('Set Value: %g (new: %g)' % (self.cached_ev, value))
        if value == self.cached_ev:
            return
        self._cached_ev = value

    @property
    def observed_ev(self):
        return self._evs[1]

    @property
    def external_ref(self):
        if self._external_ref is None:
            return self._uuid
        return self._external_ref

    @external_ref.setter
    def external_ref(self, ref):
        """
        Specify how the entity is referred to in the source dataset. If this is unset, the UUID is assumed
        to be used externally.
        :param ref:
        :return:
        """
        if self._external_ref is None:
            if ref != self.uuid:  # don't bother setting if it's the same as the UUID
                self._external_ref = ref
        else:
            raise PropertyExists('External Ref already set to %s' % self._external_ref)

    def de_name(self):
        """
        Remove a fragment's name
        :return:
        """
        self._external_ref = None

    def reference(self, flow=None):
        """
        For process interoperability
        :return:
        """
        rx = RxRef(self, self.flow, comp_dir(self.direction), self.get('Comment', None), value=self.observed_ev)
        if flow is not None:
            if not rx.flow.match(flow):
                raise ValueError('%.5s: Supplied flow %s does not match fragment' % (self.uuid, flow))
        return rx

    def make_ref(self, query):
        """
        We do NOT want to make local fragments into refs-- but we DO want to make remote fragments into refs. so
        we simply neuter the workhorse function here.
        :param query:
        :return:
        """
        return self

    @property
    def flow(self):
        return self._flow

    @property
    def direction(self):
        return self._direction

    @property
    def anch(self):
        return self._anchors[None]

    '''
    Parents and Child Flows
    '''
    def top(self):
        if self.reference_entity is None:
            return self
        return self.reference_entity.top()

    def set_parent(self, parent):
        if self.reference_entity is not None:
            self.unset_parent()
        if self.origin != parent.origin:
            if self.origin is None:
                self.origin = parent.origin
            else:
                raise AttributeError('Origin mismatch: parent (%s) vs child (%s)' % (parent.origin, self.origin))
        self._set_reference(parent)
        parent.add_child(self)

    def unset_parent(self):
        if self.is_balance:
            self.unset_balance_flow()
        self.reference_entity.remove_child(self)
        self._set_reference(None)

    def add_child(self, child):
        """
        This should only be called from the child's set_parent function
        :param child:
        :return:
        """
        # if child.reference_entity is not self:
        #     raise InvalidParentChild('Fragment should list parent as reference entity')
        if child not in self._child_flows:
            self._child_flows.append(child)
        if self.anch.is_null:  # formerly to_foreground()
            self.observe_anchor(None, self)
        for term in self._anchors.values():
            term.clear_score_cache()

    def remove_child(self, child):
        """
        This should only be called from the child's unset_parent function
        :param child:
        :return:
        """
        if child.reference_entity is not self:
            raise InvalidParentChild('Fragment is not a child')
        self._child_flows.remove(child)
        if len(self._child_flows) == 0:
            if self.anch.node is self:
                self.clear_anchor()
        for term in self._anchors.values():
            term.clear_score_cache()

    @property
    def child_flows(self):
        for k in self._child_flows:  # sorted(self._child_flows, key=lambda x: x.uuid):
            yield k

    def children_with_flow(self, flow, direction=None, anchor=None, recurse=False):
        for k in self._child_flows:
            if k.flow == flow:
                if direction is not None:
                    if k.direction != direction:
                        continue
                if anchor is not None:
                    if k.anch.node != anchor:
                        continue
                yield k
            if recurse:  # depth-first
                for z in k.children_with_flow(flow, direction, recurse=recurse):
                    yield z

    @property
    def parent(self):
        return self.reference_entity

    @property
    def is_reference(self):
        return self.reference_entity is None

    @property
    def is_background(self):
        return len(self._child_flows) == 0

    @property
    def name(self):
        if self._external_ref is None:
            return self['Name']
        return self.external_ref

    @property
    def dirn(self):
        return {
            'Input': '-<-',
            'Output': '=>='
        }[self.direction]

    @property
    def unit(self):
        """
        used for formatting the fragment in display
        :return:
        """
        if self.reference_entity is None:
            return '%4g %s' % (self.cached_ev, self.flow.unit)
        return self.anch.unit

    def __str__(self):
        if self.reference_entity is None:
            if len(self._child_flows) == 0:
                re = '(B) ref'
            else:
                re = ' ** ref'
        else:
            re = self.reference_entity.uuid[:7]
        if self.external_ref == self.uuid:
            extname = ''
        else:
            extname = '{%s}' % self.external_ref

        return '(%s) %s %.5s %s %s  [%s] %s %s' % (re, self.dirn, self.uuid, self.dirn, self.anch,
                                                   self.unit, self['Name'], extname)

    def show(self):
        print('%s' % self)
        super(Fragment, self).show()
        evs = list(self._evs.keys())
        evs.remove(1)
        print('Exchange values: ')
        print('%20.20s: %g' % ('Cached', self.cached_ev))
        print('%20.20s: %g' % ('Observed', self.observed_ev))
        for k in evs:
            print('%20.20s: %g' % (k, self._evs[k]))
        if self.is_balance:
            print('\nBalance flow: True (%s)' % self.flow.reference_entity)
        else:
            print('\nBalance flow: False')
        print('Terminations: ')
        print('%20s  %s' % ('Scenario', 'Termination'))
        for k, v in self._anchors.items():
            if v.node is self:
                print('%20.20s: %s Foreground' % (k, v))
            else:
                if v.descend:
                    desc = '     '
                else:
                    desc = '(agg)'
                print('%20.20s: %s %s %s' % (k, v, desc, v.node))

    def scale_evs(self, factor):
        """
        needed when foregrounding terminations
        :param factor:
        :return:
        """
        for k, v in self._evs.items():
            self._evs[k] = v * factor
        self._cached_ev *= factor

    def observable(self, scenario=None):
        return self._check_observability(scenario=scenario)

    def _check_observability(self, scenario=None):
        if self.reference_entity is None:
            return True
        elif self.is_balance:
            # print('observability: value set by balance.')
            return False
        elif self.reference_entity.anchor(scenario).is_subfrag:
            # print('observability: value set during traversal')
            return False
        else:
            return True

    def conversion(self, units):
        try:
            return self.flow.reference_entity.convert(units)
        except KeyError:
            logging.warning('Flow conversion error: %5.5s: %s (%s)' % (self.uuid, self.flow.reference_entity,
                                                                       units))
            return 0.0

    def observe(self, scenario=None, value=None, units=None):
        if scenario is None:
            scenario = 1
        if isinstance(scenario, tuple) or isinstance(scenario, set):
            raise ScenarioConflict('Set EV must specify single scenario')
        if value is None:
            self._evs[scenario] = self._cached_ev
        else:
            value = float(value)

            if units is not None and len(units) > 0:
                value *= self.conversion(units)
            self._evs[scenario] = value

    def observe_anchor(self, scenario, anchor_node, anchor_flow=None, descend=None):
        if scenario in ('cached', 'observed'):
            raise ValueError('scenario cannot use reserved name: %s' % scenario)

        if isinstance(scenario, tuple) or isinstance(scenario, set):
            raise ScenarioConflict('Set termination must specify single scenario')
        if scenario is not None and scenario in self._anchors:
            if not self._anchors[scenario].is_null:
                raise CacheAlreadySet('Scenario termination already set. use clear_termination()')

        anchor = Anchor(self, anchor_node, anchor_flow, descend)

        if scenario is None:
            """
            use default anchor to set stagename
            """
            if self['StageName'] == '' and not anchor.is_null:
                if anchor.is_frag:
                    self['StageName'] = anchor.node['StageName']
                elif anchor.is_context:
                    self['StageName'] = anchor.node.name
                else:
                    try:
                        self['StageName'] = anchor.node['Name']
                    except (KeyError, TypeError, IndexError):
                        print('%.5s StageName failed %s' % (self.uuid, anchor.node))
                        self['StageName'] = anchor.node.name

        self._anchors[scenario] = anchor

        return anchor

    def observe_lcia_score(self, scenario, quantity, score):
        try:
            anch = self._anchors[scenario]
        except KeyError:
            anch = self.observe_anchor(scenario, self)
        anch.add_lcia_score(quantity, score, scenario=scenario)

    def _match_scenario_ev(self, scenario):
        if scenario is None:
            return None
        _match = []
        for sc in scenario:
            try:
                m = self._evs[sc]
            except KeyError:
                continue
            _match.append(m)
        if len(_match) > 1:
            raise ScenarioConflict('Fragment %s: Multiple scenario results: %s' % (self.uuid, _match))
        m = _match[0]
        if str(m).startswith('norm') and self.parent is None:
            print('Applying and removing one-time normalization scenario %s' % m)
            # self.dbg_print('Applying and removing one-time normalization scenario %s' % m, level=0)
            scenario.remove(m)
            return m
        if scenario in self._evs:
            return scenario
        return 1

    def _match_scenario_anchor(self, scenario):
        if scenario is None:
            return None
        if isinstance(scenario, set):
            match = [scen for scen in filter(None, scenario) if scen in self._anchors.keys()]
            if len(match) == 0:
                return None
            elif len(match) > 1:
                raise ScenarioConflict('fragment: %s\ntermination matches: %s' % (self.uuid, match))
            return match[0]
        if scenario in self._anchors.keys():
            return scenario
        return None

    def anchor(self, scenario):
        s = self._match_scenario_anchor(scenario)
        return self._anchors[s]

    '''
    Balance Flow Handling
    '''
    @property
    def is_balance(self):
        return bool(self._balance)

    @property
    def balance_flow(self):
        return self._balance_child

    def set_balance_flow(self):
        """
        A balance flow balances its own reference quantity.
        :return:
        """
        if self.reference_entity is None:
            raise InvalidParentChild('Reference flow cannot be a balance flow')
        if self.is_balance is False:
            self.reference_entity.set_conservation_child(self)
            self._balance = True

    def unset_balance_flow(self):
        if self.is_balance:
            self.reference_entity.unset_conservation_child()
            self._balance = False

    def set_conservation_child(self, child):
        if child.reference_entity != self:
            raise InvalidParentChild
        if self.is_conserved_parent and (child is not self._balance_child):
            print('%.5s conserving %s\nversus %s' % (self.uuid, self._balance_child, child))
            raise BalanceAlreadySet
        self._balance_child = child
        # if self.balance_magnitude == 0:
        #     logging.warning('%.5s Notice: zero balance for conserved quantity %s' % (self.uuid,
        #                                                                              self.conserved_quantity))
        logging.info('setting balance from %.5s: %s' % (child.uuid, self._balance_child))

    @property
    def is_conserved_parent(self):
        return self._balance_child is not None

    def unset_conservation_child(self):
        self._balance_child = None

    @property
    def conserved_quantity(self):
        if self._balance_child is None:
            return None
        else:
            return self._balance_child.flow.reference_entity

    def clear_anchor(self, scenario=None):
        self._anchors[scenario] = Anchor.null(self)

    def nodes(self, scenario=None, descend=True):
        """
        Report proximal terminal nodes for the fragment (recurse until a nondescend is reached)
        :param scenario: [None]
        :param descend: [True] if False, yield subfragments as nodes
        :return: generator of terminal nodes
        """
        term = self.anchor(scenario)
        yds = set()
        if term.is_process or term.is_context or term.is_unresolved:
            if term.node not in yds:
                yield term.node
                yds.add(term.node)
        elif term.is_subfrag:
            if term.descend and descend:
                for n in term.node.nodes(scenario, descend=descend):
                    if n not in yds:
                        yield n
                        yds.add(n)
            else:
                yield term.node
            # foreground, null: do nothing
        for c in self.child_flows:
            for n in c.nodes(scenario, descend=descend):
                if n not in yds:
                    yield n
                    yds.add(n)

    def tree(self):
        """
        This is real simple- just a recursive enumeration of child flows, depth first

        :return:
        """
        yield self
        for c in sorted(self.child_flows, key=lambda x: (x['StageName'], not x.anch.is_null, x.is_background)):
            for b in c.tree():
                yield b

    def exchanges(self, scenario=None):
        """
        Generator for query compatibility with processes
        :param scenario:
        :return:
        """
        for x in self.child_flows:
            yield x
        for x in self.anchor(scenario).unobserved_exchanges():
            yield x

    def ev(self, scenarios=None):
        """

        :param scenarios: either None, or an iterable of scenarios
        :return: matching scenario, matching ev
        """
        if scenarios is None:
            return self.cached_ev
        else:
            m = self._match_scenario_ev(scenarios)
            if m is None:
                return self.observed_ev
            return self._evs[m]
