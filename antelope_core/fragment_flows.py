from antelope import comp_dir

from .entities.fragments import Fragment
from .entities.anchors import UnCachedScore, UnresolvedAnchor
from .lcia_results import LciaResult


class FragmentInventoryDeprecated(Exception):
    """
    The "inventory" term for fragments is deprecated.  "unit_inventory" is now "unit_flows" and "inventory"
    is what it has always *meant*: "cutoffs".  And the old "cutoffs" is gone because it literally never worked.
    """
    pass


class CumulatingFlows(Exception):
    """
    when a fragment includes multiple instances of the reference flow having consistent (i.e. not complementary)
    directions. Not handled in subfragment traversal bc no valid test case
    """
    pass


class FragmentFlow(object):
    """
    A FragmentFlow is a an immutable record of a traversal query. essentially an enhanced NodeCache record which
    can be easily serialized to an antelope fragmentflow record.

    A fragment traversal generates an array of FragmentFlow objects.

    X    "fragmentID": 8, - added by antelope
    X    "fragmentStageID": 80,

    f    "fragmentFlowID": 167,
    f    "name": "UO Local Collection",
    f    "shortName": "Scenario",
    f    "flowID": 371,
    f    "direction": "Output",
    f    "parentFragmentFlowID": 168,
    f    "isBackground": false,

    w    "nodeWeight": 1.0,

    t    "nodeType": "Process",
    t    "processID": 62,

    *    "isConserved": true,
    *    "flowPropertyMagnitudes": [
      {
        "flowPropertyID": 23,
        "unit": "kg",
        "magnitude": 1.0
      }
    ]

    """
    '''
    @classmethod
    def from_antelope_v1(cls, j, query):
        """
        Need to:
         * create a termination
         * create a fragment ref
         * extract node weight
         * extract magnitude
         * extract is_conserved
        :param j: JSON-formatted fragmentflow, from a v1 .NET antelope instance.  Must be modified to include StageName
         instead of fragmentStageID
        :param query: an antelope v1 catalog query
        :return:
        """
        return cls(frag, magnitude, nw, term, conserved)

    @classmethod
    def ref_flow(cls, parent, use_ev):
        """

        :param parent:
        :param use_ev: required to create reference flows from fragment refs
        :return:
        """
        fragment = GhostFragment(parent, parent.flow, comp_dir(parent.direction))
        term = FlowTermination.null(fragment)
        return cls(fragment, use_ev, 1.0, term,
                   parent.is_conserved_parent)
    '''
    @classmethod
    def from_process_inventory(cls, query, process, ref_flow, elementary=False, exterior=False):
        """
        expands the named process's inventory into a list of FragmentFlows amenable to
        LCIA computation.

        generate FragmentFlows of the process's intermediate exchanges.  the ensuing list should be suitable to feed
        directly into frag_flow_lcia() to generate a contribution analysis
        :param query: only requires .get(), so an implementation will work fine
        :param process:
        :param ref_flow:
        :param elementary: [False] if True, also generate FFs for elementary exchanges (these will otherwise get
        computed via unobserved_exchanges()
        :param exterior: [False] if True, also generate FFs for non-elementary exterior exchanges
        :return:
        """
        rx = process.reference(ref_flow)
        parent = Fragment(process.external_ref, rx.flow, comp_dir(rx.direction),
                          exchange_value=1.0, observe=True)
        node = parent.observe_anchor(None, process, anchor_flow=rx.flow)
        ff = [cls(parent, 1.0, 1.0, node, False)]
        for ex in process.ad(ref_flow):  # returns exchangevalues
            n = query.get(ex.termination)
            cf = Fragment(n.external_ref, ex.flow, ex.direction, exchange_value=ex.value, observe=True,
                          parent=parent, anchor=n, anchor_flow=ex.flow)

            ff.append(cls(cf, ex.value, ex.value, cf.anch, False))
        if exterior:
            for ex in process.bf(ref_flow=ref_flow):
                if ex.termination.elementary and not elementary:
                    continue
                # still include non-elementary cutoffs

                cf = Fragment(ex.flow.uuid, ex.flow, ex.direction, exchange_value=ex.value, observe=True,
                              parent=parent, anchor=ex.termination)
                ff.append(cls(cf, ex.value, ex.value, cf.anch, False))

        return ff

    def __init__(self, fragment, magnitude, node_weight, term, is_conserved, match_ev=None, match_term=None,
                 flow_conversion=1.0):
        """

        :param fragment:
        :param magnitude:
        :param node_weight: flow (or balance) magnitude * flow conversion / anchor inflow magnitude
        :param term:
        :param is_conserved:
        :param match_ev:
        :param match_term:
        :param flow_conversion: [1.0] stored in the FragmentFlow for information purposes only. Negative value
         indicates a direction change at the anchor (driven anchor)
        """
        # TODO: figure out how to cache + propagate scenario applications through aggregation ops
        self.fragment = fragment
        self.magnitude = magnitude
        self.node_weight = node_weight
        self.flow_conversion = flow_conversion
        self.term = term
        self.is_conserved = is_conserved
        self._subfrags_params = ()
        self.match_scenarios = (match_ev, match_term)

    @property
    def subfragments(self):
        if self.term.is_subfrag:  # and (self.term.descend is False):
            try:
                return self._subfrags_params[0]
            except IndexError:
                return []
        return []

    @property
    def subfragment_scenarios(self):
        try:
            return self._subfrags_params[1]
        except IndexError:
            return None

    def aggregate_subfragments(self, subfrags, scenarios=None):
        """
        We need to save the full traversal specification (
        :param subfrags:
        :param scenarios:
        :return:
        """
        self._subfrags_params = (subfrags, scenarios)

    def scale(self, x):
        self.node_weight *= x
        self.magnitude *= x

    @property
    def name(self):
        return self.term.name

    def screen_name(self, length=80):
        """
        auto-compact for display

        :return:
        """
        name = self.name
        if len(name) > length:
            name = name[:(length - 18)] + '....' + name[-14:]
        return name

    def __str__(self):
        return '%.5s  %10.3g [%6s] %s %s' % (self.fragment.uuid, self.magnitude, self.fragment.direction,
                                             self.term, self.name)

    def __add__(self, other):
        if isinstance(other, FragmentFlow):
            if other.fragment.uuid != self.fragment.uuid:
                raise ValueError('Fragment flows do not belong to the same fragment')
            mag = other.magnitude
            nw = other.node_weight
            if not self.term == other.term:
                raise ValueError('These fragment flows are differently terminated')

            if mag * self.node_weight != (self.magnitude * nw):  # formally if m*N/M*n != 1.0:
                raise ValueError('These fragment flows cannot be combined because their implicit evs do not match')
            conserved = self.is_conserved and other.is_conserved

            mod_mag = mag * other.flow_conversion / self.flow_conversion
        else:
            raise TypeError("Don't know how to add type %s to FragmentFlow\n %s\n to %s" % (type(other), other, self))
        # don't check unit scores-- ?????
        new = FragmentFlow(self.fragment, self.magnitude + mod_mag, self.node_weight + nw,
                           self.term, conserved, flow_conversion=self.flow_conversion)
        return new

    def __eq__(self, other):
        """
        FragmentFlows are equal if they have the same fragment and termination.  Formerly magnitude too but why?
        answer why: because if not, then two traversals from two different scenarios can appear equal
        answer why not: because of LciaResult issues, presumably- let's put this on the list for later
        :param other:
        :return:
        """
        if not isinstance(other, FragmentFlow):
            return False
        return self.fragment == other.fragment and self.term == other.term  # and self.node_weight == other.node_weight

    def __hash__(self):
        return hash(self.fragment)

    @property
    def ref_unit(self):
        return self.fragment.flow.unit


def frag_flow_lcia(fragmentflows, quantity_ref, scenario=None, **kwargs):
    """
    Recursive function to compute LCIA of a traversal record contained in a set of Fragment Flows.
    Note: refresh is no longer supported during traversal
    :param fragmentflows:
    :param quantity_ref:
    :param scenario: necessary if any remote traversals are required
    :param ignore_uncached: [True] whether to allow zero scores for un-cached, un-computable fragments
    :return:
    """
    result = LciaResult(quantity_ref, scenario=str(scenario))
    _first_ff = True
    for ff in fragmentflows:
        _recursive_remote = False
        if ff.term.is_null:
            continue

        node_weight = ff.node_weight
        if node_weight == 0:
            continue

        if ff.term.direction == ff.fragment.direction:
            # if the directions collide (rather than complement), the term is getting run in reverse
            node_weight *= -1

        # if we have subfragments, use them
        if len(ff.subfragments) == 0:  # always true for: contexts, processes, remote subfragments
            try:
                v = ff.term.score_cache(quantity=quantity_ref, **kwargs)

                # if we reach here, then we have successfully retrieved a cached unit score and we are done
                if not v.is_null:
                    result.add_summary(ff.fragment.uuid, ff, node_weight, v)

                _first_ff = False
                continue

            except UnresolvedAnchor:
                result.add_missing(ff.fragment.uuid, ff.term.term_node, node_weight)
                _first_ff = False
                continue

            except UnCachedScore:
                # a subfragment with no stored subfragments and no cached score: we gotta ask
                v = ff.term.term_node.fragment_lcia(quantity_ref, scenario=scenario)
                _recursive_remote = True

        else:
            v = frag_flow_lcia(ff.subfragments, quantity_ref, scenario=ff.subfragment_scenarios, **kwargs)
            if v.is_null:
                continue

        # if we arrive here, we have a unit score from a subfragment

        if ff.term.descend:
            if v.has_summaries:
                for k in v.keys():
                    c = v[k]
                    result.add_summary(k, c.entity, c.node_weight * node_weight, c.internal_result)
            else:
                result.add_summary(ff.fragment.uuid, ff, node_weight, v)
        else:
            result.add_summary(ff.fragment.uuid, ff, node_weight, v)

        if _first_ff and _recursive_remote:
            return result  # bail out -- not sure why though- we need to be able to continue w/ child flows
        _first_ff = False
    return result
