from ..lcia_results import LciaResult
from ..exchanges import ExchangeValue
from ..contexts import NullContext
from antelope import QuantityRequired, ConversionReferenceMismatch, BackgroundRequired, EntityNotFound, comp_dir


UNRESOLVED_ANCHOR_TYPE = 'term'  # this is used when an anchor node's origin cannot be resolved


class FlowConversionError(Exception):
    pass


class UnresolvedAnchor(Exception):
    pass


class UnCachedScore(Exception):
    """
    means that we have an LCIA-only node whose score has not been set for the requested LCIA method
    """
    pass


class Anchor(object):
    """
    An anchor is the distal partner to an exchange.
    """
    @classmethod
    def null(cls, parent):
        return cls(parent, None)

    _node = None
    _anchor_flow = None

    def __init__(self, parent, anchor_node, anchor_flow=None, descend=None):
        """
        An anchor can be one of the following five types:
         - null: a cut-off. The fragment is an input/output of the spanner
         - foreground: the fragment is its own anchor.
         - process: the fragment is anchored to a process with inventory
         - sub-fragment: the fragment is anchored to another spanner
         - context: the fragment is an exchange with a context

        :param parent:
        :param anchor_node:
        :param anchor_flow:
        :param descend:
        """
        self._parent = parent
        self._node = anchor_node
        self.anchor_flow = anchor_flow
        self._descend = descend
        self._score_cache = dict()

    @property
    def is_null(self):
        return self._node is None

    @property
    def is_fg(self):
        return (not self.is_null) and (self.node is self._parent)

    @property
    def is_process(self):
        return (not self.is_null) and (self.node.entity_type == 'process')

    @property
    def is_frag(self):
        """
        Termination is a fragment
        :return:
        """
        return (not self.is_null) and (self.node.entity_type == 'fragment')

    @property
    def is_subfrag(self):
        """
        Termination is a non-self fragment.

        :return:
        """
        return self.is_frag and not self.is_fg

    @property
    def is_context(self):
        """
        termination is a context
        :return:
        """
        return (not self.is_null) and (self._node.entity_type == 'context')

    @property
    def is_emission(self):
        """
        Pending context refactor
        :return:
        """
        return self.is_context and self.node.elementary

    @property
    def node(self):
        return self._node

    @property
    def name(self):
        if self.is_null:
            name = self.anchor_flow['Name']
        elif self.is_context:
            name = '%s, %s' % (self.anchor_flow['Name'], self.node.name)
        else:
            name = self.node.name
        return name

    @property
    def unit(self):
        return self.anchor_flow.unit

    def __str__(self):
        """
        This is repeated at least once in the Anchor model
        :return:
          '---:' = fragment I/O
          '-O  ' = foreground node
          '-*  ' = process
          '-#  ' - sub-fragment (aggregate)
          '-#::' - sub-fragment (descend)
          '-B ' - terminated background
          '--C ' - cut-off background
          '--? ' - ungrounded catalog ref
        """
        if self.is_null:
            term = '---:'  # fragment IO
        elif self.is_fg:
            term = '-O  '
        elif self.is_context:
            if self.is_emission:
                term = '-== '
            elif self.node is NullContext:
                term = '-)  '
            else:
                # TODO: intermediate contexts don't present as cutoffs (because is_null is False)
                term = '-cx '
        elif self.node.entity_type == 'process':
            if self._parent.is_background:
                term = '-B* '
            else:
                term = '-*  '
        elif self.node.entity_type == 'fragment':
            if self.descend:
                term = '-#::'
            else:
                term = '-#  '
        elif self.node.entity_type == UNRESOLVED_ANCHOR_TYPE:
            term = '--? '
        else:
            raise TypeError('I Do not understand this term for frag %.7s' % self._parent.uuid)
        return term

    @property
    def flow_conversion(self):
        """
        express the parent's flow in terms of the quantity of the term flow.
        There are two ways to do this, each case involving the quantity relation on either the parent flow or the
        term flow, between the two quantities (parent flow's r.q. is the reference quantity; term flow's r.q. is the
        query quantity).

        In each case, we want the flow's native term manager to perform the conversion using ITS OWN canonical
        quantities.  The assumption is that the parent flow's r.q. is tied to our local LciaEngine, while the
        term flow's r.q. could be either local or remote.

        The QuantityRef.quantity_relation() implicitly assumes that the invoking quantity is the QUERY quantity, so
        the "forward" (natural parent->node) direction uses the remote flow's r.q. - but we do the "reverse" direction
        first because it's local.

        how to deal with scenario cfs? tbd
        problem is, the term doesn't know its own scenario

        :return: float = amount in term_flow ref qty that corresponds to a unit of fragment flow's ref qty
        """
        '''
        What are all the possibilities?
         the parent quantity knows a characterization for the parent flow w.r.t. the term quantity
         the parent quantity knows a characterization for the term flow w.r.t. the term quantity
         the term quantity knows a characterization for the parent flow w.r.t. the parent quantity
         the term quantity knows a characterization for the term flow w.r.t. the parent quantity

        "hey look at me, net calorific value. see that mass flow f4- a unit of it is worth 35 of me"
        is how the database is constructed.

        but each call to quantity_relation should check for both forward and reverse matches
        '''
        if self.anchor_flow.reference_entity == self._parent.flow.reference_entity:
            return 1.0
        parent_q = self._parent.flow.reference_entity
        term_q = self.anchor_flow.reference_entity
        if term_q is None:
            return 0.0

        # first - natural - ask our parent flow if fit can convert to term quantity
        try:
            rev = self._parent.flow.cf(term_q, context=self.node.external_ref)
        except (QuantityRequired, NotImplementedError, ConversionReferenceMismatch):
            rev = None

        if rev:
            return rev

        # then ask the term_flow if it can convert to parent quantity
        try:
            fwd = self.anchor_flow.cf(parent_q)
        except (QuantityRequired, NotImplementedError, ConversionReferenceMismatch):
            fwd = None

        if fwd:
            return 1.0 / fwd

        # then ask if our parent qty can convert term_flow
        try:
            rev_c = parent_q.quantity_relation(self.anchor_flow, term_q, context=(self.node.origin,
                                                                                  self.node.external_ref))
        except (QuantityRequired, NotImplementedError):
            rev_c = None
        except ConversionReferenceMismatch:
            try:
                rev_c = parent_q.quantity_relation(self.anchor_flow, term_q, context=None)
            except ConversionReferenceMismatch:
                rev_c = None

        if rev_c:
            return 1.0 / rev_c.value

        # last, ask if remote quantity recognizes *our* flow
        print(' %s: flow_conversion: untested' % self._parent.link)
        try:
            fwd_c = term_q.quantity_relation(self._parent.flow, parent_q, context=(self.node.origin,
                                                                                   self.node.external_ref))
        except (QuantityRequired, NotImplementedError, ConversionReferenceMismatch):
            fwd_c = None

        if fwd_c:
            print('reverse q-hit')
            return fwd_c.value
        print(' %s: flow_conversion FAILED' % self._parent.link)
        raise FlowConversionError('Zero CF found relating %s to %s' % (self.anchor_flow, self._parent.flow))

    @property
    def inbound_exchange_value(self):
        if self.is_process:
            if self._anchor_flow.direction == self._parent.direction:
                return -1.0
            return 1.0

    @property
    def node_weight_multiplier(self):
        return self.flow_conversion * self.inbound_exchange_value

    @property
    def anchor_flow(self):
        if self.is_process:
            return self._anchor_flow.flow
        else:
            return self._anchor_flow

    @anchor_flow.setter
    def anchor_flow(self, flow):
        if self.is_process:
            self._anchor_flow = self.node.reference(flow)
        elif flow is None:
            if self.is_frag:
                self._anchor_flow = self.node.flow
            else:
                self._anchor_flow = self._parent.flow
        else:
            self._anchor_flow = flow

    @property
    def direction(self):
        if self.is_process:
            return self._anchor_flow.direction
        return comp_dir(self._parent.direction)

    @property
    def descend(self):
        if self.is_null:
            return None
        return bool(self._descend)

    @descend.setter
    def descend(self, value):
        if self.is_null:
            raise TypeError('may not set descend on a null anchor')
        else:
            self._descend = value

    def add_lcia_score(self, quantity, score, scenario=None):
        res = LciaResult(quantity, scenario=scenario)
        res.add_summary(self._parent.external_ref, self._parent, 1.0, score)
        self._score_cache[quantity] = res

    def unobserved_exchanges(self):
        """
        Generator which yields exchanges from the term node's inventory that are not found among the child flows, for
          LCIA purposes

        Challenge here going forward: we made some kind of normative decision early on that terminations do not know
        their own scenarios, that the fragment maps scenario to termination. The problem is that now terminations
        cannot themselves carry out traversal on the term_node because they don't know what scenarios to pass.

        The upshot is that we cannot meaningfully compute "unobserved exchanges" for subfragments, since we don't
        know our scenario.

        :return:
        """
        if self.is_context:
            x = ExchangeValue(self._parent, self.anchor_flow, self._parent.direction, termination=self.node,
                              value=self.node_weight_multiplier)  # TODO: need to figure out how we are handling locales
            yield x
        elif self.is_frag:  # we need to send the fragment's cutoffs-- but that cannot occur here
            raise NotImplementedError
        else:
            if self._parent.is_background:  # or len(list(self._parent.child_flows)) == 0:
                # ok we're bringing it back but only because it is efficient to cache lci
                for x in self.node.lci(ref_flow=self.anchor_flow):
                    yield x
            else:
                for x in self.node.unobserved_lci(self._parent.child_flows, ref_flow=self.anchor_flow):
                    yield x  # this should forward out any cutoff exchanges

    def compute_unit_score(self, quantity_ref, refresh=False, **kwargs):
        """
        four different ways to do this.
        0- we are a subfragment-- no direct impacts unless non-descend, which is caught earlier
        1- parent is bg: ask catalog to give us bg_lcia (process or fragment)
        2- get fg lcia for unobserved exchanges

        If
        :param quantity_ref:
        :param refresh:
        :return:
        """
        try:
            locale = self.node['SpatialScope']
        except KeyError:
            locale = 'GLO'

        try:
            res = self.node.bg_lcia(quantity_ref, observed=self._parent.child_flows, ref_flow=self.anchor_flow,
                                    locale=locale, **kwargs)
        except (BackgroundRequired, EntityNotFound, NotImplementedError):
            try:
                res = quantity_ref.do_lcia(self.unobserved_exchanges(), locale=locale,
                                           refresh=refresh, **kwargs)
            except (QuantityRequired, NotImplementedError):
                raise UnresolvedAnchor

        if isinstance(res, list):
            [k.scale_result(self.inbound_exchange_value) for k in res]
        else:
            res.scale_result(self.inbound_exchange_value)
        return res

    def score_cache(self, quantity=None, refresh=False, **kwargs):
        """
        only process-terminations are cached
        remote fragments that come back via the API can have cached scores as well, but local subfragments should not
        get cached.

        :param quantity:
        :param refresh: If True, re-compute unit score even if it is already present in the cache.
        :param kwargs:
        :return:
        """
        if quantity is None:
            return self._score_cache

        if self.is_null or self.is_fg:
            return LciaResult(quantity)

        if refresh:
            self._score_cache.pop(quantity, None)

        if quantity in self._score_cache:
            return self._score_cache[quantity]
        else:
            if self.is_frag:  # but not fg, ergo subfrag
                raise UnCachedScore(quantity)
            else:
                res = self.compute_unit_score(quantity, **kwargs)
            if isinstance(res, list):
                for k in res:
                    self._score_cache[k.quantity] = k
            self._score_cache[quantity] = res
            return self._score_cache[quantity]

    def clear_score_cache(self, quantity=None):
        if quantity is not None:
            self._score_cache.pop(quantity, None)
        else:
            self._score_cache = dict()
