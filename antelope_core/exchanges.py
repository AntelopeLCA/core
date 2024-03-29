from antelope import comp_dir
from .contexts import Context, NullContext


class ExchangeError(Exception):
    pass


class NoAllocation(Exception):
    pass


class DirectionlessExchangeError(Exception):
    pass


class DuplicateExchangeError(Exception):
    pass


class AmbiguousReferenceError(Exception):
    pass


class Exchange(object):
    """
    An exchange is an affiliation of a process, a flow, and a direction. An exchange does
    not include an exchange value- though presumably a valued exchange would be a subclass.

    An exchange may specify a uuid of a terminating process; tests for equality will distinguish
    differently-terminated flows. (ecoinvent)
    """

    entity_type = 'exchange'

    def __init__(self, process, flow, direction, termination=None, comment=None):
        """

        :param process:
        :param flow:
        :param direction:
        :param termination: external id of terminating process or None (note: this is uuid for ecospold2)
        :param comment: documentary information about the exchange
        :return:
        """
        # assert process.entity_type == 'process', "- we'll allow null exchanges and fragment-terminated exchanges"
        # assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        # assert direction in directions, "direction must be a string in (%s)" % ', '.join(directions)

        self._process = process
        self._flow = flow
        self._direction = direction
        self._termination = None
        self._comment = comment  # don't bother to serialize these yet...
        if termination is not None:
            if isinstance(termination, str):
                self._termination = termination
            elif isinstance(termination, Context):
                if termination is NullContext:
                    pass
                else:
                    self._termination = termination
                    if termination.origin is None:
                        termination.add_origin(process.origin)

            elif hasattr(termination, 'external_ref'):
                self._termination = termination.external_ref
            else:
                raise ValueError('Unintelligible termination: %s' % termination)
        # self._hash_tuple =
        self._hash = hash((process.external_ref, flow.external_ref, direction, self._termination))
        self._is_reference = False

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        """
        Comment is cumulative.  This should be generalized to the other entities.. after ContextRefactor...
        :param value:
        :return:
        """
        if self._comment is None:
            self._comment = str(value)
        else:
            self._comment += '\n%s' % str(value)

    @property
    def origin(self):
        return self._process.origin

    @property
    def is_entity(self):
        return self.process.is_entity

    @property
    def is_reference(self):
        return self._is_reference

    def set_ref(self, setter):
        if setter is self._process and (self._termination is None or self.type == 'context'):
            self._is_reference = True
            return True
        return False

    def unset_ref(self, setter):
        if setter is self._process:
            self._is_reference = False
            return True
        return False

    def trim(self):
        return self

    @property
    def unit(self):
        try:
            unit = self.flow.unit
        except AttributeError:
            unit = None
        return unit

    @property
    def value(self):
        return None

    @value.setter
    def value(self, exch_val):
        raise ExchangeError('Cannot set Exchange value')

    """
    These all need to be immutable because they form the exchange's hash
    """
    @property
    def process(self):
        return self._process

    @property
    def flow(self):
        return self._flow

    @property
    def direction(self):
        return self._direction

    @property
    def termination(self):
        return self._termination

    @property
    def term_ref(self):
        if isinstance(self._termination, Context):
            return tuple(self._termination)
        else:
            return self._termination


    @property
    def key(self):
        return self._hash

    @property
    def lkey(self):
        """
        Long key, for testing equality with exchange refs whose terminations may be tuples
        :return:
        """
        return self.flow.external_ref, self._direction, self.term_ref  # self._hash_tuple

    def is_allocated(self, reference):
        """
        Stub for compatibility
        :param reference:
        :return:
        """
        return False

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if other is None:
            return False
        '''
        if not hasattr(other, 'entity_type'):
            return False
        if other.entity_type != 'exchange':
            return False
        # if self.key == other.key and self.lkey != other.lkey:
        #     raise DuplicateExchangeError('Hash collision!')
        return self.key == other.key
        '''
        try:
            return self.key == other.key
        except AttributeError:
            return False

    def __repr__(self):
        return '%s(%s, %s, %s)' % (self.__class__.__name__, self._process.external_ref,
                                   self.direction, self._tflow)

    @property
    def comp_dir(self):
        return comp_dir(self.direction)

    @property
    def type(self):
        if self.is_reference:
            return 'reference'
        elif self.termination is not None:
            if isinstance(self.termination, Context):
                return 'context'
            elif self.termination == self.process.external_ref:
                return 'self'
            else:
                return 'node'
        return 'cutoff'

    @property
    def is_elementary(self):
        if isinstance(self.termination, Context):
            return self.termination.elementary
        else:
            return False

    @property
    def term_ref(self):
        if self.termination is None:
            return None
        if isinstance(self.termination, Context):
            return self.termination.name
        return self.termination

    @property
    def _tflow(self):
        """
        indicates how an exchange is terminated:
         '{*}' - reference
         '   ' - cutoff
         '(=)' - elementary context
         '(-)' - other context
         '(o)' - terminated to self
         '(#)' - terminated to other node
        :return:
        """
        if self.is_elementary:
            tmark = '(=)'
        else:
            tmark = {
            'reference': '{*} ',
            'cutoff': '    ',
            'self': '(o) ',
            'context': '(-) ',
            'node': '(#) '
            }[self.type]
        return tmark + str(self.flow)

    def __str__(self):
        return '%s has %s: %s %s' % (self.process, self.direction, self._tflow, self.unit)

    def f_view(self):
        return '%s of %s' % (self.direction, self.process)

    def get_external_ref(self):
        return '%s: %s' % (self.direction, self.flow.external_ref)

    def serialize(self, **kwargs):
        j = {
            'entityType': self.entity_type,
            'flow': self.flow.external_ref,
            'direction': self.direction,
        }
        if self.termination is not None:
            j['termination'] = self.term_ref
        if self.is_reference:
            j['isReference'] = True
        return j

    @classmethod
    def signature_fields(cls):
        return ['process', 'flow', 'direction', 'termination']

    @property
    def args(self):
        return {'comment': self.comment}


class ExchangeValue(Exchange):
    """
    An ExchangeValue is an exchange with a single value (corresponding to unallocated exchange value) plus a dict of
    values allocated to different reference flows.
    """
    '''
    @classmethod
    def from_exchange(cls, exch, value=None, **kwargs):
        if isinstance(exch, ExchangeValue):
            if value is not None:
                raise DuplicateExchangeError('Exchange exists and has value %g (new value %g)' % (exch.value, value))
            return exch
        return cls(exch.process, exch.flow, exch.direction, value=value, **kwargs)
    '''

    @classmethod
    def from_allocated(cls, allocated, reference):
        """
        Use to flatten an allocated process inventory into a standalone inventory
        :param allocated:
        :param reference: a reference exchange
        :return:
        """
        if isinstance(allocated, ExchangeValue):
            return cls(allocated.process, allocated.flow, allocated.direction, value=allocated[reference],
                       termination=allocated.termination, comment=allocated.comment)
        elif isinstance(allocated, Exchange):
            return allocated

    @classmethod
    def from_scenario(cls, allocated, scenario, fallback):
        try:
            value = allocated[scenario]
        except KeyError:
            value = allocated[fallback]
        return cls(allocated.process, allocated.flow, allocated.direction, value=value,
                   termination=allocated.termination)

    def reterminate(self, term=None):
        new = type(self)(self.process, self.flow, self.direction, termination=term, value=self.value,
                         value_dict=self._value_dict)
        if self.comment is not None:
            new.comment = self.comment
        return new

    def add_to_value(self, value, reference=None):
        if reference is None:
            self._value += value
        else:
            if reference in self._value_dict:
                self._value_dict[reference] += value

    def trim(self):
        x = Exchange(self.process, self.flow, self.direction, termination=self.termination)
        if self.is_reference:
            x.set_ref(self.process)
        return x

    def __init__(self, *args, value=None, value_dict=None, **kwargs):
        super(ExchangeValue, self).__init__(*args, **kwargs)
        # assert isinstance(value, float), 'ExchangeValues must be floats (or subclasses)'
        self._value = value
        if value_dict is None:
            self._value_dict = dict()  # keys must live in self.process.reference_entity
        else:
            self._value_dict = value_dict

    @property
    def value(self):
        """
        unallocated value
        :return:
        """
        if self._value is None and len(self._value_dict) == 1:
            return next(v for v in self._value_dict.values())
        return self._value

    @property
    def value_string(self):
        if self._value is None:
            return ' --- '
        return '%.3g' % self._value

    @value.setter
    def value(self, exch_val):
        """
        May only be set once. Otherwise use add_to_value
        :param exch_val:
        :return:
        """
        if self._value is not None:
            raise DuplicateExchangeError('Unallocated exchange value already set to %g (new: %g)' % (self._value,
                                                                                                     exch_val))
        self._value = exch_val

    @property
    def values(self):
        """
        Some Good Question here about what to use for the key part- can't go wrong with str
        :return:
        """
        rtn = {k.flow.external_ref: v for k, v in self._value_dict.items()}
        if self._value is not None:
            rtn[None] = self._value
        return rtn

    def is_allocated(self, key):
        """
        Report whether the exchange is allocated with respect to a given reference.
        :param key: an exchange
        :return:
        """
        if len(self._value_dict) > 0:
            return key in self._value_dict
        return False

    def __getitem__(self, item):
        """
        Implements the exchange relation-- computes the quantity of self that is exchanged for a unit of item,
        according to whatever allocation is specified for the exchange or process.  self and item must share the
        same process (tested as external_ref to allow entities and references to interoperate)

        If self is a reference exchange, the exchange relation will equal either 0.0 or 1.0 depending on whether item
        is self or a different reference.  FOR NOW: Reference exchanges cannot

        When an exchange is asked for its value with respect to a particular reference, lookup the allocation in
        the value_dict.  IF there is no value_dict, then the default _value is returned AS LONG AS the process has
        only 0 or 1 reference exchange.

        Allocated exchange values should add up to unallocated value.  When using the exchange values, don't forget to
        normalize by the chosen reference flow's input value (i.e. utilize an inbound exchange when computing
        node weight or when constructing A + B matrices)
        :param item:
        :return:
        """
        if isinstance(item, str) and item in ('process', 'flow', 'termination', 'value'):
            return getattr(self, item)  # %*(#%)(* Marshmallow!
        if not self.process.is_entity:
            return self.process.exchange_relation(item, self.flow, self.direction, self.termination)
        '''
        if len(self._value_dict) == 0:
            # unallocated exchanges always read the same
            return self._value
        '''
        try:
            item = self.process.get_exchange(item.key)
        except KeyError:
            raise ExchangeError('Reference exchange belongs to a different process')
        # elif len(self.process.reference_entity) == 1:
        #    # no allocation necessary
        #    return self.value
        if item.is_reference:
            if self.is_reference:  # if self is a reference entity, the allocation is either .value or 0
                if item.lkey == self.lkey:  # and item.direction == self.direction:
                    return 1.0
                return 0.0
            elif self.process.alloc_qty is not None:
                exch_norm = self.value / self.process.alloc_total  # exchange value per total output quantity
                ref_norm = self.process.alloc_qty.cf(item.flow)  # quantity for a unit output of ref
                return exch_norm * ref_norm
            elif len(self._value_dict) > 0:
                try:
                    return self._value_dict[item] / item.value  # quotient added to correct ecoinvent LCI calcs
                except KeyError:
                    return 0.0
                    # no need to raise on zero allocation
                    # raise NoAllocation('No allocation found for key %s in process %s' % (item, self.process))
                # else fall through
        elif item.termination is not None:
            if not isinstance(item.termination, Context):
                raise ExchangeError('Cannot compute exchange values with respect to a terminated exchange')
        return self._value / item.value

    def __setitem__(self, key, value):
        """
        Used to set custom or "causal" allocation factors.  Here the allocation should be specified as a portion of the
        unallocated exchange value to be allocated to the reference.  The entered values will be normalized by the
        reference exchange's reference value when they are retrieved, to report the exchange value per unit of
        reference.

        This approach makes consistency of causal allocation easy to check: the allocation factors should simply add up
        to the unallocated factor. (this does not apply in ecoinvent, where the different co-products are pre-normalized
        and basically co-inhabiting the same process, and the unallocated factors are not present. The normalization
        must still occur, though, because ecoinvent still does use the *sign* of reference exchanges to indicate
        directionality)
        :param key: a reference exchange
        :param value:
        :return:
        """
        if key.process.external_ref != self.process.external_ref:
            raise ExchangeError('Reference exchange belongs to a different process')
        if not key.is_reference:
            raise AmbiguousReferenceError('Allocation key is not a reference exchange')
        if key in self._value_dict:
            # print(self._value_dict)
            raise DuplicateExchangeError('Exchange value already defined for this reference!')
        if self.is_reference:
            # if it's a reference exchange, it's non-allocatable and should have an empty value_dict
            if self.lkey == key.lkey:
                self.value = value
                # if value != 1:
                #    raise ValueError('Reference Allocation for reference exchange should be 1.0')
            else:
                if value != 0:
                    raise ValueError('Non-reference Allocation for reference exchange should be 0.')
        else:
            self._value_dict[key] = value

    def remove_allocation(self, key):
        """
        Removes allocation if it exists; doesn't complain if it doesn't.
        :param key: a reference exchange.
        :return:
        """
        if key in self._value_dict:
            self._value_dict.pop(key)

    def __str__(self):
        if self.process.entity_type == 'fragment':
            if self.flow == self.process.flow and self.comp_dir == self.process.direction:
                ref = '{*}'
            else:
                ref = '   '
        else:
            if self.is_reference:
                ref = '{*}'
            else:
                ref = '   '
        return '%6.6s: %s [%s %s] %s' % (self.direction, ref, self.value_string, self.unit, self._tflow)

    def f_view(self):
        return '%6.6s: [%s %s] %s' % (self.direction, self.value_string, self.unit, self.process)

    def _serialize_value_dict(self):
        j = dict()
        for k, v in self._value_dict.items():
            j['%s:%s' % (k.direction, k.flow.external_ref)] = v
        return j

    def serialize(self, values=False):
        j = super(ExchangeValue, self).serialize()
        if values:
            if self.value is not None:
                j['value'] = float(self.value)
            if not self.is_reference and len(self._value_dict) > 0:
                j['valueDict'] = self._serialize_value_dict()
        return j


class DissipationExchange(ExchangeValue):
    """
    Composition / Dissipation mechanics can be encapsulated entirely into the exchange object- the problem
     is reduced to a serialization / deserialization problem.

    Composition / Dissipation probably conflicts with Allocation, i.e. it is not supported to add a dissipation
    factor to an AllocatedExchange.

    This is a problem because the allocated exchange is also the secret sauce being used to implement processflow
    parameters- and would presumably be the same mechanism to implement dissipation parameters.

    best solution would be to fold reference spec higher up in inheritance- also need an ironclad way to determine
     the input flow (or to not support dissipation for processes with more than one reference flow)
    and simply make flow_quantity not None be the key to interpret the exchange value as a dissipation rate.
    except that I don't want to squash the non-dissipation exchange value.

    Maybe I should not be worrying about this right now.

    """
    def __init__(self, *args, flow_quantity=None, scale=1.0, dissipation=1.0, value=None, **kwargs):
        self.flow_quantity = flow_quantity
        self.scale = scale
        self.dissipation = dissipation
        super(ExchangeValue, self).__init__(*args, **kwargs)
        self._value = value  # used only when dissipation is not defined

    def content(self, ref_flow=None):
        """
        :param ref_flow: a flow LcEntity
        :return:
        """
        if ref_flow is None:
            ref_flow = list(self.process.reference_entity)[0]
            if len(self.process.reference_entity) > 1:
                raise AmbiguousReferenceError
        if ref_flow.cf(self.flow_quantity) != 0:
            return ref_flow.cf(self.flow_quantity)
        return None

    @property
    def value(self):
        c = self.content()
        if c is not None:
            return c * self.scale * self.dissipation
        return self._value

    def __str__(self):
        raise NotImplemented

    def serialize(self, values=False):
        raise NotImplemented


class MarketExchange(Exchange):
    """
    A MarketExchange is an alternative implementation of an ExchangeValue that handles the multiple-input process
    case, i.e. when several processes produce the same product, and the database must balance them according to
    some apportionment of market value or production volume.

    The client code has to explicitly create a market exchange.  How does it know to do that? in the case of
    ecospold2, it has to determine whether the process has duplicate [non-zero] flows with activityLinkIds.

    In other cases, it will be foreground / linker code that does it.

    Add market suppliers using dictionary notation.  Use exchange values or production volumes, but do it consistently.
    The exchange value returned is always the individual supplier's value divided by the sum of values.
    """
    def __init__(self, *args, **kwargs):
        super(MarketExchange, self).__init__(*args, **kwargs)
        self._market_dict = dict()

    def _sum(self):
        return sum([v for k, v in self._market_dict.items()])

    def keys(self):
        return self._market_dict.keys()

    def markets(self):
        return self._market_dict.items()

    def __setitem__(self, key, value):
        if key in self._market_dict:
            raise KeyError('Key already exists with value %g (new value %g)' % (self._market_dict[key], value))
        self._market_dict[key] = value

    def __getitem__(self, item):
        return self._market_dict[item] / self._sum()

    def __str__(self):
        return 'Market for %s: %d suppliers, %g total volume' % (self.flow, len(self._market_dict), self._sum())

    def serialize(self, values=False):
        j = super(MarketExchange, self).serialize()
        if values:
            j['marketSuppliers'] = self._market_dict
        else:
            j['marketSuppliers'] = [k for k in self.keys()]
        return j
