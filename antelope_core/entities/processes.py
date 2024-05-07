from __future__ import print_function, unicode_literals
from numbers import Number
from math import isclose

import uuid

from collections import defaultdict

from antelope import check_direction, NoReference

from .entities import LcEntity
from ..exchanges import ExchangeValue, DuplicateExchangeError, AmbiguousReferenceError


class ZeroAllocation(Exception):
    pass


class MissingAllocation(Exception):
    pass


class NoExchangeFound(Exception):
    pass


class MultipleReferencesFound(Exception):
    """
    Whereas AmbiguousReferenceError indicates that further filtering is possible; MultipleReferencesFound indicates
    that there is no way to provide additional information.
    """
    pass


class NotAReference(Exception):
    pass


class AlreadyAReference(Exception):
    pass


class ReferenceSettingFailed(Exception):
    pass


class LcProcess(LcEntity):

    _ref_field = 'referenceExchange'
    _new_fields = ['SpatialScope', 'TemporalScope', 'Classifications']

    @classmethod
    def new(cls, name, **kwargs):
        """
        :param name: the name of the process
        :return:
        """
        u = uuid.uuid4()
        return cls(str(u), entity_uuid=u, Name=name, **kwargs)

    def __init__(self, external_ref, **kwargs):
        """
        THe process's data is a set of exchanges.

        A process's reference entity is a subset of these.  It is an error for these exchanges to have terminations
        (if they're terminated, they're not reference flows- they're dependencies). These references can be used
        as allocation keys for the exchanges.

        The entities in reference_entity and _exchanges are not necessarily the same, although they should hash the
        same.  Not sure whether this is a design flaw or not- but the important thing is that reference entities do
        not need to have exchange values associated with them (although they could).

        process.find_reference(key), references() [generator], and reference(flow) all return entries from _exchanges,
        not entries from reference_entity.  The only public interface to the objects in reference_entity is
        reference_entity itself.
        :param entity_uuid:
        :param kwargs:
        """
        self._exchanges = dict()  # maps exchange key to exchange
        self._exch_map = defaultdict(set)  # maps flow external_ref to exchanges having that flow

        super(LcProcess, self).__init__('process', external_ref, **kwargs)
        if self._reference_entity is not None:
            raise AttributeError('How could the reference entity not be None?')
        self._reference_entity = dict()  # it is not possible to specify a valid reference_entity on init
        self._alloc_by_quantity = None
        self._alloc_sum = 0.0

        if 'SpatialScope' not in self._d:
            self._d['SpatialScope'] = 'GLO'
        if 'TemporalScope' not in self._d:
            self._d['TemporalScope'] = '0'
        if 'Classifications' not in self._d:
            self._d['Classifications'] = []

    @property
    def name(self):
        return self['Name']

    def __str__(self):
        return '%s [%s]' % (self._d['Name'], self._d['SpatialScope'])

    def __len__(self):
        return len(self._exchanges) + len(self._reference_entity)

    @property
    def reference_entity(self):
        return set(self._reference_entity.values())

    def _validate_reference(self, ref_set):
        """
        An exchange may be a reference only if it is null-terminated or terminated to a non-elementary context.
        :param ref_set:
        :return:
        """
        for x in ref_set:
            if x.termination is not None and x.type not in ('context', 'reference'):  # excludes 'node', 'self'
                return False
            if not super(LcProcess, self)._validate_reference(x):
                return False
        return True

    def _print_ref_field(self):
        return 'see exchanges'

    def _set_reference(self, exch_to_be_ref):
        """
        :param exch_to_be_ref: a dependent exchange to be made a reference exchange. This will fail if the exchange
        is already a reference.
        :return:
        """
        if not self._validate_reference({exch_to_be_ref}):
            raise ReferenceSettingFailed(exch_to_be_ref)

        if exch_to_be_ref.key in self._exchanges:
            # assert exch_to_be_ref is self._exchanges[exch_to_be_ref.key]
            self._exchanges.pop(exch_to_be_ref.key)
            if exch_to_be_ref.set_ref(self):
                self._reference_entity[exch_to_be_ref.flow.external_ref, exch_to_be_ref.direction] = exch_to_be_ref
            else:
                raise ReferenceSettingFailed('%s\n%s' % (self, exch_to_be_ref))
        else:
            raise NoExchangeFound
        if self.alloc_qty is not None:
            self.allocate_by_quantity(self._alloc_by_quantity)

    @property
    def alloc_qty(self):
        if self._alloc_sum != 0:
            return self._alloc_by_quantity
        return None

    @property
    def alloc_total(self):
        return self._alloc_sum

    def show_inventory(self, reference=None):
        """
        Convenience wrapper around self.inventory() which:
         * sorts the exchanges by reference, then by direction
         * prints the exchanges to output
         * provides an enumeration of exchanges for interactive access
         = returns the exchanges as a sorted list.
        :param reference:
        :return:
        """
        num = 0
        it = sorted(self.inventory(reference), key=lambda x: (not x.is_reference, x.direction))
        if reference is None:
            print('%s' % self)
        else:
            print('Reference: %s' % reference)
        for i in it:
            print('%2d %s' % (num, i))
            num += 1
        return it

    def _gen_exchanges(self, flow=None, direction=None, reference=None):
        """
        Generate a list of exchanges matching the supplied flow and direction.

        If this is too slow, we could hash the process's exchanges by flow
        :param flow: either a flow entity, flow ref, or external_ref
        :param direction: [None] or filter by direction
        :param reference: [None] find any exchange; True: only reference exchanges; False: only non-reference exchanges
        :return:
        """
        if flow is None:
            if reference is True:
                _x_gen = self.references()
            elif reference is False:
                _x_gen = (x for x in self._exchanges.values())
            else:
                _x_gen = (x for s in self._exch_map.values() for x in s)
        else:
            if hasattr(flow, 'entity_type'):
                if flow.entity_type != 'flow':
                    raise TypeError('Flow argument must be a flow')
                flow = flow.external_ref
            if reference is True:
                _x_gen = (x for x in self._exch_map[flow] if x.is_reference)
            elif reference is False:
                _x_gen = (x for x in self._exch_map[flow] if not x.is_reference)
            else:
                _x_gen = (x for x in self._exch_map[flow])
        for x in _x_gen:
            if direction is not None:
                if x.direction != direction:
                    continue
            yield x

    def get_exchange(self, key):
        try:
            return self._exchanges[key]
        except KeyError:
            try:
                return next(x for x in self.reference_entity if x.key == key)
            except StopIteration:
                raise KeyError

    def exchanges(self, flow=None, direction=None):
        for x in self._gen_exchanges(flow=flow, direction=direction):
            yield x.trim()

    def exchange_values(self, flow, direction=None):
        """
        Yield full exchanges matching flow specification.  Flow specification required.
        Will only yield multiple results if there are multiple terminations for the same flow.
        :param flow:
        :param direction:
        :return:
        """
        for x in self._gen_exchanges(flow=flow, direction=direction):
            yield x

    def has_exchange(self, flow, direction=None):
        try:
            next(self.exchange_values(flow, direction=direction))
        except StopIteration:
            return False
        return True

    def inventory(self, ref_flow=None):
        """
        generate a process's exchanges.  If no reference is supplied, generate unallocated exchanges, including all
        reference exchanges.  If a reference is supplied AND the process is allocated with respect to that reference,
        generate ExchangeValues as allocated to that reference flow, and exclude reference exchanges.  If a reference
        is supplied but the process is NOT allocated to that reference, generate unallocated ExchangeValues (excluding
        the reference itself).  Reference must be a flow or exchange found in the process's reference entity.

        :param ref_flow:
        :return:
        """
        if ref_flow is None:
            ref_exch = None
            for i in self.reference_entity:
                yield i
        else:
            ref_exch = self.reference(ref_flow)
        for i in self._exchanges.values():
            if ref_exch is None:
                # generate unallocated exchanges
                yield i
            elif ref_exch.is_reference:
                # generate allocated, normalized, non-reference exchanges
                if i in self.reference_entity:
                    continue
                else:
                    yield ExchangeValue.from_allocated(i, ref_exch)
            else:
                # generate un-allocated, normalized, non-query exchanges
                if i is ref_exch:
                    continue
                else:
                    yield ExchangeValue.from_allocated(i, ref_exch)

    def exchange_relation(self, ref_flow, exch_flow, direction, termination=None):
        rx = self.reference(ref_flow)
        if termination is None:
            xs = [x for x in self.exchange_values(flow=exch_flow, direction=direction)]
            if len(xs) == 1:
                return xs[0].value / rx.value
            elif len(xs) == 0:
                return 0.0
            else:
                return sum([x.value for x in xs]) / rx.value
        else:
            x = self.get_exchange(hash((self.external_ref, exch_flow, direction, termination)))
            return x[rx]

    def find_reference(self, spec=None, direction=None):
        """
        returns a reference exchange matching the specification.

        If multiple results are found, filters out terminated exchanges

        :param spec: could be None, external_ref, flow, flow ref, or exchange
        :param direction: could be helpful if the object is a non-reference exchange
        :return:
        """
        if hasattr(spec, 'entity_type'):
            if spec.entity_type == 'exchange':
                if direction is None:
                    direction = spec.direction
                _x_gen = self._gen_exchanges(flow=spec.flow.external_ref, direction=direction, reference=True)
            elif spec.entity_type == 'flow':
                _x_gen = self._gen_exchanges(flow=spec.external_ref, direction=direction, reference=True)
            else:
                raise TypeError('Cannot interpret specification %s (%s)' % (spec, type(spec)))
        else:  # works for spec=external_ref or spec is None
            _x_gen = self._gen_exchanges(spec, direction=direction, reference=True)

        candidates = list(_x_gen)

        if len(candidates) == 0:
            raise NoExchangeFound('%s: %s' % (self.external_ref, spec))
        elif len(candidates) > 1:
            raise MultipleReferencesFound('%d exchanges found; try specifying direction' % len(candidates))
        else:
            return candidates[0]

    '''
    def _strip_term(self, flow, dirn):
        """
        Removes an existing terminated exchange and replaces it with an unterminated one
        """
        exs = [k for k in self._gen_exchanges(flow, dirn) if k.termination is not None]
        if len(exs) > 1:
            raise DuplicateExchangeError('%d terminated exchanges found for %s: %s' % (len(exs), dirn, flow))
        elif len(exs) == 0:
            return  # NoExchangeFound in _set_reference instead of here
        ex = exs[0]

        old = self._exchanges.pop(ex.key)
        new = old.reterminate(None)
        if new.key in self._exchanges:
            raise KeyError('Unterminated exchange already exists!')
        self._exchanges[new.key] = new
        self._exch_map[new.flow.external_ref].remove(old)
        self._exch_map[new.flow.external_ref].add(new)
    '''

    def set_reference(self, flow, dirn):
        """
        Exchange must already exist.
        fmr: If the exchange is currently terminated, the termination is removed.
        now: exchanges terminated to non-elementary context are now allowed
        :param flow:
        :param dirn:
        :return:
        """
        # self._strip_term(flow, dirn)
        dirn = check_direction(dirn)
        if (flow.external_ref, dirn) in self._reference_entity:
            # already a reference
            return self._reference_entity[flow.external_ref, dirn]
        else:
            rx = list(self._gen_exchanges(flow, dirn, reference=False))
            if len(rx) == 0:
                raise NoExchangeFound(flow, dirn)
            elif len(rx) > 1:
                raise AmbiguousReferenceError(rx)
            else:
                self._set_reference(rx[0])
                return rx[0]

    def remove_reference(self, flow, dirn):
        dirn = check_direction(dirn)
        k = (flow.external_ref, dirn)
        rx = self._reference_entity[k]
        if rx.key in self._exchanges:
            raise DuplicateExchangeError(rx)
        self._reference_entity.pop(k)
        rx.unset_ref(self)
        self.remove_allocation(rx)
        self._exchanges[rx.key] = rx
        if self._alloc_by_quantity is not None:
            self.allocate_by_quantity(self._alloc_by_quantity)

    def references(self):
        for rf in self.reference_entity:
            yield rf

    def reference(self, flow_ref=None):
        if flow_ref in self.reference_entity:
            return flow_ref
        try:
            return self.find_reference(flow_ref)
        except NoExchangeFound:
            raise NoReference(flow_ref)

    ''' # don't think I want this
    def reference_value(self, flow=None):
        return self.get_exchange(self.find_reference(flow).key).value
    '''

    def has_reference(self, flow=None):
        try:
            self.find_reference(flow)
            return True
        except NoExchangeFound:
            return False

    def _alloc_dict(self, quantity=None):
        """
        Returns a dict mapping reference key to NON-NORMALIZED allocation factor.  This factor reports the amount of
        the reference flow, in dimensions of the specified quantity, that is produced by a UNIT activity of the process.
        Normalized allocation factors are obtained by dividing by the sum of these amounts.
        :param quantity:
        :return:
        """
        if quantity is None:
            if self._alloc_by_quantity is None:
                raise ValueError('An allocation quantity is required to compute normalized allocation factors')
            quantity = self._alloc_by_quantity

        return {rf: rf.value * quantity.cf(rf.flow)
                for rf in self.reference_entity
                if rf.value is not None}

    def allocate_by_quantity(self, quantity):
        """
        Store a quantity for partitioning allocation.  All non-reference exchanges will have their exchange values
        computed based on the total, determined by the quantity specified.  For each
        reference exchange, computes the magnitude of the quantity output from the unallocated process. Reference flows
        lacking characterization in that quantity will receive zero allocation.

        Each magnitude is the allocation numerator for that reference, and the sum of the magnitudes is the allocation
        denominator.
        :param quantity: an LcQuantity (or None to remove quantity allocation)
        :return:
        """
        if quantity is None:
            self._alloc_by_quantity = None
            self._alloc_sum = 0.0
            self._d.pop('AllocatedByQuantity', None)
            return

        mags = self._alloc_dict(quantity)

        total = sum([v for v in mags.values()])
        if total == 0:
            raise ZeroAllocation('%s: zero total found; not setting allocation by qty %s' % (self.external_ref, quantity))

        self._alloc_by_quantity = quantity
        self._alloc_sum = total
        self['AllocatedByQuantity'] = quantity

    def allocation_factors(self, quantity=None):
        """
        Returns a dict mapping reference exchange to that reference's allocation factor according to the specified
        allocation quantity.
        If no quantity is specified, the current allocation quantity is used.  DOES NOT AFFECT CURRENT ALLOCATION.
        :param quantity: allocation quantity
        :return:
        """
        d = self._alloc_dict(quantity)
        s = sum(d.values())
        return {k: v / s for k, v in d.items()}

    def is_allocated(self, reference, strict=False):
        """
        Tests whether a process's exchanges contain allocation factors for a given reference.
        :param reference:
        :param strict: [False] if True, raise an exception if some (but not all) exchanges are missing allocations.
        :return: True - allocations exist; False - no allocations exist; raise MissingFactor - some allocations exist
        """
        try:
            reference = self.find_reference(reference)
        except NoExchangeFound:
            print('Not a reference exchange.')
            return False
        if self.alloc_qty is not None:
            if self.alloc_qty.cf(reference.flow) == 0:
                return False
            return True
        missing_allocations = []
        has_allocation = []
        for x in self._exchanges.values():
            if x.is_allocated(reference):
                has_allocation.append(x)
            else:
                missing_allocations.append(x)
            if not strict:
                if len(has_allocation) > 0:
                    return True  # for nonstrict, bail out as soon as any allocation is detected
        if len(has_allocation) * len(missing_allocations) == 0:
            if len(has_allocation) == 0:
                return False
            return True
        if strict:
            for x in missing_allocations:
                print('in process %s [%s]\nReference: %s' % (self['Name'], self.external_ref,
                                                             reference.flow.external_ref))
                print('%s' % x)
                raise MissingAllocation('Missing allocation factors for above exchanges')

    def test_allocation_consistency(self, flow=None, display=True):
        """
        For each non-reference item in the inventory, test that the values allocated to each reference, weighted by the
        reference values, sum up to the un-allocated value.

        :param flow: [None] single flow to test; defaults to entire inventory
        :param display: [True] whether to print output to the screen
        :return:
        """
        _d = {1: '', 0: 'X'}
        if flow is None:
            xs = list(self._exchanges.values())
        else:
            xs = list(self._gen_exchanges(flow))
        if display:
            print('%-60.60s %-10.10s %-10.10s' % ('exchange', 'total', 'unalloc'))
        test = True
        for x in xs:
            total = sum(x[rx] * rx.value for rx in self.references())
            loc = isclose(total, x.value, rel_tol=1e-8)
            test &= loc
            if display:
                print('%-60.60s %10.10s %10.10s  %s' % (x, total, x.value, _d[int(loc)]))
        if display:
            print({True: 'PASS', False: 'FAIL'}[test])
        return test

    def remove_allocation(self, reference):
        for x in self._exchanges.values():
            x.remove_allocation(reference)

    def unobserved_lci(self, *args, **kwargs):
        raise NotImplementedError

    def add_exchange(self, flow, dirn, reference=None, value=None, termination=None, add_dups=False):
        """
        This is used to create Exchanges and ExchangeValues and AllocatedExchanges.

        If the flow+dir+term is already in the exchange set:
            if no reference is specified and/or no value is specified- nothing to do
            otherwise (if reference and value are specified):
                upgrade the exchange to an allocatedExchange and add the new reference exch val
        otherwise:
            if reference is specified, create an AllocatedExchange
            otherwise create an Exchange / ExchangeValue

        :param flow:
        :param dirn:
        :param reference:
        :param value:
        :param termination: None for reference or cutoff flows; a context for elementary flows; a valid external_ref
         for terminated intermediate flows.
        :param add_dups: (False) set to true to handle "duplicate exchange" errors by cumulating their values
        :return:
        """
        dirn = check_direction(dirn)
        if (flow.external_ref, dirn) in self._reference_entity:
            raise AlreadyAReference((flow, dirn))
        _x = hash((self.external_ref, flow.external_ref, dirn, termination))
        if _x in self._exchanges:
            e = self._exchanges[_x]
            if value is None or value == 0:
                return e  # the client code *never* expects None
            if reference is None:
                if isinstance(value, dict):
                    e.update(value)
                else:
                    try:
                        e.value = value  # this will catch already-set errors
                    except DuplicateExchangeError:
                        if add_dups:
                            e.add_to_value(value)
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.external_ref, e))
                            raise
                return e

            else:
                try:
                    e[reference] = value  # this will catch already-set errors
                except DuplicateExchangeError:
                    if add_dups:
                        e.add_to_value(value, reference=reference)
                    else:
                        print('Duplicate exchange in process %s:\n%s' % (self.external_ref, e))
                        raise
                except ValueError:
                    print('Error adding [%s] = %10.3g for exchange\n%s\nto process\n%s' % (
                        reference.flow.external_ref, value, e, self.external_ref))
                    raise

                return e

        else:
            if isinstance(value, Number) or value is None:
                if reference is None:
                    e = ExchangeValue(self, flow, dirn, value=value, termination=termination)
                else:
                    if reference not in self.reference_entity:
                        raise KeyError('Specified reference is not registered with process: %s' % reference)
                    e = ExchangeValue(self, flow, dirn, value=None, termination=termination)
                    e[reference] = value

            elif isinstance(value, dict):
                e = ExchangeValue(self, flow, dirn, value_dict=value, termination=termination)
            else:
                raise TypeError('Unhandled value type %s' % type(value))

            # This is the only point an exchange (must be ExchangeValue) is added to the process (see also _strip_term)
            self._exchanges[e.key] = e
            self._exch_map[e.flow.external_ref].add(e)
            return e

    def merge(self, other):
        raise NotImplemented('This should be done via fragment construction + aggregation')

    def serialize(self, exchanges=False, domesticate=False, drop_fields=(), **kwargs):
        j = super(LcProcess, self).serialize(domesticate=domesticate, drop_fields=drop_fields)
        j.pop(self._ref_field)  # reference reported in exchanges
        j['exchanges'] = sorted([x.serialize(**kwargs) for x in self.reference_entity],
                                key=lambda x: (x['direction'], x['flow']))
        if exchanges:
            # if exchanges is true, report all exchanges
            j['exchanges'] += sorted([x.serialize(**kwargs) for x in self._exchanges.values()],
                                     key=lambda x: (x['direction'], x['flow']))
        else:
            j.pop('allocationFactors', None)  # added just for OpenLCA JSON-LD, but could be generalized
        return j
