"""
The CLookup is a classic example of trying to solve a very complex data problem with a very complex data structure.
It does not entirely succeed, but it works well enough as a stopgap until I have a chance to give an honest crack at
a graph database.

Flow characterization is the very complex data problem: the "quantity relation" maps a large, diverse set of inputs:
 - flowable (substance)
 - reference quantity (in our data system, tied to flowable)
 - query quantity
 - context / compartment
 - location / locale
to a numeric output, namely the amount of the query quantity that corresponds to a unit of the reference quantity.
Technically, this amount also has uncertainty / other quantitative characteristics.

The idea behind a CLookup is that it contains all known characterizations for a given flowable [substance] with respect
to a given quantity.  The CLookup is selected by specifying the flowable and the quantity, and then the CLookup is used
to retrieve a set of Characterization objects for a given context (hence the 'C' in 'CLookup'). The characterization
already stores a mapping of locale to factor, and also stores the flowable (with its native reference quantity used
to interpret the factor).

So it solves a very narrow portion of the problem and leaves a lot to outside code.

The dream would be to design a graph database that held all of these parameters and magically obtained all the factors
that applied to a given query-- that graph database would replace the current Term Manager and everything else under
its hood. But first we will learn to walk...
"""
from ..contexts import Context, NullContext


class QuantityMismatch(Exception):
    pass


class DuplicateOrigin(Exception):
    pass


class CLookup(object):
    """
    A CLookup is a kind of fuzzy dictionary that maps context to best-available characterization. A given CLookup is
    associated with a single quantity and a specific flowable.  The query then provides the compartment and returns
    either: a set of best-available characterizations; a single characterization according to a selection rule; or a
    characterization factor (float) depending on which method is used.
    """
    def __init__(self):
        self._dict = dict()
        self._q = None

    def __repr__(self):
        return '%s(%s: %d contexts)' % (self.__class__.__name__, self._q, len(self._dict))

    def __getitem__(self, item):
        """
        Returns
        :param item:
        :return:
        """
        if item is None:
            item = NullContext
        if not isinstance(item, Context):
            raise TypeError('Supplied CLookup key is not a Context: %s (%s)' % (item, type(item)))
        if item in self._dict:
            return self._dict[item]
        return set()

    def __contains__(self, item):
        return item in self._dict

    def _check_qty(self, cf):
        if self._q is None:
            self._q = cf.quantity
        else:
            if cf.quantity != self._q:
                raise QuantityMismatch('Inbound: %s\nCurrent: %s' % (cf.quantity, self._q))

    def add(self, value, key=None):
        if key is None:
            key = value.context
        if isinstance(key, Context):
            self._check_qty(value)
            if key not in self._dict:
                self._dict[key] = set()
            '''
            if any(k.origin == value.origin for k in self._dict[key]):
                raise DuplicateOrigin(value.origin)
            '''
            self._dict[key].add(value)
        else:
            raise ValueError('Context is not valid: %s (%s)' % (key, type(key)))

    def remove(self, value):
        key = value.context
        self._dict[key].remove(value)

    def keys(self):
        return self._dict.keys()

    def cfs(self):
        for c, cfs in self._dict.items():
            for cf in cfs:
                yield cf

    def _context_origin(self, item, origin):
        if origin is None:
            return list(self.__getitem__(item))
        else:
            return [k for k in self.__getitem__(item) if k.origin == origin]

    def find(self, item, dist=1, return_first=True, origin=None):
        """
        Hunt for a matching compartment. 'dist' param controls the depth of search:
          dist = 0: equivalent to __getitem__
          dist = 1: also check compartment's children (subcompartments), to any depth, returning all CFs encountered
            (unless return_first is True, in which case all CFs from the first nonempty compartment found are returned)
          dist = 2: also check compartment's parent
          dist = 3: also check all compartment's parents until root. Useful for finding unit conversions.
        By default (dist==1), checks compartment self and children. Returns a set.
        :param item: a Compartment
        :param dist: how far to search (with limits) (default: 1= compartment + children)
        :param return_first: stop hunting as soon as a cf is found
        :param origin: [None] if present, only return cfs whose origins match the specification
        :return: a list of characterization factors that meet the query criteria, ordered by increasing dist
        """
        #!TODO: This should be performed by the term manager, not the CLookup (though that is the whole point of CLookup)
        if not isinstance(item, Context):
            return []

        def found(res):
            return len(res) > 0 and return_first
        results = self._context_origin(item, origin)
        if found(results):
            return results

        if dist > 0:
            for s in item.self_and_subcompartments:  # note: this is depth first
                if s is item:
                    continue  # skip self, just recurse subcompartments
                results += self._context_origin(s, origin)
                if found(results):
                    return results

        if dist > 1:
            item = item.parent
            results += self._context_origin(item, origin)
            if found(results):
                return results

        while dist > 2 and item is not None:
            item = item.parent
            results += self._context_origin(item, origin)
            if found(results):
                return results

        return results

    def find_first(self, item, dist=3):
        cfs = self.find(item, dist=dist, return_first=True)
        return list(cfs)[0]

    @staticmethod
    def _ser_set(cf_set, values=False):
        return {cf.origin: cf.serialize(values=values, concise=True) for cf in cf_set}

    def serialize(self, values=False):
        return {str(c): self._ser_set(cfs, values=values) for c, cfs in self._dict.items()}

    def serialize_for_origin(self, origin, values=False):
        """
        Note: In the event that the CLookup includes multiple CFs for the same flowable and the same origin, only the
        first (at random) will be included, because originally I had disallowed multiple CFs for the same origin.
        :param origin:
        :param values:
        :return:
        """
        cxs = dict()
        for c, cfs in self._dict.items():
            try:
                cf_filt = next(cf for cf in cfs if cf.origin == origin)  # duplicate entries per origin not allowed
            except StopIteration:
                continue
            cxs[str(c)] = cf_filt.serialize(values=values, concise=True)
        return cxs


class FactorCollision(Exception):
    pass


class SCLookup(CLookup):
    """
    A Strict CLookup that permits only one CF to be stored per compartment and raises an error if an additional one
    is added.
    """
    def add(self, value, key=None):
        if key is None:
            key = value.context
        if key in self._dict and len(self._dict[key]) > 0:
            existing = list(self._dict[key])[0]
            if existing.value == value.value:
                return
            print('Collision with context: %s' % repr(key))
            print(repr(value))
            print('%s current' % repr(existing))
            raise FactorCollision('This context already has a CF defined!')
        super(SCLookup, self).add(value, key)
