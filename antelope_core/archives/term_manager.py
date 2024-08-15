"""
Edelen and Ingwersen et al 2017:
Recommendations: "... separating flowable names from context and unit information ..."

Term Manager - used to handle string references to entities that are known by several synonyms.  Specifically:
 - contexts, which are drawn from flows' specified 'Compartment' or other classifying properties
 - flowables, which are drawn from flows' uuid and external link, and Name, CasNumber, Synonyms properties
 - quantities, which should be recognizable by external ref or uuid

The main objective of the TermManager is to enable the use of generalized synonym matching in performing LCIA. It
collects two kinds of mapping information: mapping string terms to flowables and contexts; and mapping (quantity,
flowable, context) tuples to [regionalized] characterization objects.

LciaEngine is designed to handle information from multiple origins to operate as a qdb in a catalog context.  The
subclass adds canonical lists of flowables and contexts that would be redundant if loaded into individual archives,
introduces "smart" hierarchical context lookup (CLookup), and adds the ability to quell biogenic CO2 emissions.
Someday, it might make sense to expose it as a massive, central graph db.

INTERFACE
---------
The TermManager is assumed to implement the following interface:
Required for external operability:
 - is_lcia_engine: [bool] whether the term manager performs flow and context matching
 - is_context(obj): [bool] whether the supplied object maps to a known Context;;; hmm, this was implemented twice
 - __getitem__(obj): retrieve a context or None **note: this is because archives all return None for failed __getitem__,
    which I know is bad but I haven't been moved to change it yet)
 - get_canonical(qty): return the best-fit quantity, or raise EntityNotFou
 - synonyms()  # currently required by BasicImplementation

Required by the default implementation:
Post data:
 - add_quantity()
 - add_context()
 - add_flow()
 - add_characterization()
 - add_from_json()

retrieve data:
 - serialize()
 - flows_for_flowable()
 - factors_for_flowable()
 - factors_for_quantity()
 - get_flowable()
 - flowables()
 - quantities()
 - contexts()



"""
from collections import namedtuple

from synonym_dict import SynonymDict, MergeError
from synonym_dict.compartments import InconsistentLineage, NonSpecificCompartment

from antelope import EntityNotFound
from ..contexts import ContextManager, NullContext
from .quantity_manager import QuantityManager

from ..characterizations import Characterization, DuplicateCharacterizationError

import logging


DuplicateCharacterization = namedtuple('DuplicateCharacterization', ('qq_link', 'fb', 'cx_list', 'origin', 'flowable', 'location'))


class FactorConflict(Exception):
    pass


''' # cannot figure out what this is supposed to guard against
class QuantityConflict(Exception):
    pass
'''


class FlowableConflict(Exception):
    """
    A list of synonyms indicates two or more flowables
    """
    pass


class NoFQEntry(Exception):
    """
    This exception has the specific meaning that there is no lookup for the named flow-quantity pair
    """
    pass


class OriginStrangeness(Exception):
    pass


from collections import defaultdict


class TermManager(object):
    """
    A TermManager is an archive-specific mapping of string terms to flowables and contexts.  During normal operation
    it is captive to an archive and automatically harvests flowable and context terms from flows when added to the
    archive.  It also hosts a set of CLookups for the archive's quantities, which enable fuzzy traversal of
    context hierarchies to find best-fit characterization factors.

    When a new entity is added to the archive, there are two connections to make: combine uuids and human-readable
    descriptors into a common database of synonyms, and connect those sets of synonyms to a set of entities known
    to the local archive.

    When harvesting terms from a new flow, the general approach is to merge the new entry with an existing entry
    if the existing one uniquely matches.  If there is more than one match, there are three general strategies:

      'prune': trim off the parts that match existing entries and make a new entry with only the distinct terms.
       this creates a larger more diffuse

      'merge': merge all the terms from the new entry and any existing entries together

    The mapping is prolific in both cases- adds a flowable-to-flow mapping for every flowable that matches a given flow
    (merge_strategy='merge' ensures that this is exactly one flowable), and adds the CF to every flowable that matches.

    USAGE MODEL: TODO
    The Term Manager is basically a giant pile of Characterization objects, grouped by canonical query quantity,
    flowable, and context. The basic TermManager enforces a restriction of one CF per qq | fb | cx combination.  The
    LciaEngine subclass uses a CLookup that can either be strict or non-strict, but nonetheless enforces one CF per
    qq | fb | cx | origin combination.

    There are thus four things the Term Manager must be able to do:
     - store names of flowables and contexts from source data
     - retrieve canonical flowable and context names from the store
     - store characterizations, properly indexed by canonical names
     - retrieve characterizations according to a query

    The components used to accomplish this are:
     _cm and _fm: the context and flowable synonym sets
     _flow_map: reverse-maps flowable terms to flows

     _q_dict: a 3-level nested dictionary:
         defaultdict: quantity uuid -> defaultdict: flowable -> CLookup: context-> {CFs}
       - first level defaultdict maps quantity uuid to second level
         - second level defaultdict maps flowable canonical name to CLookup / subclass
           - third level CLookup maps context to a set of CFs
     _fq_map: reverse-maps flowable canonical name to a set of quantities that characterize it
    """
    is_lcia_engine = False

    def __init__(self, contexts=None, flowables=None, quantities=None, merge_strategy='graft', quiet=True):
        """
        :param contexts: optional filename to initialize CompartmentManager
        :param flowables: optional filename to initialize FlowablesDict
        :param merge_strategy: (can also be specified at add_flow())

           'graft': - on conflict, follow incoming flow's link or name; add new terms to the existing flowable and
            discard conflicting terms.  solves many "shared CAS number" problems-- by depriving the new flowable of the
            CAS identifier

           'prune' or 'distinct': - on conflict, create a new flowable containing only new terms, discard all
            conflicting terms. This is the default for flows added with no context (so that distinct intermediate
            flows with the same name e.g. "bolt 10mm" could have different characterizations. If you want to
            prevent this, give your flow any nonempty context)

           'merge': - aggressively merge all co-synonymous flowables.  not tested.

        :param quiet:

        """
        # the synonym sets
        self._cm = ContextManager(source_file=contexts)
        self._fm = SynonymDict(source_file=flowables, entry_group="Flowables")
        self._qm = QuantityManager(source_file=quantities)

        self._q_dict = dict()  # dict of dicts. _q_dict[canonical quantity][canonical flowable] -> a context lookup
        #
        self._cl_typ = dict

        # the reverse mappings
        self._flow_map = defaultdict(set)  # maps flowable to flows having that flowable
        self._fq_map = dict()  # maps flowable to quantities characterized by that flowable

        # track duplicates
        self._dupes = []  #  List[DuplicateCharacterization]

        # config
        self._merge_strategy = merge_strategy
        self._quiet = bool(quiet)

    '''
    Utilities
    '''
    def is_context(self, item):
        try:
            self._cm.__getitem__(item)
            return True
        except KeyError:
            return False

    def __getitem__(self, item):
        """
        TermManager.__getitem__ retrieves a context known to the TermManager, or None if one is not found.
        Getitem exposes only the contexts, since flow external_refs are used as flowable synonyms
        :param item:
        :return:
        """
        if item is None:
            return None
        try:
            return self._cm.__getitem__(item)
        except KeyError:
            return None

    def _print(self, *args):
        if not self._quiet:
            print(*args)

    def add_terms(self, term_type, *terms, **kwargs):
        d = {'context': self._cm.new_entry,
             'flow': self._create_flowable,
             'flowable': self._create_flowable,
             'quantity': self._qm.new_entry}[term_type]
        d(*terms, **kwargs)

    ''' # OOPS
    @staticmethod
    def is_context(cx):
        return isinstance(cx, Context)
    '''

    @property
    def quiet(self):
        return self._quiet

    def add_quantity(self, quantity):
        if quantity.entity_type != 'quantity':
            raise TypeError('Must be quantity type')
        try:
            ex = self._qm[quantity.link]
            ''' # can't understand what this was supposed to guard against-- and the test that used it was removed so
            if not ex is quantity:
                raise QuantityConflict('Incoming %s does not match existing\n%s' % (ex, quantity))
            '''
        except KeyError:
            self._qm.add_quantity(quantity)
        return self._canonical_q(quantity)

    '''
    Info Storage
    def set_context(self, context_manager):
        """
        A flow will set its own context- but it needs a context manager to do so.

        Not sure whether to (a) remove FlowWithoutContext exception and test for None, or (b) allow set_context to
        abort silently if context is already set. Currently chose (b) because I think I still want the exception.


        :param context_manager:
        :return:
        """
        if context_manager.is_context(self._context):
            # cannot change context once it's set
            return
        if self.has_property('Compartment'):
            _c = context_manager.add_compartments(self['Compartment'])
        elif self.has_property('Category'):
            _c = context_manager.add_compartments(self['Category'])
        else:
            _c = context_manager.get('none')
            # raise AttributeError('Flow has no contextual attribute! %s' % self)
        if not context_manager.is_context(_c):
            raise TypeError('Context manager did not return a context! %s (%s)' % (_c, type(_c)))
        self._context = _c
        self._flowable = context_manager.add_flow(self)
    '''
    def _context_and_parents(self, cx):
        c = self[cx]
        while c is not None:
            yield c
            c = c.parent

    def _add_compartments(self, comps):
        """
        Complicated issue here-- basically, we have to decide on a conflict resolution strategy for
        ContextManager.add_compartments() -- and that strategy must be different for TermManager and LciaEngine.
        For TermManager, the most important thing is to not mess up the literal context specifications, because
        fidelity to source is paramount. Therefore, we should 'rename' contexts and keep them separated.

        Whereas, in LciaEngine the main concern is to harmonize contexts, which means forcing them to play well
        together. This is why we have hints in LciaEngine but not in TermManager.  That is also why the default is to
        attach trees together, rather than duplicate them.

        To TEST: try adding ('emissions', 'emissions to water') and ('elementary flows', 'emissions to water')-- the
        outcome should be different for TermManager and LciaEngine.
        :param comps:
        :return:
        """
        return self._cm.add_compartments(comps, conflict='rename')

    def add_context(self, context, *terms, origin=None):
        """
        We are using conflict='attach' by default so that 'emissions' and 'resources' can be attached to
        'Elementary flows' if it exists. It may cause undesirable side-effects if different intermediate categories
        use similar terms, e.g. ('buildings', 'heat', 'steam') and ('chemical', 'heat', 'process') could result in
        InconsistentLineage  (or else something crazy like ('chemical', 'buildings', 'heat', ('process', 'steam')) )

        :param context:
        :param origin: apply origin to context if it has none
        :return:
        """
        if isinstance(context, str):
            context = (context,)
        cx = self._add_compartments(tuple(context))
        if origin is not None:
            if cx.fullname == cx.name:
                cx.add_origin(origin)
        for term in terms:
            self._cm.add_synonym(cx.name, term)
        return cx

    def _check_context(self, flow):
        """
        The point of this function is simply to log which local context corresponds to the flow's presented context
        :param flow:
        :return:
        """
        if not hasattr(flow, 'context'):
            return NullContext
        try:
            _c = self._cm[flow.get_context()]
        except (KeyError, InconsistentLineage):
            try:
                _c = self._add_compartments(tuple(flow.context))
            except NonSpecificCompartment:
                return NullContext
        _c.add_origin(flow.origin)
        return _c

    def _create_flowable(self, name, *syns, prune=False):
        fb = self._fm.new_entry(name, *syns, prune=prune).object
        if fb in self._fq_map:
            self._print('Adding terms to existing flowable %s' % fb)
        else:
            try:
                self._print('Adding new flowable %s' % fb)
            except TypeError:
                print('Failed to create flowable for %s (%s)' % (name, syns))
                raise
            self._fq_map[fb] = set()
        return fb

    def _check_merge_conflicts(self, dominant, *syns):
        """
        This just brute forces it which must be ungodly slow, and btw it also hasn't been tested

        Inputs must all be canonical _fm entries

        :param dominant:
        :param syns:
        :return:
        """
        fq_conflicts = []
        for f_dict in self._q_dict.values():
            try:
                combo = f_dict[dominant]
            except KeyError:
                combo = self._cl_typ()
            for syn in syns:
                if syn not in f_dict:
                    continue
                cl = f_dict[syn]
                for k in cl.keys():  # of course, we have to check all contexts
                    if k in combo:
                        cand = (combo[k], cl[k])
                        if cand[0].value == cand[1].value:
                            continue
                        fq_conflicts.append(cand)  # but it's really only a conflict if the cf values differ

        if len(fq_conflicts) > 0:
            print('%d Merge conflicts encountered' % len(fq_conflicts))
            raise FactorConflict(fq_conflicts)

    def merge_flowables(self, dominant, *syns):
        return self._merge_terms(dominant, *syns)

    def _merge_terms(self, dominant, *syns):
        """
        Two parts to this: merge the entries in the flowables manager; update local reverse mappings:
         _q_dict [maps canonical quantities to a map of canonical flowables to CLookups]
         _fq_map (nonconflicting) maps flowable to quantities characterized by that flowable
         _flow_map (nonconflicting) maps flowable to flows having that flowable
        Before we can do either, we need to check for collisions

        the second two are nonconflicting- we just union the sets when we merge the flowables. The first one will
        raise a conflict if the two flowables are characterized differently by any one quantity
        :param dominant: MUST BE [canonical] flowables (already stored in _fm)
        :param syns: MUST BE [canonical] flowables (already stored in _fm)
        :return:
        """
        self._check_merge_conflicts(dominant, *syns)

        for qq, f_dict in self._q_dict.items():
            # once we are sure there are no conflicts, we perform the actual merge
            for syn in syns:
                if syn not in f_dict:
                    continue
                cl = f_dict[syn]
                for cx in cl.keys():
                    for cf in cl[cx]:
                        self._qassign(qq, dominant, cf, context=cx)

        if dominant not in self._fq_map:
            self._fq_map[dominant] = set()
        for syn in syns:
            self._fq_map[dominant] |= self._fq_map.pop(syn, set())
            self._flow_map[dominant] |= self._flow_map.pop(syn, set())

        for syn in syns:
            self._fm.merge(dominant, syn)
        return dominant

    @staticmethod
    def _flow_terms(flow):
        """
        This function was created because we don't want TermManagers getting confused about things like misassigned
        CAS numbers, because they don't have the capacity to store multiple CFs.  So we will save the full synonym
        list for the LciaEngine.
        :param flow:
        :return:
        """
        return flow.name, flow.link

    @staticmethod
    def _check_fb_map(fb_map):
        """
        This function was created because we want to be able to manage the list of flowables to merge with
        :param fb_map:
        :return:
        """
        pass

    def flows_for_flowable(self, flowable):
        fb = self.get_flowable(flowable)
        if fb in self._flow_map:
            for f in self._flow_map[fb]:
                yield f

    def _add_to_existing_flowable(self, fb, new_terms):
        for term in new_terms:
            self._fm.add_synonym(str(fb), term)

    def add_flow_terms(self, flow, merge_strategy=None):
        """
        This process takes in an inbound FlowInterface instance, identifies the flowable(s) that match its terms, and
        adds new terms to the existing or new flowable.  May update a flow's name in case of conflict.
        :param flow:
        :param merge_strategy:
        :return:
        """
        merge_strategy = merge_strategy or self._merge_strategy
        fb_map = defaultdict(list)  # list instead of set because sequence matters
        for syn in self._flow_terms(flow):  # we rely on SynonymSet.terms yielding flow.name first
            # make a list of all the existing flowables that match the incoming flow
            fb_map[self._fm.get(syn)].append(syn)
        self._check_fb_map(fb_map)  # allow subclasses to curate fb_map

        new_terms = fb_map.pop(None, [])

        if len(fb_map) == 0:  # all new terms
            if len(new_terms) == 0:
                raise AttributeError('Flow appears to have no terms: %s' % flow)
            fb = self._create_flowable(flow.name, *new_terms)
            new_terms = []  # these have all been added

        elif merge_strategy in ('prune', 'distinct'):
            # fb = self._add_distinct_terms(flow, new_terms)
            if len(new_terms) == 0:
                fb = self._fm.get(flow.link)  # um, I guess if there are no new terms...
            else:
                fb = self._create_flowable(*new_terms, prune=True)  # new_terms cannot be 0
                new_terms = []  # these have all been added or pruned

        elif len(fb_map) == 1:  # one existing match- graft onto that
            fb = list(fb_map.keys())[0]

        else:  # > 2 matches-- invoke merge strategy
            if merge_strategy == 'graft':
                """
                We want link to be dominant but name to be preferred. we have 3 cases:
                 - link is new, name is new: all new flowable with new terms
                 - link is new, name is old: add new terms to old flowable
                 - link is old: add only new terms to old flowable, including or excluding name
                 
                
                note that the keys of fb_map are flowables and the values are incoming terms. but anything remaining 
                in fb_map at this point is a known term so does not need to be added
                """
                self._print('\nPruning entry for %s' % flow)
                if flow.link in new_terms:
                    if flow.name in new_terms:
                        fb = self._create_flowable(flow.name)
                    else:
                        fb = self._fm[flow.name]
                else:
                    fb = self._fm[flow.link]

            elif merge_strategy == 'merge':
                # this is trivial but has never been tested, I mean even once
                self._print('Merging')
                # sort the keys so the one with the greatest number of terms is dominant
                fb = self._merge_terms(*sorted(fb_map.keys(), key=lambda x: len(x), reverse=True))
            else:
                raise ValueError('merge strategy %s' % self._merge_strategy)

        self._add_to_existing_flowable(fb, new_terms)

        # validate that the flow's name looks up proper flowable
        if self._fm[flow.name] is not fb:
            try:
                flow.name = next(k for k in self._flow_terms(flow) if self._fm[k] is fb)
            except StopIteration:
                raise AttributeError('None of the synonyms map to flowable [%s]: %s' % (fb, flow))

        # log the flowables that match the flow
        self._flow_map[fb].add(flow)
        for _tf in fb_map.keys():
            self._flow_map[_tf].add(flow)  # all this belongs in a Graph DB
        return fb

    def add_flow(self, flow, merge_strategy=None):
        """
        We take a flow from outside and add its known terms. That means
         - adding flow's reference quantity
         - merging any context with the local context tree;
         - adding flowable terms to the flowables list;
         - mapping flow to all flowables (assigning flowable if none is found)

        Note that all flows having null context are forced to be distinct from one another. If you want two flowables
        to share properties, you must add them having non-null contexts. not sure how I feel about this but it does work.

        :param flow:
        :param merge_strategy: overrule default merge strategy
        :return: the Flowable object to which the flow's terms have been added
        """
        if hasattr(flow, 'reference_entity') and flow.reference_entity is not None:
            if isinstance(flow.reference_entity, str):
                try:
                    self.get_canonical(flow.reference_entity)
                except EntityNotFound:
                    raise ValueError('unrecognized quantity spec %s ' % flow.reference_entity)
            else:
                self.add_quantity(flow.reference_entity)  # ensure exists
        cx = self._check_context(flow)
        if cx is NullContext:
            merge_strategy = 'distinct'  # keep distinct terms for null-context flows
        return self.add_flow_terms(flow, merge_strategy=merge_strategy)

    def add_characterization(self, flowable, ref_quantity, query_quantity, value, context=None, origin=None,
                             location=None, overwrite=False):
        """
        Replacement for flow-based add_characterization.  THE ONLY place to create Characterization objects.
        Add them to all flowables that match the supplied flow.
        :param flowable: if not known to the flowables dict, added as a new flowable
        :param ref_quantity: [entity or ref]
        :param query_quantity: [entity or ref]
        :param value: mandatory. either a numeric value or a dict mapping locations to values
        :param context: the context for which the characterization applies.  Should be a string or tuple.  None means
          the factor applies to all contexts.
        :param overwrite: whether to overwrite an existing value if it already exists (ignored if value is a dict)
        :param location: (ignored if value is a dict) 'GLO' used if no location is provided
        :param origin: (optional; origin of value; defaults to quantity.origin)
        :return: created or updated characterization
        """
        if origin is None:
            origin = query_quantity.origin

        try:
            cx = self._cm[context]
        except KeyError:
            cx = self.add_context(context, origin=origin)

        try:
            fb = self._fm[flowable]
        except KeyError:
            fb = self._create_flowable(flowable)

        try:
            rq = self._canonical_q(ref_quantity)
        except KeyError:
            rq = self.add_quantity(ref_quantity)

        try:
            qq = self._canonical_q(query_quantity)
        except KeyError:
            qq = self.add_quantity(query_quantity)

        cf = self._find_exact_cf(qq, fb, cx, origin)

        if location is None:
            location = 'GLO'

        if cf is None:
            # create our new Characterization with the provided flowable, not the detected flowable
            new_cf = Characterization(flowable, rq, qq, context=cx, origin=origin)
            if isinstance(value, dict):
                new_cf.update_values(**value)
            else:
                if value == 0:
                    self._print('zero CF: %s, %s, %s, %s, %s, %s' % (flowable, rq.link, qq.link, cx, origin, location))
                new_cf.add_value(value, location=location)

            # add our new CF to the lookup tree
            self._qassign(qq, fb, new_cf, context=cx)

            return new_cf
        else:
            if cf.ref_quantity != rq:
                try:
                    return self._create_conversion_cf(cf, rq, cx, origin, location, value, overwrite=overwrite)
                except DuplicateCharacterizationError as e:
                    print((qq.link, fb, cx.as_list(), origin, flowable, location))
                    print('recursing to %s %s' % (cf.ref_quantity.name, cf.ref_quantity.uuid))
                    print('ignoring duplicate characterization %s' % e)
                    return cf
            # update entry in the lookup tree
            elif isinstance(value, dict):
                cf.update_values(**value)
            else:
                try:
                    cf.add_value(value, location=location, overwrite=overwrite)
                except DuplicateCharacterizationError as e:
                    self._dupes.append(DuplicateCharacterization(qq.link, fb, cx.as_list(), origin, flowable, location))
                    print('ignoring duplicate characterization %s' % e)
            return cf

    def _create_conversion_cf(self, ex_cf, rq, cx, origin, location, value, overwrite=False):
        # create a conversion-factor instead:
        if value == 0:
            # we cannot interpret a 0 as a cf- so we ditch
            return ex_cf

        try:
            factor = value / ex_cf[location]
        except ZeroDivisionError:
            logging.error('Zero-division on conversion CF %s\n%s' % (value, ex_cf))
            try:
                factor = value / ex_cf.value
            except TypeError:  # dict = fail
                factor = value

        if factor < 0:
            logging.warning('Negative conversion CF %s\n%s' % (factor, ex_cf))

        # this recurses to add a cf between our flow's ref quantity and the ref quantity of the already-seen cf
        return self.add_characterization(ex_cf.flowable, rq, ex_cf.ref_quantity, factor, context=cx, origin=origin,
                                         location=location, overwrite=overwrite)

    '''
    Info Retrieval
    '''
    def _find_exact_cf(self, qq, fb, cx, origin):
        """
        The purpose of this function is to retrieve an exact CF if one exists.
        WHY does this not take rq into account??

        origin used in subclasses
        :param qq:
        :param fb:
        :param cx:
        :param origin:
        :return:
        """
        try:
            clookup = self._qlookup(qq, fb)
        except NoFQEntry:
            return None
        if cx in clookup:
            return clookup[cx]
        return None

    def get_canonical(self, quantity):
        try:
            return self._canonical_q(quantity)
        except KeyError:
            raise EntityNotFound(quantity)

    def _canonical_q(self, quantity):
        """
        override here
        :param quantity: a quantity entity or descriptor
        :return: a canonical quantity or raise KeyError
        """
        try:
            return self._qm[quantity]
        except KeyError:
            if isinstance(quantity, str):
                raise
            if hasattr(quantity, 'link'):
                try:
                    return self._qm[quantity.link]
                except KeyError:
                    if hasattr(quantity, 'quantity_terms'):
                        return self._qm.find_matching_quantity(quantity)  # or else KeyError
            return self._qm[str(quantity)]  # or else KeyError

    def _canonical_q_ref(self, quantity):
        return self._canonical_q(quantity).external_ref

    def _qlookup(self, qq, fb):
        """
        Returns a mapping from context to CF
        :param qq: a canonical quantity
        :param fb: a canonical flowable
        :return:
        """
        try:
            return self._q_dict[qq][fb]
        except KeyError:
            raise NoFQEntry

    def _qaccess(self, qq, fb=None):
        """
        creates an entry if none exists
        :param qq: a canonical quantity
        :param fb [None]: a canonical flowable. If None, return the quantity dict
        :return:
        """
        if qq in self._q_dict:
            qd = self._q_dict[qq]
        else:
            qd = dict()
            self._q_dict[qq] = qd
        if fb is None:
            return qd
        elif fb in qd:
            return qd[fb]
        else:
            cl = self._cl_typ()
            qd[fb] = cl
            return cl

    @staticmethod
    def _store_cf(cl, context, new_cf):
        """
        Assigns the cf to the mapping; does subclass-specific collision checking
        :param cl:
        :param new_cf:
        :return:
        """
        if context in cl:
            raise FactorConflict
        cl[context] = new_cf

    def _qassign(self, qq, fb, new_cf, context=None):
        """
        Assigns the new_cf to the canonical quantity and flowable, taking context and origin from new_cf
        :param qq: a canonical quantity
        :param fb: a canonical flowable
        :param new_cf: a characterization, having a canonical context
        :return:
        """
        if context is None:
            context = new_cf.context
        cl = self._qaccess(qq, fb)
        self._store_cf(cl, context, new_cf)
        self._fq_map[fb].add(qq)

    def _factors_for_flowable(self, fb, qq, cx, **kwargs):
        """
        detach lookup for cleanness. canonical everything
        :param fb:
        :param qq:
        :param cx:
        :param kwargs: used in subclasses
        :return:
        """
        try:
            cl = self._qlookup(qq, fb)
        except NoFQEntry:
            return
        if cx is None:
            for v in cl.values():
                yield v
        else:
            if cx in cl:
                yield cl[cx]

    def factors_for_flowable(self, flowable, quantity=None, context=None, **kwargs):
        """
        This is the method that actually performs the lookup.  Other methods are wrappers for this.

        If context is None, this is "unspecified". It matches all CFs regardless of context.

        Core to this is getting a canonical context, which is done by __getitem__.  In TermManager, this returns
        None for contexts that are not known
        :param flowable: a string
        :param quantity: a quantity known to the quantity manager
        :param context: [None] default provide all contexts; must explicitly provide 'none' to filter by null context
        :return:
        """
        try:
            fb = self._fm[flowable]
            cx = self[context]
        except KeyError:
            return
        if quantity is None:
            for qq in self._fq_map[fb]:
                for cf in self._factors_for_flowable(fb, qq, cx, **kwargs):
                    yield cf
        else:
            qq = self._canonical_q(quantity)
            for cf in self._factors_for_flowable(fb, qq, cx, **kwargs):
                yield cf

    def factors_for_quantity(self, quantity, flowable=None, context=None, **kwargs):
        """
        param dist [0] only used if compartment is specified. by default report only exact matches.

        :param quantity:
        :param flowable:
        :param context:
        :return:
        """
        if flowable is not None:
            for k in self.factors_for_flowable(flowable, quantity=quantity, context=context, **kwargs):
                yield k
        else:
            qq = self._canonical_q(quantity)
            for f in self._qaccess(qq).keys():
                for k in self.factors_for_flowable(f, quantity=qq, context=context, **kwargs):
                    yield k

    def get_flowable(self, term, strict=True):
        """
        Input is a Flow or str
        :param term:
        :param strict: [True] if the input corresponds to more than one flowable and strict is True, raise FlowableConflict
        :return:
        """
        if hasattr(term, 'synonyms'):
            terms = list(term.synonyms)
        elif hasattr(term, 'name'):
            terms = [term.name]
        else:
            terms = [str(term)]
        result_set = set()
        for t in terms:
            try:
                result_set.add(self._fm[t])
            except KeyError:
                continue
        if len(result_set) == 1:
            return result_set.pop()
        elif len(result_set) > 1:
            if strict:
                raise FlowableConflict(result_set)
            else:
                return result_set.pop()
        raise KeyError(term)

    def flowables(self, search=None, origin=None, quantity=None):
        """
        :param origin: used in subclass
        :param search:
        :param quantity:
        :return:
        """
        if quantity is None:
            qq = None
        else:
            qq = self.get_canonical(quantity)
        if search is None:
            if quantity is None:
                for fb in self._fm.objects:
                    yield fb
            else:
                for fb in self._q_dict[qq].keys():
                    yield fb
        else:
            for fb in self._fm.objects_with_string(search):
                if qq is None:
                    yield fb
                else:
                    if fb in self._q_dict[qq]:
                        yield fb

    def unmatched_flowables(self, flowables):
        """
        Given an iterable of flowable strings, return a list of entries that were not recognized as synonyms to known
        flowables
        :param flowables:
        :return:
        """
        unknown = []
        for fb in flowables:
            try:
                self._fm[fb]
            except KeyError:
                unknown.append(fb)
        return unknown

    def synonyms(self, term):
        """
        Search for synonyms, first in contexts, then flowables, then quantities.
        The somewhat awkward structure here is because of the dynamics of returning generators-- using
        try: return self._cm.synonyms(term) except KeyError: ... the KeyError was not getting caught because the
        generator was already returned before iterating.

        contexts are searched first, then quantities, then flowables
        :param term:
        :return:
        """
        try:
            obj = self._cm[term]  # we need this to trip the KeyError if it's not found
            it = self._cm.synonyms(term)
        except KeyError:
            try:
                obj = self._qm[term]  # we need this to trip the KeyError if it's not found
                it = self._qm.synonyms(term)
            except KeyError:
                try:
                    obj = self._fm[term]  # we need this to trip the KeyError if it's not found
                    it = self._fm.synonyms(term)
                except KeyError:
                    it = ()
        for k in it:
            yield k

    def contexts(self, search=None, origin=None):
        for cx in self._cm.objects:
            if origin is not None:
                if not cx.has_origin(origin):
                    continue
            if search is None:
                yield cx
            else:
                if cx.contains_string(search):
                    yield cx

    def quantities(self, search=None, origin=None):
        for q in self._qm.entries:
            if origin is not None:
                if not q.object.origin.startswith(origin):
                    continue
            if search is None:
                yield q.object
            else:
                if q.contains_string(search, ignore_case=True):
                    yield q.object

    '''
    De/Serialization
    
    Strategy here is that term manager serializations should be totally self-contained and also minimal. 
    
    Well, not totally self-contained, since quantities themselves are not going to be messed with.
    
    So aside from quantities, which are not going to be messed with, the term manager serialization should be specified
    by quantity. The routine must determine, for all query quantities listed:
     (a) the set of reference quantities encountered
     (b) the set of flowables encountered
     (c) the set of contexts encountered
    The reference and query quantities must all be serialized per normal, with others omitted.  The term manager 
    section should include flowables, contexts, and characterizations, but only the minimal ones required to cover the
    specified quantities.
    
    The serialized CFs should include ONLY CFs whose QUANTITY is owned by the SPECIFIED ORIGIN.
    
    Then de-serialization is straightforward: after quantities are loaded, simply apply all flowables, contexts, and
    then characterizations- flows not required!
    '''
    def _serialize_qdict(self, quantity, values=False, origin=None):
        _ql = self._qaccess(quantity)
        d = {}
        for fb, cl in _ql.items():
            if origin:
                _od = {str(c): cf.serialize(values=values, concise=True) for c, cf in cl.items() if cf.origin == origin}
            else:
                _od = {str(c): cf.serialize(values=values, concise=True) for c, cf in cl.items()}
            if len(_od) > 0:
                d[str(fb)] = _od
        return d

    def _serialize_factors(self, origin, *quantities, values=False):
        """

        in LciaEngine We [may] need a way to record the original flowable + context names, so that origin-specific
        serialization stays closed.
        :param origin: this is NEVER called with origin=None (i.e. ONLY the single-origin case actually occurs)
        :param quantities:
        :param values:
        :return:
        """
        if len(quantities) == 0:
            qqs = self._q_dict.keys()
        else:
            qqs = [self._canonical_q(q) for q in quantities]
        j = dict()
        for q in qqs:
            if origin is not None and q.origin != origin:
                # serialize CFs belonging to this origin from remote quantities
                _sq = self._serialize_qdict(q, values=values, origin=origin)
            else:
                # serialize all CFs for local quantities
                _sq = self._serialize_qdict(q, values=values)
            if len(_sq) > 0:
                j[self._canonical_q_ref(q)] = _sq
        return j

    def serialize(self, origin, *quantities, values=False):
        """

        :param origin: CFs are limited to: (for quantities belonging to origin) all CFs (for other quantities) only
         CFs that match origin. If origin is None, all quantities are serialized with all CFs
        :param quantities:
        :param values:
        :return: 3-tuple:
         - serialized term manager as dict,
         - set of query quantity external refs,
         - set of reference quantity uuids
           == not sure WHY UUIDs rather than external refs, but that is what characterizations.py gives us
        """
        qqs = set()  # query quantities to serialize
        rqs = set()  # reference quantities to serialize
        fbs = set()  # flowables to serialize
        cxs = set()  # contexts to serialize

        # serialize factors themselves
        j = {'Characterizations': self._serialize_factors(origin, *quantities, values=values)}

        # identify terms + qs
        for qq, factors in j['Characterizations'].items():
            qqs.add(qq)
            for fb, char in factors.items():
                fbs.add(fb)
                for cx, cf in char.items():
                    for c in self._context_and_parents(cx):
                        cxs.add(str(c))
                    try:
                        rqs.add(cf['ref_quantity'])
                    except KeyError:
                        print('%s\n%s' % (cx, cf))
                        raise

        # add flowables and contexts
        j.update(self._fm.serialize(obj for obj in self._fm.entries if str(obj) in fbs))
        j.update(self._cm.serialize(obj for obj in self._cm.entries if str(obj) in cxs))

        return j, qqs, rqs

    def add_from_json(self, j, q_map, origin=None):
        """

        :param j:
        :param q_map: a dict whose keys are external_refs and uuids, and whose values are quantities
        :param origin:
        :return:
        """
        if 'SynonymSets' in j and 'Flowables' not in j:
            j['Flowables'] = j.pop('SynonymSets')

        self._cm.load_dict(j)  # automatically pulls out 'Compartments'
        # self._fm.load_dict(j)
        for flowable in j['Flowables']:
            syns = flowable.pop('synonyms', [])
            try:
                self.add_terms('flowable', flowable['name'], *syns)
            except MergeError:
                self.merge_flowables(flowable['name'], *syns)
        for f in self._fm.objects:
            if f not in self._fq_map:
                self._fq_map[f] = set()
        self._add_from_json(j['Characterizations'], q_map, origin)

    def _add_from_json(self, j, q_map, origin):
        """
        Argument: the contents of archive['characterizations'], which looks like this (general case):
        'termManager': {
          'SynonymSets': [ {flowable}..],
          'Compartments': [ {context}..],
          'Characterizations': {
          query_quantity.external_ref: {
            flowable: {
              context: {
                origin: {
                  ref_quantity: xxx,
                  value: {
                    locale: val
                  }
                }
              }
            }
          }
        }

        (single origin case):
        'characterizations': {
          'SynonymSets': [ {flowable}..],
          'Compartments': [ {context}..],
          query_quantity.external_ref: {
            flowable: {
              context: {
                ref_quantity: xxx,
                value: {
                  locale: val
                }
              }
            }
          }
        }

        :param j:
        :return:
        """
        for query_ext_ref, fbs in j.items():
            try:
                query_q = q_map[query_ext_ref]
            except KeyError:
                query_q = self._canonical_q(query_ext_ref)
            for fb, cxs in fbs.items():
                if origin is None:
                    for cx, cfs in cxs.items():
                        for org, spec in cfs.items():
                            try:
                                ref_q = q_map[spec['ref_quantity']]
                            except KeyError:
                                ref_q = self._canonical_q(spec['ref_quantity'])
                            self.add_characterization(fb, ref_q, query_q, spec['value'], context=cx,
                                                      origin=org)
                else:
                    for cx, spec in cxs.items():
                        try:
                            ref_q = q_map[spec['ref_quantity']]
                        except KeyError:
                            ref_q = self._canonical_q(spec['ref_quantity'])
                        self.add_characterization(fb, ref_q, query_q, spec['value'], context=cx,
                                                  origin=origin)
