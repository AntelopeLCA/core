"""
Contexts in this sense are environmental compartments, except they have the added capability to keep lists of origins.

Edelen and Ingwersen et al 2017:
"Recommendations: ...setting an exclusive or inclusive nomenclature for flow context information that includes
directionality and environmental compartment information."

In the antelope architecture, there are two different objectives for handling contexts as-presented by the data source.

 In the default case, for every static resource or stand-alone archive a "TermManager" is created which is captive to
 the archive.  The role of this class is to collect information from the data source in as close to its native
 presentation as possible. This creates an "inclusive" nomenclature for the source.

 In the Catalog case, catalog's local quantity DB is an LciaEngine, which is also shared among all foreground
 resources.  In this case the objective is to match a given context to the existing (exclusive) nomenclature built-in
 to the LciaEngine, so that contexts are guaranteed to coincide during LCIA.

In order to accomplish this, the native add_context() method needs to be expansive, fault tolerant, and widely accepting
of diverse inputs, whereas find_matching_context() needs to be more discerning and rigorous.  Thus the former accepts
a tuple of string terms or a context, but the latter requires a context.  find_matching_context searches first bottom-up
then top-down to look for matches.  It requires an input context to have an origin already specified, so that the
resources can assign "hints" that map local terms to canonical contexts on an origin-specific basis.

Note that a "matching" generally requires an exact (case-insensitive) match to an entry in the contexts database.
Curation of the synonym set is required.

Because contexts must be directional, some terms are protected as ambiguous: "air", "water", and "ground" should be
avoided in favor of explicit "from air" or "to air" or synonyms.

The NullContext is a singleton defined in this file that is meant to imply no specific context.  It is interpreted in
two different ways:
 - on the characterization side, NullContext indicates the characterization has no specific context, and thus applies to
   all contexts, as long as a more applicable characterization is not found.
 - on the query side, NullContext indicates that a context was specified but no match was found.  Queries with
   NullContext should not match any existing characterizations (except NullContext itself).

The NullContext should be returned by the context manager
"""

from synonym_dict import Compartment, CompartmentManager, NonSpecificCompartment, TermExists, MergeError
from synonym_dict.compartments.compartment import InvalidSubCompartment
from antelope import valid_sense

SINKS = {'emission', 'emissions'}
SOURCES = {'resource', 'resources'}

ELEMENTARY = {'elementary flows'} | SINKS | SOURCES

PROTECTED = ('air', 'water', 'ground')


class ProtectedTerm(Exception):
    """
    currently triggered by the stale contexts file-- leading to an error in which
    "emissions;to water;low population density, long-term" and
    "emissions;to air;low population density, long-term"
    are synonyms in basic TermManagers. Should not present problems in LciaEngines, but the specific case is not tested.

    Solution is to patch the local-contexts.json file to exclude this redundant term for water.
    Justification: the canonical contexts should be well-behaved and not contain any inherent conflicts. It is
    a nomenclature problem for the two contexts to have the same name.
    """
    pass


class InconsistentSense(Exception):
    pass


class FrozenElementary(Exception):
    """
    top-level elementary contexts may not be assigned non-elementary parents
    """
    pass


class ImmutableContextName(Exception):
    """
    A context's hash comes from its name and thus the name may not be changed.
    """
    pass


def _dir_mod(arg, sense):
    if arg.lower() in PROTECTED:
        if sense is None:
            raise ProtectedTerm(arg)
        mod = {'Source': 'from ', 'Sink': 'to '}[sense]
        arg = '%s%s' % (mod, arg)
    return arg


class Context(Compartment):
    """
    If 'resources' or 'emissions' match any terms in a compartment, it is considered 'elementary', along with all its
    subcompartments.

    A context has a natural directional "sense", which is either 'Source', 'Sink', or None.  A Source context
    generates flows which may be inputs to the activity; a Sink context absorbs flows which are output from the
    activity.

    If a context has a parent, it inherits the sense of the parent- specifying the opposite sense will raise
    an error.
    """
    _first_origin = None
    entity_type = 'context'
    _elem = None

    def set_name(self, name):
        raise ImmutableContextName('May not change canonical name for contexts')

    def __iter__(self):
        for k in self.as_list():
            yield k

    def __hash__(self):
        return hash(self._id)  # don't mess with the hash

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        try:
            return tuple(self) == tuple(other)
        except TypeError:
            return False

    def __len__(self):
        if self.parent is None:
            return 1
        return self.parent.__len__() + 1

    @property
    def origin(self):
        return self._first_origin

    @staticmethod
    def validate():
        """
        Need this for termination checking
        :return:
        """
        return True

    @property
    def fullname(self):
        if self._first_origin is None:
            return self.name
        return '%s:%s' % (self.origin, self.name)

    @property
    def origins(self):
        for o in self._origins:
            yield o

    def __init__(self, *args, sense=None, **kwargs):
        self._sense = None
        super(Context, self).__init__(*args, **kwargs)
        self._origins = set()
        if sense is not None:
            self.sense = sense

    @property
    def terms(self):
        j = None
        if self._first_origin is not None:
            yield self.fullname
            j = self.fullname
        for t in super(Context, self).terms:
            if t != j:
                yield t

    @property
    def sense(self):
        if self._sense is not None:
            return self._sense
        elif self.parent is None:
            return self._sense
        return self.parent.sense

    def _check_sense(self, v_sense):
        if self.sense is not None and self.sense != v_sense:
            raise InconsistentSense('%s: %s conflicts with self.sense=%s' % (self.name, v_sense, self.sense))
        for sub in self.subcompartments:
            sub._check_sense(v_sense)

    @sense.setter
    def sense(self, value):
        sense = valid_sense(value)
        self._check_sense(sense)
        self._sense = sense
        #for sub in self.subcompartments:  # do not set subcompartment senses- only set determinative compartments
        #    sub.sense = sense

    @property
    def parent(self):  # duplicating here to override setter
        return self._parent

    @parent.setter
    def parent(self, parent):
        if self._parent is None:
            if parent:
                if self.elementary and not parent.elementary:
                    raise FrozenElementary
            self._elem = None  # reset
        if parent:
            if parent.sense is not None:
                self._check_sense(parent.sense)
        # print('Setting %s.parent <- %s' % (self, parent))
        if str(self) in PROTECTED and str(parent) in PROTECTED and str(self) != str(parent):
            raise ProtectedTerm
        Compartment.parent.fset(self, parent)

    def _is_elem(self):
        for t in self.terms:
            if t.strip().lower() in ELEMENTARY:
                return True
        return False

    @property
    def elementary(self):
        """
        A context's "elementary-ness" is determined by the top-most parent.  Everything inherits from above.
        If the (lower cased) parent context is found in ELEMENTARY, it's elementary.

        The only way to make a context elementary is to name it such, or give it an elementary parent.
        :return:
        """
        if self.parent is None:
            if self._elem is None:
                self._elem = self._is_elem()
            return self._elem
        return self.parent.elementary

    @property
    def external_ref(self):
        return self.name

    def add_origin(self, origin):
        if self._first_origin is None:
            self._first_origin = origin
        self._origins.add(origin)
        if self.parent is not None:  # why would we recurse on this?
            # ans: if our parent is also missing an origin
            if self.parent.origin is None:
                self.parent.add_origin(origin)

    def has_origin(self, origin, strict=False):
        try:
            if strict:
                next(x for x in self._origins if x == origin)
            else:
                next(x for x in self._origins if x.startswith(origin))
        except StopIteration:
            return False
        return True

    def serialize(self):
        d = super(Context, self).serialize()
        if self.parent is None:
            if self.sense is not None:
                d['sense'] = self.sense
        return d

    def __repr__(self):
        return '<Context(%s)>' % ';'.join(self.as_list())


NullContext = Context.null()


class ContextManager(CompartmentManager):
    _entry_group = 'Compartments'  # we keep this so as to access compartment-compatible serializations
    _syn_type = Context
    _ignore_case = True

    _null_entry = NullContext

    def __init__(self, source_file=None):
        super(ContextManager, self).__init__()

        self.new_entry('Resources', *SOURCES, sense='source')
        self.new_entry('Emissions', *SINKS, sense='sink')
        self.load(source_file)

    def add_context_hint(self, origin, term, canonical):
        """
        Method for linking foreign context names to canonical contexts
        :param origin: origin of foreign context
        :param term: foreign context name
        :param canonical: recognized synonym for canonical context that should match the foreign one
        :return:
        """
        c = self[canonical]
        if c is None:
            raise ValueError('Unrecognized canonical context %s' % c)
        syn = '%s:%s' % (origin, term)
        try:
            self.add_synonym(c, syn)
        except TermExists:
            self.del_term(syn)
            self.add_synonym(c, syn)

    def new_entry(self, *args, parent=None, **kwargs):
        args = tuple(filter(None, args))
        if parent is not None:
            if not isinstance(parent, Compartment):
                parent = self._d[parent]
            if parent.sense is not None:
                args = tuple(_dir_mod(arg, parent.sense) for arg in args)
        return super(ContextManager, self).new_entry(*args, parent=parent, **kwargs)

    def _gen_matching_entries(self, cx, sense):
        for t in cx.terms:
            mt = _dir_mod(t, sense)  # check for protected terms
            if t in self._d:
                yield self._d[t]
            elif mt in self._d:
                yield self._d[mt]

    def _merge(self, existing_entry, ent):
        """
        Need to check lineage. We adopt the rule: merge is acceptable if both entries have the same top-level
        compartment or if ent has no parent.  existing entry will obviously be dominant, but any sense specification
        will overrule a 'None' sense in either existing or new.
        :param existing_entry:
        :param ent:
        :return:
        """
        if ent.sense is not None:
            existing_entry.sense = ent.sense  # this is essentially an assert w/raises InconsistentSense

        super(ContextManager, self)._merge(existing_entry, ent)  # yowza, moronic bug

    def add_compartments(self, comps, conflict=None):
        if conflict is not None:
            return super(ContextManager, self).add_compartments(comps, conflict=conflict)
        else:
            try:
                return super(ContextManager, self).add_compartments(comps, conflict='attach')
            except (FrozenElementary, InvalidSubCompartment, InconsistentSense):
                return super(ContextManager, self).add_compartments(comps, conflict='rename')
            """
            except ProtectedTerm:
                print('Protected Term! %s' % comps)
                return super(ContextManager, self).add_compartments(comps, conflict='rename')
            """

    '''
    def add_lineage(self, lineage, parent=None):
        """
        Create a set of local contexts, beginning with parent, that replicate those in lineage
        :param lineage:
        :param parent: [None] parent of lineage
        :return: the last created context
        """
        if parent:
            if parent not in self._l:
                raise ValueError('Parent is not known locally')
        new = None
        for lx in lineage:
            new = self.new_entry(*lx.terms, parent=parent)
            if lx.origin is not None:
                self.add_synonym(lx.fullname, new)
            parent = new
        return new
    '''

    def _add_but_not_protected(self, entry, context):
        self.add_synonym(entry, context.fullname)
        if context.name.lower() not in PROTECTED:
            try:
                self.add_synonym(entry, context.name)
            except TermExists:
                pass

    def find_matching_context(self, context):
        """
        A complicated function to both (1) retrieve the best / most-specific "canonical" context for a given "foreign"
        (i.e. incoming) context and also (2) add the full semantic content of the foreign context to the canonical
        context manager.

        (1) is accomplished by a heuristic both-first search, first looking at the foreign context from most-specific
        to least-specific and trying to find a match.  if no match is found, we go back from least-to-most specific,
        using a slightly broader set of possible matches including heuristic names derived from contextual senses.
        If no match at all is found, then NullContext is returned -- the foreign context matches no canonical context.

        (2) If a canonical context is found, then all non-matching contexts from least-specific to most-specific,
        are mapped onto existing sub-contexts of the match, or if none is found, collapsed into the match.
        :param context:
        :return:
        """
        if context.name == context.fullname:
            raise AttributeError('Context origin must be specified')
        current = NullContext  # current = deepest local match

        # first, look for stored auto_names or context_hints to find an anchor point:
        missing = []
        for cx in context.seq[::-1]:
            if cx.fullname in self:
                current = self[cx.fullname]
                break
            else:
                missing = [cx] + missing  # prepend

        # if current is None when we get here, then we haven't found anything, so start over and hunt from bottom up
        while len(missing) > 0:
            this = missing.pop(0)  # this = active foreign match - least-specific first
            if current is NullContext:
                """
                In this block, we still haven't found a match. So we broaden our criteria.
                """
                try:
                    current = next(self._gen_matching_entries(this, None))
                except StopIteration:
                    continue
                except ProtectedTerm:
                    continue
                if current is NullContext:
                    continue
                # we found a match-- add our active foreign match to the current match
                self._add_but_not_protected(current, this)
            else:
                """
                In this block, we have an active canonical match. We check our (more-specific) foreign subcontext
                to see if it matches any of our current context's subcompartments
                """
                try:
                    nxt = next(k for k in self._gen_matching_entries(this, current.sense)
                               if k.is_subcompartment(current))
                    self._add_but_not_protected(nxt, this)
                    current = nxt
                except StopIteration:
                    if 0:  # old
                        self._add_but_not_protected(current, this)  # old
                    else:  # new
                        """
                        DWR! Changing core behavior! We have an existing match but our foreign context is more specific
                        so-- add a new canonical context
                        
                        (old behavior was to smush all foreign subcontexts together into the canonical context)
                        """
                        try:
                            nxt = self.new_entry(this.name, parent=current, sense=this.sense)
                        except (MergeError, InconsistentSense):
                            # for some reason USLCI FY21 has a "from ground, subterranean" category??
                            # and USEEIO has "from water; subterranean; fresh water body" and "to water; fresh water body"
                            merge_name = ', '.join([current.name, this.name])
                            nxt = self.new_entry(merge_name, parent=current, sense=this.sense)
                            # this fix is awfully special-purpose but hey- things are mostly working
                        nxt.add_origin(this.origin)
                        self._add_but_not_protected(nxt, this)
                        current = nxt
        if current is not NullContext:
            self._add_but_not_protected(current, context)
        return current

    def __getitem__(self, item):
        try:
            if isinstance(item, Context):
                try:
                    return super(ContextManager, self).__getitem__(item.fullname)
                except (NonSpecificCompartment, KeyError):
                    pass
            return super(ContextManager, self).__getitem__(item)
        except NonSpecificCompartment:
            return NullContext
