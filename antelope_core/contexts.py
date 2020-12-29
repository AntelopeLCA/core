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

from synonym_dict.example_compartments import Compartment, CompartmentManager
from antelope import valid_sense

ELEMENTARY = {'elementary flows', 'resource', 'emission', 'resources', 'emissions'}

PROTECTED = ('air', 'water', 'ground')


class ProtectedTerm(Exception):
    """
    currently unused
    """
    pass


class InconsistentSense(Exception):
    pass


class FrozenElementary(Exception):
    """
    top-level elementary contexts may not be assigned parents
    """
    pass


def _dir_mod(arg, sense):
    mod = {'Source': 'from ', 'Sink': 'to ', None: ''}[sense]
    if arg.lower() in PROTECTED:
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
        super(Context, self).__init__(*args, **kwargs)
        self._origins = set()
        self._sense = None
        if sense is not None:
            self.sense = sense

    @property
    def terms(self):
        if self._first_origin is not None:
            yield self.fullname
        for t in super(Context, self).terms:
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
            raise InconsistentSense('Value %s conflicts with current v_sense %s' % (v_sense, self.sense))
        for sub in self.subcompartments:
            sub._check_sense(v_sense)

    @sense.setter
    def sense(self, value):
        sense = valid_sense(value)
        self._check_sense(sense)
        self._sense = sense
        for sub in self.subcompartments:
            sub.sense = sense

    @property
    def parent(self):  # duplicating here to override setter
        return self._parent

    @parent.setter
    def parent(self, parent):
        if self._parent is not None:
            self._parent.deregister_subcompartment(self)
        else:
            if self.elementary and not parent.elementary:
                raise FrozenElementary
        self._parent = parent
        if parent is not None:
            parent.register_subcompartment(self)

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
    def seq(self):
        if self.parent is None:
            return [self]
        return self.parent.seq + [self]

    @property
    def external_ref(self):
        return self.name

    def add_origin(self, origin):
        if self._first_origin is None:
            self._first_origin = origin
        self._origins.add(origin)
        if self.parent is not None:
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

        self.new_entry('Resources', sense='source')
        self.new_entry('Emissions', sense='sink')
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
            mt = _dir_mod(t, sense)
            if mt in self._d:
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

        super(CompartmentManager, self)._merge(existing_entry, ent)

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

    def find_matching_context(self, context):
        if context.name == context.fullname:
            raise AttributeError('Context origin must be specified')
        current = None  # current = deepest local match

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
            this = missing.pop(0)  # this = active foreign match
            if current is None:
                try:
                    current = next(self._gen_matching_entries(this, None))
                except StopIteration:
                    continue
                self.add_synonym(current, this.fullname)
            else:
                try:
                    nxt = next(k for k in self._gen_matching_entries(this, current.sense)
                               if k.is_subcompartment(current))
                    self.add_synonym(nxt, this.fullname)
                    current = nxt
                except StopIteration:
                    self.add_synonym(current, this.fullname)
        if current is not None:
            self.add_synonym(current, context)
        return current
