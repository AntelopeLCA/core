from antelope import NoReference, EntityNotFound

from .entity_store import EntityStore, EntityExists, uuid_regex
from .basic_archive import BasicArchive, BASIC_ENTITY_TYPES, InterfaceError, ArchiveError, LD_CONTEXT, ContextCollision
from .archive_index import index_archive, BasicIndex, LcIndex
from .term_manager import TermManager
from .lc_archive import LcArchive, LC_ENTITY_TYPES
from ..from_json import from_json

from ..entities.flows import new_flow
from ..implementations.quantity import UnknownRefQuantity

from pathlib import Path
from collections import defaultdict

# import pkgutil

REF_QTYS = str(Path(__file__).parent / 'data' / 'elcd_reference_quantities.json')


class Qdb(BasicArchive):
    """
    A simple archive that just contains the 25-odd reference (non-LCIA) quantities of the ELCD database circa v3.2
    """
    @classmethod
    def new(cls, ref='local.qdb'):
        """
        Create a Quantity database containing the ILCD reference quantities.  Specify a ref if desired.
        :param ref: ['local.qdb']
        """
        return cls.from_file(REF_QTYS, ref=ref, static=True)

    def _fetch(self, entity, **kwargs):
        return self.__getitem__(entity)

    def _load_all(self, **kwargs):
        self.load_from_dict(from_json(self.source))

    def new_flow(self, name, ref_quantity=None, **kwargs):
        """
        :param name:
        :param ref_quantity: defaults to "Number of items"
        :param kwargs:
        :return:
        """

        if ref_quantity is None:
            ref_quantity = 'Number of items'
        try:
            ref_q = self.tm.get_canonical(ref_quantity)
        except EntityNotFound:
            raise UnknownRefQuantity(ref_quantity)
        f = new_flow(name, ref_q, **kwargs)
        self.add_entity_and_children(f)
        return self.get(f.external_ref)


def update_archive(archive, json_file):
    archive.load_from_dict(from_json(json_file), jsonfile=json_file)


# find antelope providers
init_map = {
    'basicarchive': BasicArchive,
    'basicindex': BasicIndex,
    'lcarchive': LcArchive,
    'lcindex': LcIndex
}


def archive_factory(ds_type):
    """
    Returns an archive class
    :param ds_type:
    :return:
    """
    dsl = ds_type.lower()
    if dsl in init_map:
        return init_map[dsl]
    raise ArchiveError('No provider found for %s' % ds_type)


def archive_from_json(fname, factory=archive_factory, catalog=None, **archive_kwargs):
    """
    :param fname: JSON filename
    :param factory: function returning a class
    :param catalog: [None] necessary to retrieve upstream archives, if specified
    :return: an ArchiveInterface
    """
    j = from_json(fname)

    if 'upstreamReference' in j or catalog is not None:
        print('**Upstream reference encountered: %s' % j['upstreamReference'])
        print('**XX Upstream is gone; catalog argument is deprecated\n')
    cls = factory(j.pop('dataSourceType', 'LcArchive'))

    return cls.from_already_open_file(j, fname, quiet=True, **archive_kwargs)


def create_archive(source, ds_type, factory=archive_factory, **kwargs):
    """
    Create an archive from a source and type specification.
    :param source:
    :param ds_type:
    :param factory: override archive factory with fancier version
    :param kwargs:
    :return:
    """
    if ds_type.lower() == 'json':
        a = archive_from_json(source, factory=factory, **kwargs)
    else:
        cls = factory(ds_type)
        a = cls(source, **kwargs)
    return a


class CheckTerms(object):
    """
    A utility for reviewing the integrity of exchanges in an archive
    """
    def __init__(self, query):
        """
        Analyzes the linking characteristics of a data source. Requires exchange and index interfaces.
        :param query:
        """
        self._q = query
        self._check = defaultdict(list)
        self._broken = dict()
        self._ambig = dict()
        self._p = 0
        self._rx = 0
        self._x = 0

        for p in query.processes():
            self._p += 1
            for rx in p.references():
                self._rx += 1
                for x in p.inventory(rx):
                    self._x += 1
                    if x.type == 'node':
                        try:
                            query.get(x.termination).reference(x.flow)
                            self._check['anchored'].append(x)
                        except NoReference:
                            # determine if broken flows are also ambiguous
                            ng = len(list(t for t in query.targets(x.flow, direction=x.direction) if t != x.process))
                            self._check['broken'].append(x)
                            self._broken[x] = ng
                    elif x.type == 'context':
                        if x.is_elementary:
                            self._check['elementary'].append(x)
                        else:  # unlinked intermediate flows with context specified
                            tg = list(t for t in query.targets(x.flow, direction=x.direction) if t != x.process)
                            if len(tg) == 0:
                                self._check['cutoff'].append(x)
                            elif len(tg) > 1:
                                self._ambig[x] = len(tg)
                                self._check['ambiguous'].append(x)
                            else:
                                self._check['anchored'].append(x)
                    else:
                        self._check[x.type].append(x)
        self.show()

    @property
    def ambiguous_flows(self):
        """
        Generates a list of flows for which the target is ambiguous
        :return:
        """
        flows = set()
        for t in self._check['ambiguous']:
            if t.flow not in flows:
                flows.add(t.flow)
                yield t.flow
        for t in self._check['broken']:
            if self._broken[t] > 1:
                if t.flow not in flows:
                    flows.add(t.flow)
                    yield t.flow

    @property
    def broken_anchors(self):
        """
        Generates a list of exchanges with faulty anchors (the targeted dataset does not list the flow as a reference)
        :return:
        """
        for t in sorted(self._check['broken'], key=lambda x: self._broken[x]):
            yield t

    def show(self):
        print('%d processes\n%d reference exchanges\n%d dependent exchanges:' % (self._p, self._rx, self._x))
        ks = list(self._check.keys())
        for k in ('anchored', 'cutoff', 'elementary', 'self'):
            if k in ks:
                v = self._check[k]
                ks.remove(k)
            else:
                v = []
            print('  %s: %d exchanges' % (k, len(v)))
        print('')
        for k in ks:
            v = self._check[k]
            print('  %s: %d exchanges' % (k, len(v)))

    def _show_bad(self, exchs, counter):
        last = None
        for t in sorted(exchs, key=lambda x: x.process.external_ref):
            if t.process.external_ref != last:
                if last is not None:
                    print('')
                last = t.process.external_ref
                print('Process: %s' % t.process)
            dirn = {'Input': '<--#',
                    'Output': '==>#'}[t.direction]
            count = counter[t]
            if count > 1:
                bad = '*'
            else:
                bad = ' '
            if t.type == 'node':
                tgt = self._q.get(t.termination).name
            else:
                tgt = '[%s]' % t.termination
            print('%s %s (%d) %s ! %s' % (bad, t.flow.name, count, dirn, tgt))
        if last is None:
            print('No broken exchanges')

    def show_broken(self):
        """
        Print broken links in human-readable form:

        Process: process name:
        ? flow name (valid targets) <---> # ! bad target
        ...

        '?' is either a blank (0 or 1 valid target) or a * (more than 1 valid target)
        :return:
        """
        self._show_bad(self._check['broken'], self._broken)

    def show_ambiguous(self):
        """
        Print ambiguous links in human-readable form:

        Process: process name:
        * flow name (valid targets) <---> # ! [flow context]
        ...

        :return:
        """
        self._show_bad(self._check['ambiguous'], self._ambig)

    def exchanges(self, key):
        return self._check[key]
