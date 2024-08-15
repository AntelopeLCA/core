from antelope import NoReference, EntityNotFound

from .entity_store import EntityStore, EntityExists, uuid_regex
from .basic_archive import BasicArchive, BASIC_ENTITY_TYPES, InterfaceError, ArchiveError
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
        self._check = defaultdict(list)
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
                            self._check['terminated'].append(x)
                        except NoReference:
                            self._check['missing'].append(x)
                    elif x.is_elementary:
                        self._check['elementary'].append(x)
                    elif x.type == 'context':
                        tg = list(query.targets(x.flow, direction=x.direction))
                        if len(tg) == 0:
                            self._check['cutoff'].append(x)
                        elif len(tg) > 1:
                            self._check['ambiguous'].append(x)
                        else:
                            self._check['terminated'].append(x)
                    else:
                        self._check[x.type].append(x)
        self.show()

    def ambiguous_flows(self):
        for t in set(k.flow for k in self._check['ambiguous']):
            yield t

    def show(self):
        print('%d processes\n%d reference exchanges\n%d dependent exchanges' % (self._p, self._rx, self._x))
        ks = list(self._check.keys())
        for k in ('terminated', 'cutoff', 'elementary', 'self'):
            if k in ks:
                v = self._check[k]
                ks.remove(k)
            else:
                v = []
            print('%s: %d exchanges' % (k, len(v)))
        print('')
        for k in ks:
            v = self._check[k]
            print('%s: %d exchanges' % (k, len(v)))

    def exchanges(self, key):
        return self._check[key]
