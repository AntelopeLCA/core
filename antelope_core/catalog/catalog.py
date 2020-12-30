"""
The LcCatalog provides a semantic interface to a collection of (local and remote) read-only LcArchives, which provide
access to physical data.

It is made up of the following components:

  * built on an LciaEngine
  + local, persistent storage of resources, indexes, cache data + etc
  + A resolver, which translates semantic references into resources.  Input: semantic ref. output: CatalogInterface.
  + an interface generator, which creates archive accessors on demand based on resource information from the resolver
  x An internal cache of entities retrieved, by full reference-- this has been cut

From the catalog_ref file, the catalog should meet the following spec:
          Automatic - entity information
           catalog.query(origin) - returns a query interface
           catalog.lookup(origin, external_ref) - returns the origin of the lowest-priority resource resolving the ref
           catalog.fetch(origin, external_ref) - return a reference to the object that can be queried + handled

          LC Queries:
           see lcatools.interfaces.*

"""

import os
import re
import hashlib
# from collections import defaultdict

from ..archives import InterfaceError
from ..lcia_engine import LciaDb


from antelope import CatalogRef, UnknownOrigin
from ..catalog_query import CatalogQuery, INTERFACE_TYPES, zap_inventory
from .lc_resolver import LcCatalogResolver
from ..lc_resource import LcResource
# from lcatools.flowdb.compartments import REFERENCE_INT  # reference intermediate flows


class DuplicateEntries(Exception):
    pass


class CatalogError(Exception):
    pass


class StaticCatalog(object):

    """
    Provides query-based access to LCI information. The static version is ideal for creating read-only web resources
    from curated LcCatalogs. However, it must already exist. Only an LcCatalog (or subclasses) support de novo
    instantiation.

    A catalog is stored in the local file system and creates and stores resources relative to its root directory.
    Subfolders (all accessors return absolute paths):
    Public subfolders:
     LcCatalog.resource_dir
     LcCatalog.archive_dir

    Public filenames:
     LcCatalog.cache_file(src) returns a sha1 hash of the source filename in the [absolute] cache dir
     LcCatalog.download_file(src) returns a sha1 hash of the source filename in the [absolute] download dir

    Private folders + files:
     LcCatalog._download_dir
     LcCatalog._index_dir
     LcCatalog._index_file(src) returns a sha1 hash of the source filename in the [absolute] index dir
     LcCatalog._cache_dir
     LcCatalog._entity_cache: local entities file in root
     LcCatalog._reference_qtys: reference quantities file in root
     LcCatalog._compartments: local compartments file (outmoded in Context Refactor)


    """
    @property
    def resource_dir(self):
        return os.path.join(self._rootdir, 'resources')

    @property
    def _download_dir(self):
        return os.path.join(self._rootdir, 'downloads')

    @staticmethod
    def _source_hash_file(source):
        """
        Creates a stable filename from a source argument.  The source is the key found in the _archive dict, and
        corresponds to a single physical data source.  The filename is a sha1 hex-digest, .json.gz
        :param source:
        :return:
        """
        h = hashlib.sha1()
        h.update(source.encode('utf-8'))
        return h.hexdigest()

    @property
    def _index_dir(self):
        return os.path.join(self._rootdir, 'index')

    def _index_file(self, source):
        return os.path.join(self._index_dir, self._source_hash_file(source) + '.json.gz')

    @property
    def _cache_dir(self):
        return os.path.join(self._rootdir, 'cache')

    def cache_file(self, source):
        return os.path.join(self._cache_dir, self._source_hash_file(source) + '.json.gz')

    @property
    def archive_dir(self):
        return os.path.join(self._rootdir, 'archives')

    '''
    @property
    def _entity_cache(self):
        return os.path.join(self._rootdir, 'entity_cache.json')
    '''

    @property
    def _reference_qtys(self):
        return os.path.join(self._rootdir, 'reference-quantities.json')

    '''
    @property
    def _compartments(self):
        """
        Deprecated
        :return:
        """
        return os.path.join(self._rootdir, 'local-compartments.json')
    '''

    @property
    def _contexts(self):
        return os.path.join(self._rootdir, 'local-contexts.json')

    @property
    def _flowables(self):
        return os.path.join(self._rootdir, 'local-flowables.json')

    def _localize_source(self, source):
        if source is None:
            return None
        if source.startswith(self._rootdir):
            return re.sub('^%s' % self._rootdir, '$CAT_ROOT', source)
        return source

    def abs_path(self, rel_path):
        if os.path.isabs(rel_path):
            return rel_path
        elif rel_path.startswith('$CAT_ROOT'):
            return re.sub('^\$CAT_ROOT', self.root, rel_path)
        return os.path.join(self.root, rel_path)

    @property
    def root(self):
        return self._rootdir

    def __init__(self, rootdir, strict_clookup=True, **kwargs):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param rootdir: directory storing LcResource files.
        :param strict_clookup: [True] whether to enforce uniqueness on characterization factors (raise an error when a
         non-matching duplicate characterization is encountered). If False, selection among conflicting factors is
         not well defined and may be done interactively or unpredictably
        :param kwargs: passed to Qdb
        """
        self._rootdir = os.path.abspath(rootdir)
        if not os.path.exists(self._rootdir):
            raise FileNotFoundError(self._rootdir)
        self._resolver = LcCatalogResolver(self.resource_dir)

        """
        _archives := source -> archive
        _names :=  ref:interface -> source
        _nicknames := nickname -> source
        """
        self._nicknames = dict()  # keep a collection of shorthands for sources

        self._queries = dict()  # keep a collection of CatalogQuery instances for each origin

        '''
        LCIA: 
        '''
        qdb = LciaDb.new(source=self._reference_qtys, contexts=self._contexts, flowables=self._flowables,
                         strict_clookup=strict_clookup, **kwargs)
        self._qdb = qdb
        res = LcResource.from_archive(qdb, interfaces=('index', 'quantity'), store=False)
        self._resolver.add_resource(res, store=False)

    @property
    def lcia_engine(self):
        return self._qdb.tm

    def register_quantity_ref(self, q_ref):
        print('registering %s' % q_ref.link)
        self._qdb.add(q_ref)

    @property
    def sources(self):
        for k in self._resolver.sources:
            yield k

    @property
    def references(self):
        for ref, ints in self._resolver.references:
            yield ref

    @property
    def interfaces(self):
        for ref, ints in self._resolver.references:
            for i in ints:
                yield ':'.join([ref, i])

    def show_interfaces(self):
        for ref, ints in sorted(self._resolver.references):
            print('%s [%s]' % (ref, ', '.join(ints)))

    '''
    Nicknames
    '''
    @property
    def names(self):
        """
        List known references.
        :return:
        """
        for k, ifaces in self._resolver.references:
            for iface in ifaces:
                yield ':'.join([k, iface])
        for k in self._nicknames.keys():
            yield k

    def add_nickname(self, source, nickname):
        """
        quickly refer to a specific data source already present in the archive
        :param source:
        :param nickname:
        :return:
        """
        if self._resolver.known_source(source):
            self._nicknames[nickname] = source
        else:
            raise KeyError('Source %s not found' % source)


    def has_resource(self, res):
        return self._resolver.has_resource(res)

    '''
    Retrieve resources
    '''
    def _find_single_source(self, origin, interface, source=None, strict=True):
        r = self._resolver.get_resource(ref=origin, iface=interface, source=source, include_internal=False, strict=strict)
        r.check(self)
        return r.source

    def get_resource(self, name, iface=None, source=None, strict=True):
        """
        retrieve a resource by providing enough information to identify it uniquely.  If strict is True (default),
        then parameters are matched exactly and more than one match raises an exception. If strict is False, then
        origins are matched approximately and the first (lowest-priority) match is returned.

        :param name: nickname or origin
        :param iface:
        :param source:
        :param strict:
        :return:
        """
        if name in self._nicknames:
            return self._resolver.get_resource(source=self._nicknames[name], strict=strict)
        iface = zap_inventory(iface, warn=True)  # warn when requesting the wrong interface
        return self._resolver.get_resource(ref=name, iface=iface, source=source, strict=strict)

    def get_archive(self, ref, interface=None, strict=False):
        interface = zap_inventory(interface, warn=True)
        if interface in INTERFACE_TYPES:
            rc = self.get_resource(ref, iface=interface, strict=strict)
        else:
            rc = self.get_resource(ref, strict=strict)
        rc.check(self)
        return rc.archive

    '''
    Main data accessor
    '''
    def _sorted_resources(self, origin, interfaces, strict):
        for res in sorted(self._resolver.resolve(origin, interfaces, strict=strict),
                          key=lambda x: (not (x.is_loaded and x.static), x.priority, x.reference != origin)):
            yield res

    def gen_interfaces(self, origin, itype=None, strict=False):
        """
        Generator of interfaces by spec

        :param origin:
        :param itype: single interface or iterable of interfaces
        :param strict: passed to resolver
        :return:
        """
        if itype is None:
            itype = 'basic'  # fetch, get properties, uuid, reference

        # if itype == 'quantity':
        #    yield self._qdb.make_interface(itype)

        for res in self._sorted_resources(origin, itype, strict):
            res.check(self)
            try:
                yield res.make_interface(itype)
            except InterfaceError:
                continue

        '''
        # no need for this because qdb is (a) listed in the resolver and (b) upstream of everything
        if 'quantity' in itype:
            yield self._qdb  # fallback to our own quantity db for Quantity Interface requests
            '''

    """
    public functions -- should these operate directly on a catalog ref instead? I think so but let's see about usage
    """
    def query(self, origin, strict=False, refresh=False, **kwargs):
        """
        Returns a query using the first interface to match the origin.
        :param origin:
        :param strict: [False] whether the resolver should match the origin exactly, as opposed to returning more highly
         specified matches.  e.g. with strict=False, a request for 'local.traci' could be satisfied by 'local.traci.2.1'
         whereas if strict=True, only a resource matching 'local.traci' exactly will be returned
        :param refresh: [False] by default, the catalog stores a CatalogQuery instance for every requested origin.  With
         refresh=True, any prior instance will be replaced with a fresh one.
        :param kwargs:
        :return:
        """

        next(self._resolver.resolve(origin, strict=strict))

        if refresh or (origin not in self._queries):
            self._queries[origin] = CatalogQuery(origin, catalog=self, **kwargs)
        return self._queries[origin]

    def lookup(self, catalog_ref, keep_properties=False):
        """
        Attempts to return a valid grounded reference matching the one supplied.
        :param catalog_ref:
        :param keep_properties: [False] if True, apply incoming ref's properties to grounded ref, probably with a
        prefix or something.
        :return:
        """
        ref = self.query(catalog_ref.origin).get(catalog_ref.external_ref)
        if keep_properties:
            for k in catalog_ref.properties():
                ref[k] = catalog_ref[k]
        return ref

    '''
    def lookup(self, origin, external_ref=None):
        """
        Attempts to secure an entity
        :param origin:
        :param external_ref:
        :return: The origin of the lowest-priority resource to match the query
        """
        if external_ref is None:
            origin, external_ref = origin.split('/', maxsplit=1)
        for i in self.gen_interfaces(origin):
            if i.lookup(external_ref):
                return i.origin
        for i in self.gen_interfaces('.'.join(['foreground', origin])):
            if i.lookup(external_ref):
                return i.origin
        raise EntityNotFound('%s/%s' % (origin, external_ref))

    def fetch(self, origin, external_ref=None):
        if external_ref is None:
            origin, external_ref = origin.split('/', maxsplit=1)
        org = self.lookup(origin, external_ref)
        return self.query(org).get(external_ref)
    '''

    def catalog_ref(self, origin, external_ref, entity_type=None, **kwargs):
        """
        TODO: make foreground-generated CatalogRefs lazy-loading. This mainly requires removing the expectation of a
        locally-defined reference entity, and properly implementing and using a reference-retrieval process in the
        basic interface.
        :param origin:
        :param external_ref:
        :param entity_type:
        :return:
        """
        try:
            q = self.query(origin)
            ref = q.get(external_ref)
        except UnknownOrigin:
            ref = CatalogRef(origin, external_ref, entity_type=entity_type, **kwargs)
        return ref
