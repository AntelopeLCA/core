"""
The LcCatalog provides a semantic interface to a collection of (local and remote) read-only LcArchives, which provide
access to physical data.

It is made up of the following components:

  * built on an LciaEngine
  + local, persistent storage of resources, indexes, cache data + etc
  + A resolver, which translates semantic origins into resources.  Input: semantic ref. output: CatalogInterface.
  + an interface generator, which creates archive accessors on demand based on resource information from the resolver

From the catalog_ref file, the catalog should meet the following spec:
          Automatic - entity information
           catalog.query(origin) - returns a query interface
           catalog.lookup(origin, external_ref) - returns the origin of the lowest-priority resource resolving the ref
           catalog.fetch(origin, external_ref) - return a reference to the object that can be queried + handled

          LC Queries:
           see lcatools.interfaces.*

"""

import os
# import re
import hashlib
from collections import defaultdict

from ..archives import InterfaceError, EntityExists
from ..lcia_engine import LciaDb


from antelope import UnknownOrigin, InvalidQuery  # , EntityNotFound
from ..catalog_query import CatalogQuery, INTERFACE_TYPES, zap_inventory
from .lc_resolver import LcCatalogResolver
from ..lc_resource import LcResource
from ..archives import REF_QTYS
from ..lcia_engine import DEFAULT_CONTEXTS, DEFAULT_FLOWABLES
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
        if self._rootdir is None:
            return None
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

    def check_cache(self, source):
        return os.path.exists(self.cache_file(source))

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
        if self._rootdir is None:
            return REF_QTYS
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
        if self._rootdir is None:
            return DEFAULT_CONTEXTS
        return os.path.join(self._rootdir, 'local-contexts.json')

    @property
    def _flowables(self):
        if self._rootdir is None:
            return DEFAULT_FLOWABLES
        return os.path.join(self._rootdir, 'local-flowables.json')

    def _localize_source(self, source):
        if source is None or self._rootdir is None:
            return None
        if source.startswith(self._rootdir):
            # return re.sub('^%s' % self._rootdir, '$CAT_ROOT', source)
            # Should work on both mac and windows
            return os.path.join('$CAT_ROOT', os.path.relpath(source, self._rootdir))
        return source

    def abs_path(self, rel_path):
        if os.path.isabs(rel_path) or self._rootdir is None:
            return rel_path
        elif rel_path.startswith('$CAT_ROOT'):
            # return re.sub('^\$CAT_ROOT', self.root, rel_path)
            # Should work on both mac and windows
            return os.path.abspath(os.path.join(self.root, os.path.relpath(rel_path, '$CAT_ROOT')))
        return os.path.abspath(os.path.join(self.root, rel_path))

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
        if rootdir is None:
            self._rootdir = None
            self._resolver = LcCatalogResolver(None)
        else:
            self._rootdir = os.path.abspath(rootdir)
            if not os.path.exists(self._rootdir):
                raise FileNotFoundError(self._rootdir)
            self._resolver = LcCatalogResolver(self.resource_dir)

        """
        _archives := source -> archive
        _names :=  ref:interface -> source
        _nicknames := nickname -> source
        """
        self._nicknames = dict()  # keep a collection of shorthands for origins

        self._queries = dict()  # keep a collection of CatalogQuery instances for each origin
        self._bad_origins = defaultdict(set)

        '''
        LCIA: 
        '''
        qdb = LciaDb.new(source=self._reference_qtys, contexts=self._contexts, flowables=self._flowables,
                         strict_clookup=strict_clookup, **kwargs)
        self._qdb = qdb
        res = LcResource.from_archive(qdb, interfaces=('basic', 'index', 'quantity'), store=False)
        self._resolver.add_resource(res, store=False)

    def get_canonical(self, arg):
        return self.lcia_engine.get_canonical(arg)

    def synonyms(self, arg):
        return self.lcia_engine.synonyms(arg)

    @property
    def bad_origins(self):
        for o in self._bad_origins.keys():
            yield o

    def bad_refs(self, origin):
        for y in self._bad_origins[origin]:
            yield y

    '''
    The thing that distinguishes a catalog from an archive is its centralized handling of quantities via the qdb
    '''
    @property
    def qdb(self):
        """
        Provides query access to the quantity database. Should be like cat.query('local.qdb'), except that
        it provides a basic query- which is what internal quantities use themselves
        :return:
        """
        return self._qdb.query

    @property
    def lcia_engine(self):
        return self._qdb.tm

    def register_entity_ref(self, q_ref):
        if q_ref.is_entity:
            raise TypeError('Supplied argument is an entity')
        try:
            self._qdb.add(q_ref)
            # print('registered %s' % q_ref.link)
        except EntityExists:
            pass

    def get_qdb_entity(self, origin, external_ref, entity_type='flow'):
        if origin is None or external_ref is None:
            raise ValueError('%s/%s not valid' % (origin, external_ref))
        link = '/'.join([origin, external_ref])
        ent = self._qdb[link]
        if ent is None:
            raise KeyError(link)
        if ent.entity_type != entity_type:
            raise TypeError(ent)
        return ent

    @property
    def sources(self):
        for k in self._resolver.sources:
            yield k

    @property
    def origins(self):
        for ref, ints in self._resolver.origins:
            yield ref

    @property
    def interfaces(self):
        for ref, ints in self._resolver.origins:
            for i in ints:
                yield ':'.join([ref, i])

    def show_interfaces(self):
        for ref, ints in sorted(self._resolver.origins):
            print('%s [%s]' % (ref, ', '.join(ints)))

    '''
    Nicknames
    
    These are vestigial from the very earliest days of the catalog-- at the time we thought it made sense to assign 
    a nickname to a specific SOURCE but in practice, the only thing we have ever wanted to do was assign a nickname
    to an ORIGIN. so we are going to make that official.
    '''
    @property
    def names(self):
        """
        List known references.
        :return:
        """
        for k, ifaces in self._resolver.origins:
            for iface in ifaces:
                yield ':'.join([k, iface])

    def add_nickname(self, nickname, origin, interface=None):
        """
        alternate names for origins (optional interface) in the catalog
        :param nickname: short name to be used
        :param origin: origin to refer to
        :param interface: [None] interface to specify
        :return:
        """
        try:
            next(self._resolver.resolve(origin, interfaces=interface))
        except UnknownOrigin:
            raise KeyError('Origin %s not found' % origin)
        self._nicknames[nickname] = (origin, interface)

    def is_nickname(self, nickname):
        try:
            org, _ = self._nicknames[nickname]
        except KeyError:
            org = False
        return org

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
            # return self._resolver.get_resource(source=self._nicknames[name], strict=strict)
            name, nick_i = self._nicknames[name]
            iface = iface or nick_i
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
                          key=lambda x: (x.priority, len(x.origin))):  #
            '''
            sort key was formerly: (not (x.is_loaded and x.static), x.priority, x.origin != origin)):
            What were we thinking overriding priority with whether a static resource was loaded?
            
            ans: bad logic. The bad logic was: if we already have the (static JSON) index loaded, we should just use
            it because it's easier / possibly more reliable (?) than accessing the exchange interface.
            
            But this is properly managed by data owners with priorities. Any source-specific optimizations are 
            just that.  
            '''
            yield res

    def resources(self, origin=None, loaded=None):
        """
        Generate a list of resources known to the resolver.  Optionally filter by origin prefix and
        by whether the resource has been loaded.
        :param origin:
        :param loaded: True | False | [None]
        :return:
        """
        def _match_loaded(_res):
            ldd = bool(_res.archive is not None)
            if loaded is None:
                return True
            if ldd is loaded:
                return True
            return False
        for res in self._resolver.resources:
            if origin:
                if not res.origin.startswith(origin):
                    continue
            if _match_loaded(res):
                yield res

    def gen_interfaces(self, origin, itype=None, strict=False):
        """
        Generator of interfaces by spec

        :param origin:
        :param itype: single interface or iterable of interfaces
        :param strict: passed to resolver
        :return:
        """
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
    _query_type = CatalogQuery

    def known_origin(self, origin, strict=False):
        try:
            self._resolver.resolve(origin, strict=strict)
        except UnknownOrigin:
            return False
        return True

    def query(self, origin, strict=False, refresh=False, cache=True, **kwargs):
        """
        Returns a query using the first interface to match the origin.
        :param origin:
        :param strict: [False] whether the resolver should match the origin exactly, as opposed to returning more highly
         specified matches.  e.g. with strict=False, a request for 'local.traci' could be satisfied by 'local.traci.2.1'
         whereas if strict=True, only a resource matching 'local.traci' exactly will be returned
        :param refresh: [False] by default, the catalog stores a CatalogQuery instance for every requested origin.  With
         refresh=True, any prior instance will be replaced with a fresh one.
        :param cache: [True] whether to retain the query
        :param kwargs:
        :return:
        """
        if origin in self._nicknames:
            origin, _ = self._nicknames[origin]

        try:
            next(self._resolver.resolve(origin, strict=strict))
        except StopIteration:
            raise UnknownOrigin(origin, strict)

        if refresh:
            self._queries.pop(origin, None)

        if cache and origin in self._queries:
            return self._queries[origin]

        query = self._query_type(origin, catalog=self, **kwargs)
        if query.validate():
            pass
        else:
            raise InvalidQuery(origin)
        if cache:
            self._queries[origin] = query
        return query

    def lookup(self, catalog_ref):
        """
        Attempts to return a valid grounded reference matching the one supplied.
        :param catalog_ref:
        :deprecated keep_properties: [False] if True, apply incoming ref's properties to grounded ref, probably with a
        prefix or something.
        :return:
        """
        ref = self.query(catalog_ref.origin).get(catalog_ref.external_ref)
        '''
        if keep_properties:
            for k in catalog_ref.properties():
                ref[k] = catalog_ref[k]
        '''
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
    '''

    def fetch(self, link):
        origin, external_ref = link.split('/', maxsplit=1)
        return self.query(origin).get(external_ref)

    '''
    # why is this here? I don't think we even want this.
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
        except UnknownOrigin:

            ref = CatalogRef(origin, external_ref, entity_type=entity_type, **kwargs)
            print('Ungrounded catalog ref %s' % ref.link)
            self._bad_origins[origin].add(ref)
            return ref
        return q.get(external_ref)
        # except EntityNotFound:  why would we catch this?
        #     return CatalogRef.from_query(external_ref, q, entity_type=entity_type, **kwargs)
    '''
