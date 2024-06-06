import json
import os
from collections import defaultdict

from antelope import UnknownOrigin

from .archives import InterfaceError, index_archive, update_archive, create_archive

# from .foreground import LcForeground
from .catalog_query import INTERFACE_TYPES, NoCatalog, zap_inventory

# from .providers import create_archive
from . import herd_factory


class ResourceInvalid(Exception):
    """
    resource points to an invalid filename
    """
    pass


class LcResource(object):
    """
    This is a record that links a semantic reference to a physical data source, and specifies the capabilities
    (and someday, access limitations) of the data source.

    The LcResource serializes to a json file with the following format:
    { ref: [ { "dataSource": source, "dataSourceType": ds_type, .... }, ... ] }
    where ref is the semantic reference.

    """
    @classmethod
    def from_archive(cls, archive, interfaces, source=None, **kwargs):
        source = source or archive.source
        ref = archive.ref
        ds_type = archive.__class__.__name__  # static flag indicates whether archive is complete
        kwargs.update(archive.init_args)
        static = kwargs.pop('static', archive.static)
        res = cls(ref, source, ds_type, interfaces=interfaces, static=static, preload_archive=archive, **kwargs)

        return res

    @classmethod
    def from_dict(cls, ref, d):
        """
        Returns a single LcResource loaded from a dict.  only required field is 'dataSourceType'.
        other fields are passed to the constructor and either interpreted directly or added as supplemental args

        If 'dataSource' is not present, one had better hope that url is present in the dict to download the source
        :param ref:
        :param d:
        :return:
        """
        source = d.pop('dataSource', None)
        ds_type = d.pop('dataSourceType')

        # patch to deal with changing Background extension handling-- this is WACKO
        filetype = d.pop('filetype', None)
        if filetype is not None:
            if not source.endswith(filetype):
                source += filetype

        return cls(ref, source, ds_type, **d)

    @classmethod
    def from_file(cls, file):
        """
        generates LcResources contained in the named file, sorted by increasing priority.  The filename and
        the reference must be the same.
        :param file:
        :return: an ordered list of resources
        """
        ref = os.path.basename(file)
        with open(file, 'r') as fp:
            j = json.load(fp)
        return cls.from_json(j, ref)

    @classmethod
    def from_json(cls, j, ref):
        return sorted([cls.from_dict(ref, d) for d in j[ref]], key=lambda x: x.priority)

    def _instantiate(self, catalog=None):
        """
        Instantiate the archive described by the current resource.  Several steps:
         - download file if applicable
         - rename relative path to absolute path
         - instantiate the archive
         - load any cached data into the archive
         - override static spec to be consistent with archive's own spec
         - load_all() if static
         - apply_config()

        :param catalog: required for: file downloads, foreground interfaces, local paths + caches
        :return:
        """
        if self.source is None:
            # download
            if catalog is None:
                raise NoCatalog('Remote resource encountered')
            if 'download' in self._args:
                print('Downloading from %s' % self._args['download']['url'])
                self._source = catalog.download_file(localize=True, **self._args['download'])
                self.write_to_file(catalog.resource_dir)  # update resource file
            else:
                raise AttributeError('Resource has no source specified and no download information')

        if self.source.startswith('$CAT_ROOT'):
            try:
                src = catalog.abs_path(self.source)
            except AttributeError:
                raise NoCatalog('Relative path encountered but no catalog supplied')
        else:
            src = self.source

        kwargs = {**self.init_args}

        if 'foreground' in self.interfaces:
            kwargs['catalog'] = catalog

        if self.ds_type.lower() == 'ecoinventlcia':
            # this is a GIANT HACK
            # we need to bring along a local ecoinvent archive to lookup flow reference qtys-
            # Partial solution to the hack: reference entity is QUERYABLE from a basic query which the catalog
            # can provide- still need to specify
            ei_ref = '.'.join(['local', 'ecoinvent', kwargs['version']])
            try:
                res = catalog.get_resource(ei_ref, iface='exchange', strict=False)
                res.check(catalog)
                if hasattr(res.archive, 'load_flows'):
                    res.archive.load_flows()
                kwargs['ei_archive'] = res.archive
            except UnknownOrigin:
                pass

        try:
            self._archive = create_archive(src, self.ds_type, factory=herd_factory,
                                           ref=self.origin, **kwargs)
        except FileNotFoundError as e:
            raise ResourceInvalid('%s: %s' % (self.origin, e.filename))

        if catalog is not None and catalog.check_cache(self.source):
            update_archive(self._archive, catalog.cache_file(self.source))
        self._static |= self._archive.static
        if self.static and self.ds_type.lower() != 'json':
            self._archive.load_all()  # static json archives are loaded on open- load_all() would be redundant

    @property
    def is_loaded(self):
        return self._archive is not None  # and self._archive._loaded ??

    def remove_archive(self):
        self._archive = None

    def check(self, catalog):
        if self._archive is None:
            # TODO: try/catch exceptions or return false
            print('QQQQQQQQQQQQQQQQQQ %s QQQQQQQQQQQQQQQQQQ' % self.origin)
            self._instantiate(catalog)
            self.apply_config(catalog)  # can't remember why I set this to happen recurrently- but it's no good
            ''' # on second thought, I dont think we want to do this-- catalog_names is a mapping of ref to source
            if catalog:
                for name in self.archive.catalog_names:
                    if name not in catalog.origins:
                        catalog.add_nickname(name, self.origin)
            '''

        return True

    def save(self, catalog):
        self.write_to_file(catalog.resource_dir)

    def make_index(self, index_file, force=True, save=True):
        self.check(None)
        self._archive.load_all()

        the_index = index_archive(self._archive, index_file, force=force, save=save)

        return the_index

    def make_cache(self, cache_file):
        # note: do not make descendant
        self._archive.write_to_file(cache_file, complete=True, gzip=True)
        print('Created archive of %s containing:' % self._archive)
        self._archive.check_counter()

    def make_interface(self, iface):
        return self._archive.make_interface(iface)

    def apply_config(self, catalog=None):
        # if len(self._config) == 0:  # NOW we don't even need to alter blackbook!!!
        #    return
        # print('Applying stored configuration')
        try:
            self._archive.make_interface('configure').apply_config(self._config)
        except InterfaceError:
            pass
        # we are moving this below apply_config to allow the archive to add/edit hints, which it can do because
        # it receives the authentic config dict as an argument
        if catalog is not None:
            if 'hints' in self._config:
                catalog.lcia_engine.apply_hints(self._archive.catalog_names, self._config['hints'])

    def remove_interface(self, iface):
        iface = zap_inventory(iface)  # don't warn when interpreting resource specifications
        if iface in self._interfaces:
            self._interfaces.remove(iface)

    def add_interface(self, iface):
        iface = zap_inventory(iface)  # don't warn when interpreting resource specifications
        if iface in INTERFACE_TYPES:
            self._interfaces.add(iface)

    def _normalize_interfaces(self, interfaces):
        """
        Ensures that:
         - interfaces spec can be string or list
         X 'basic' appears -- we don't want a basic interface on every resource
        :param interfaces:
        :return:
        """
        # self.add_interface('basic')
        if interfaces is None:
            return
        if isinstance(interfaces, str):
            self.add_interface(interfaces)
        else:
            for k in interfaces:
                self.add_interface(k)

    def __init__(self, origin, source, ds_type, interfaces=None, privacy=0, priority=50, static=False,
                 preload_archive=None, config=None, **kwargs):
        """

        :param origin: semantic reference to data origin
        :param source: physical data source; 'None' allowed if 'downloadLink' argument provided
        :param ds_type: data source type
        :param interfaces: list which can include 'entity', 'foreground', or 'background'. Default 'foreground'
        :param privacy: Ignored / No longer used.
        :param priority: [50] priority level.. numeric (nominally 0-100), lowest priority resource is loaded first
        :param static: [False] if True, load_all() after initializing
        :param preload_archive: [None] use to assign an existing archive
        :param config: ConfigureInterface specifications
        :param kwargs: additional keyword arguments to constructor. Some interesting ones:
          download: a dict containing 'url' and optional 'md5sum' fields
          prefix: often used when accessing zipped archives
          token: a jwt used for authenticating to xdb
          'options': popped with values used as kwargs (per ResourceSpec)


        """
        '''
        if not os.path.exists(source):
            raise EnvironmentError('%s not found' % source)
        '''

        self._archive = preload_archive

        options = kwargs.pop('options', dict())
        kwargs.update(options)

        self._org = origin
        if source is None:
            if 'download' not in kwargs:
                raise KeyError('Resource must be initialized with either source or download')
        self._source = source
        self._type = ds_type
        self._static = static

        self._issaved = False

        self._interfaces = set()
        self._normalize_interfaces(interfaces)

        if 'add_interfaces' in kwargs:
            self._normalize_interfaces(kwargs['add_interfaces'])

        self._priority = int(priority)

        self._internal = kwargs.pop('_internal', False)

        self._config = defaultdict(set)

        if config:
            for k, v in config.items():
                for q in v:
                    self._add_config(k, *q)

        self._args = kwargs

    def __repr__(self):
        flags = ['']
        if self.internal:
            flags.append('_int')
        if self.static:
            flags.append('static')
        if self._archive is not None:
            flags.append('loaded ')
        if len(self._config) > 0:
            flags.append('%d cfg' % len(self._config))
        fgs = ' '.join(flags)

        return 'LcResource(%s, dataSource=%s:%s, %s [%d]%s)' % (self.origin, self.source, self.ds_type,
                                                                [k for k in self.interfaces], self.priority, fgs)

    def exists(self, path):
        if path is None:
            return False
        filename = os.path.join(path, self.origin)
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as fp:
                    j = json.load(fp)
            except json.JSONDecodeError:
                return False

            if any([self.matches(k) for k in j[self.origin]]):
                return True
        return False

    @property
    def archive(self):
        return self._archive

    @property
    def is_saved(self):
        return self._issaved

    @property
    def origin(self):
        return self._org

    @property
    def source(self):
        return self._source

    @property
    def ds_type(self):
        return self._type

    @property
    def interfaces(self):
        for k in self._interfaces:
            yield k

    @property
    def internal(self):
        return self._internal

    @property
    def priority(self):
        return self._priority

    @property
    def static(self):
        return self._static or self.ds_type.lower() == 'json'

    @property
    def init_args(self):
        return self._args

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config_dict):
        if self.archive is not None:
            try:
                cf = self.archive.make_interface('configure')
                for cfg, cfgs in config_dict.items():
                    for args in cfgs:
                        cf.check_config(cfg, args)
            except InterfaceError:
                pass
        # apply config without replacing it blindly
        for k, v in config_dict.items():
            for t in v:
                self._config[k].add(tuple(t))

    def satisfies(self, ifaces):
        if ifaces is None:
            return True
        if isinstance(ifaces, str):
            ifaces = [ifaces]
        for i in ifaces:
            # if i == 'basic':
            #     return True
            if i in self._interfaces:
                return True
        return False

    def add_hint(self, hint_type, term, matches, catalog=None):
        if hint_type in ('context', 'flowable', 'quantity'):
            self._add_config('hints', hint_type, term, matches)
            if catalog:
                catalog.lcia_engine.apply_hints(self._archive.catalog_names, [(hint_type, term, matches)])
                self.save(catalog)
            else:
                print('Hint is neither saved nor applied')
        else:
            raise TypeError('Invalid hint type %s' % hint_type)

    def _add_config(self, config, *args):
        """
        does no validation
        :param config:
        :param args:
        :return:
        """
        self._config[config].add(args)

    def _serialize_config(self):
        j = dict()
        for k, v in self._config.items():
            j[k] = sorted([list(g) for g in v], key=lambda x: x[0])
        return j

    def export_config(self):
        return self._serialize_config()

    def serialize(self, stripped=False):
        if stripped:
            j = {"static": self.static,
                 "priority": self.priority}
        else:
            j = {
                "dataSource": self.source,
                "dataSourceType": self.ds_type,
                "interfaces": sorted([k for k in self.interfaces]),
                "priority": self.priority,
                "static": self.static
            }
            if self.internal:
                j['_internal'] = True
        j.update(self._args)
        j['config'] = self._serialize_config()
        return j

    def matches(self, k):
        """
        Pretty cheesy.  When we serialize a set of resources, we need to make sure not to include self twice.  To
        make the comparison concrete, use a serialized resource as input.

        We were using dataSource as a unique identifier for resource entries; but the introduction of download links
         breaks that because a downloadable resource has no source until it's been downloaded.
         The solution is to fallback to download.url ONLY IF the resource has no source specified.
        :param k: a serialized LcResource
        :return:
        """
        if k['dataSource'] is not None and self.source is not None:
            return k['dataSource'] == self.source
        return k['download']['url'] == self._args['download']['url']

    def write_to_file(self, path, assign_ref=None, apply_config=None):
        """
        Adds the resource to a file whose name is the resource's semantic reference. If the same datasource is
        already present in the file, replace it with the current resource.  otherwise append.
        :param path: directory to store the resource file.
        :param assign_ref: assign this ref instead of the resource's current ref
        :param apply_config: overwrites configuration with supplied dict
        :return:
        """
        if assign_ref is None:
            assign_ref = self.origin
        if apply_config is not None:
            self.config = apply_config  # tests configuration before storing it
        if not os.path.isdir(path):
            if os.path.exists(path):
                raise ValueError('Please provide a directory path')
            os.makedirs(path)


        filename = os.path.join(path, assign_ref)
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as fp:
                    j = json.load(fp)
            except json.JSONDecodeError:
                j = {assign_ref: []}

            resources = [k for k in j[assign_ref] if not self.matches(k)]
            resources.append(self.serialize())
        else:
            resources = [self.serialize()]
        with open(os.path.join(path, assign_ref), 'w') as fp:
            json.dump({assign_ref: resources}, fp, indent=2)
        self._issaved = True
