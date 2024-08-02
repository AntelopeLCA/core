"""
FileAccessor, for standardizing access to antelope resources on a filesystem having the following structure:

{DATA ROOT}/[origin]/[interface]/[ds_type]/[source_file]  - source
{DATA ROOT}/[origin]/[interface]/[ds_type]/config.json    - configuration

A filesystem having this structure will enable automatic registration of resources, taking origin, interface, and
ds_type from the directory structure and the source+config files as-discovered by traversing the filesystem.

Should probably alter gen_sources to only return the one "best" source per path, instead of generating all of them.
[One] problem with that is that, for instance, for Tarjan background I would need to ignore files that end with
ORDERING_SUFFIX, but I can't import anything from antelope_background without creating a dependency loop.

For now, generating the sources is probably fine.

"""

import os
import json
from .lc_resource import LcResource
from antelope import BackgroundRequired
from shutil import copy2


DEFAULT_PRIORITIES = {
    'exchange': 20,
    'quantity': 20,
    'index': 50,
    'background': 80
}


class FileAccessor(object):

    def __init__(self, load_path, prefix=None):
        self._path = os.path.abspath(load_path)  # this has the benefits of collapsing '..' and trimming trailing '/'
        self._origins = os.listdir(self._path)
        self._prefix = prefix

    @property
    def path(self):
        return self._path

    @property
    def origins(self):
        for k in self._origins:
            yield k

    def refresh(self):
        self._origins = os.listdir(self._path)

    @staticmethod
    def read_config(source):
        cfg = os.path.join(os.path.dirname(source), 'config.json')
        if os.path.exists(cfg):
            with open(cfg) as fp:
                config = json.load(fp)
        else:
            config = dict()
        return config

    @staticmethod
    def write_config(source, **kwargs):
        """
        Note: use the config argument add_interfaces=[list] to allow a resource to implement additional interfaces.
        :param source:
        :param kwargs:
        :return:
        """
        cfg = os.path.join(os.path.dirname(source), 'config.json')
        with open(cfg, 'w') as fp:
            json.dump(kwargs, fp, indent=2)

    def update_config(self, source, **updates):
        existing_cfg = self.read_config(source)
        existing_cfg.update(updates)
        self.write_config(source, **existing_cfg)

    def clear_configs(self, origin):
        opath = os.path.join(self.path, origin)
        for iface in os.listdir(opath):
            ipath = os.path.join(opath, iface)
            for ds_type in os.listdir(ipath):
                dc = os.path.join(ipath, ds_type, 'config.json')  # don't understand why this is an error
                if os.path.exists(dc):
                    os.remove(dc)

    def source(self, org, iface):
        """
        Return the "best" (right now: first) single source
        :param org:
        :param iface:
        :return:
        """
        return next(self.gen_sources(org, iface))

    def gen_sources(self, org, iface):
        iface_path = os.path.join(self._path, org, iface)
        if not os.path.exists(iface_path):
            return
        for ds_type in os.listdir(iface_path):
            ds_path = os.path.join(iface_path, ds_type)
            if not os.path.isdir(ds_path):
                continue
            for fn in os.listdir(ds_path):
                if fn == 'config.json':
                    continue
                # if we want to order sources, this is the place to do it
                source = os.path.join(ds_path, fn)
                yield source

    def create_resource(self, source, basic=False, **kwargs):
        if not source.startswith(self._path):
            raise ValueError('Path not contained within our filespace')
        rel_source = source[len(self._path)+1:]
        org, iface, ds_type, fn = rel_source.split(os.path.sep)  # note os.pathsep is totally different

        cfg = self.read_config(source)
        cfg.update(kwargs)
        priority = cfg.pop('priority', DEFAULT_PRIORITIES[iface])

        if self._prefix is not None:
            org = '.'.join([str(self._prefix), org])

        # do this last
        ifaces = set()
        ifaces.add(iface)
        if basic:
            ifaces.add('basic')
        # if iface == 'index':
        #     # TODO: WORKAROUND: automatically add 'basic' to index interfaces until we have it sorted out
        #     ifaces.add('basic')

        return LcResource(org, source, ds_type, interfaces=tuple(ifaces), priority=priority, **cfg)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class ResourceLoader(FileAccessor):
    """
    This class crawls a directory structure and adds all the specified resources to the catalog provided as
    an input argument.

    After loading all pre-initialized resources, the class runs check_bg on each origin to auto-generate a
    background ordering within the catalog's filespace if one does not already exist.
    """

    def _load_origin(self, cat, org, check, replace=True):
        """
        Crawls the data path at the specified origin and creates a resource for each source found.
        TODO: figure out whether basic should be a standalone resource // how to handle documentation
        :param org:
        :param check: Whether to instantiate the interfaces
        :return:
        """
        check_bg = False
        for iface in ('exchange', 'index', 'background', 'quantity'):
            for i, source in enumerate(self.gen_sources(org, iface)):
                res = self.create_resource(source)
                cat.add_resource(res, replace=replace)
                if check:
                    res.check(cat)
                    if iface == 'background':
                        check_bg = True
        if self._prefix:
            dest_org = '.'.join([self._prefix, org])
        else:
            dest_org = org
        try:
            next(cat.gen_interfaces(dest_org, 'basic'))
            valid = True
        except StopIteration:
            print('Warning: %s: no basic interface (add manually and save)' % dest_org)
            valid = False
        if check_bg:
            try:
                bg = cat.query(dest_org).check_bg()
            except BackgroundRequired:
                bg = False
            return valid and bg
        else:
            return valid

    def load_resources(self, cat, origin=None, check=False, replace=True):
        """
        For named origin (or all origins), load resources into the catalog. this will not open the resources, unless
        check=True
        :param cat:
        :param origin:
        :param check: [False] whether to load the resources and check background
        :param replace: [True] whether to replace an existing resource with the new one
        :return:
        """
        if origin is None:
            status = []
            for org in self.origins:
                status.append(self._load_origin(cat, org, check, replace=replace))
        else:
            status = self._load_origin(cat, origin, check, replace=replace)
        return status


class ResourceWriter(object):

    def __init__(self, resource, root_dir, origin=None, interface=None, **kwargs):
        self.res = resource
        self.fa = FileAccessor(root_dir)
        if origin is None:
            self._origin = self.res.origin
        else:
            self._origin = origin
        if interface is None:
            for k in ('exchange', 'quantity', 'index', 'background'):
                if k in self.res.interfaces:
                    interface = k
                    break
            if interface is None:
                raise AttributeError('no interfaces!')

        self.interface = interface
        self.add_interfaces = [k for k in self.res.interfaces if k != interface]
        self.write(**kwargs)

    @property
    def origin(self):
        return self._origin

    @property
    def target_path(self):
        return os.path.join(self.fa.path, self.origin, self.interface, self.res.ds_type)

    @property
    def target(self):
        return os.path.join(self.target_path, os.path.basename(self.res.source))

    def write(self, force=False, **kwargs):
        if os.path.exists(self.target):
            if not force:
                raise FileExistsError(self.target)
        os.makedirs(self.target_path, exist_ok=True)
        if self.origin not in self.fa.origins:
            self.fa.refresh()
        if os.path.exists(self.res.source):
            copy2(self.res.source, self.target)
        else:
            self.res.archive.write_to_file(self.target, domesticate=True)
        ex_cfg = self.res.serialize(stripped=True)
        ex_cfg.update(kwargs)
        self.fa.write_config(self.target, add_interfaces=self.add_interfaces, **ex_cfg)
        print('written to %s' % self.target)
