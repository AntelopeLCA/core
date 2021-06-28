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
from .lc_resource import LcResource, INTERFACE_TYPES


DEFAULT_PRIORITIES = {
    'exchange': 20,
    'quantity': 20,
    'index': 50,
    'background': 80
}


class FileAccessor(object):

    def __init__(self, load_path):
        self._path = os.path.abspath(load_path)  # this has the benefits of collapsing '..' and trimming trailing '/'
        self._origins = os.listdir(self._path)

    @property
    def path(self):
        return self._path

    @property
    def origins(self):
        for k in self._origins:
            yield k

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
                dc = os.path.join(ipath, ds_type, 'config.json')
                if os.path.exists(dc):
                    os.remove(dc)

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

    def create_resource(self, source):
        if not source.startswith(self._path):
            raise ValueError('Path not contained within our filespace')
        rel_source = source[len(self._path)+1:]
        org, iface, ds_type, fn = rel_source.split(os.path.sep)  # note os.pathsep is totally different

        cfg = self.read_config(source)
        priority = cfg.pop('priority', DEFAULT_PRIORITIES[iface])

        # do this last
        iface = (iface,)
        for ad in cfg.pop('add_interfaces', ()):
            if ad in INTERFACE_TYPES:
                iface += (ad, )

        return LcResource(org, source, ds_type, interfaces=iface, priority=priority, **cfg)
