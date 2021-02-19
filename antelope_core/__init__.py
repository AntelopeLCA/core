"""
Initialization file for antelope_core package

Package Includes:
 - Exchanges, Characterizations, and Lcia Results
 - LCA Process, Flow, and Quantity entities
 - An Entity Store (which should really be a database instead of a python object) and subclasses for life cycle data
"""


from synonym_dict import LowerDict
from antelope import antelope_herd

import importlib

from .from_json import from_json, to_json
from .archives import archive_factory, ArchiveError

FOUND_PROVIDERS = LowerDict()
def _find_providers():
    for ant in [__name__] + antelope_herd:
        try:
            importlib.import_module('.', package=ant)
        except ModuleNotFoundError:
            continue
        p = importlib.import_module('.providers', package=ant)
        try:
            provs = getattr(p, 'PROVIDERS')
        except AttributeError:
            print('No PROVIDERS found in %s' % ant)
            continue
        for ds_type in provs:
            FOUND_PROVIDERS[ds_type] = p

    print('Found Antelope providers:' )
    for k, v in FOUND_PROVIDERS.items():
        print('%s:%s' % (v.__name__, k))


def herd_factory(ds_type):
    try:
        return archive_factory(ds_type)
    except ArchiveError:
        if len(FOUND_PROVIDERS) == 0:
            _find_providers()
        if ds_type in FOUND_PROVIDERS:
            prov = FOUND_PROVIDERS[ds_type]
            try:
                return getattr(prov, ds_type)
            except AttributeError:
                dsl = ds_type.lower()
                try:
                    attr = next(k for k in prov.PROVIDERS if k.lower().startswith(dsl))
                    return getattr(prov, attr)
                except (StopIteration, AttributeError):
                    raise ArchiveError('ds_type %s not found in %s' % (ds_type, prov.__name__))
    print('# LENGTH OF PROVIDERS: %d' % len(FOUND_PROVIDERS))
    raise ImportError('Cannot find a package for loading %s' % ds_type)


def add_antelope_providers(mod, provs=None):
    """
    Manually register Antelope providers
    :param mod: a python module or package that contains data providers
    :param provs: a list of provider class names that can be imported from module (if omitted,
    attempts to getattr(module, 'PROVIDERS')
    (note: may also supply a single class as a string- it will get listified)
    :return:
    """
    if isinstance(provs, str):
        provs = [provs]
    if len(FOUND_PROVIDERS) == 0:
        _find_providers()
    if provs is None:
        try:
            provs = getattr(mod, 'PROVIDERS')
        except AttributeError:
            print('No providers found in %s' % mod.__name__)
            provs = []
    for ds_type in provs:
        print('Adding %s:%s' % (mod.__name__, ds_type))
        FOUND_PROVIDERS[ds_type] = mod


from .catalog import LcCatalog
from .lc_resource import LcResource
from .data_sources.local import make_config
