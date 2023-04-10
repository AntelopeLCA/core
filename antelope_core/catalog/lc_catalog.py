import tempfile

from antelope.xdb_tokens import ResourceSpec

from .catalog import StaticCatalog
from ..archives import REF_QTYS, archive_from_json
from ..lc_resource import LcResource
from ..lcia_engine import DEFAULT_CONTEXTS, DEFAULT_FLOWABLES
from ..providers.xdb_client.rest_client import RestClient

import requests
from requests.exceptions import HTTPError

from shutil import copy2, rmtree
import os
import json
import hashlib
import getpass

# TEST_ROOT = os.path.join(os.path.dirname(__file__), 'cat-test')  # volatile, inspectable


def download_file(url, local_file, md5sum=None):
    r = requests.get(url, stream=True)
    md5check = hashlib.md5()
    with open(local_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                md5check.update(chunk)
                # f.flush() commented by recommendation from J.F.Sebastian
    if md5sum is not None:
        assert md5check.hexdigest() == md5sum, 'MD5 checksum does not match'


class LcCatalog(StaticCatalog):
    """
    A catalog that supports adding and manipulating resources during runtime
    """
    def download_file(self, url=None, md5sum=None, force=False, localize=True):
        """
        Download a file from a remote location into the catalog and return its local path.  Optionally validate the
        download with an MD5 digest.
        :param url:
        :param md5sum:
        :param force:
        :param localize: whether to return the filename relative to the catalog root
        :return: the full path to the downloaded file 
        """
        local_file = os.path.join(self._download_dir, self._source_hash_file(url))
        if os.path.exists(local_file):
            if force:
                print('File exists.. re-downloading.')
            else:
                print('File already downloaded.  Force=True to re-download.')
                if localize:
                    return self._localize_source(local_file)
                return local_file

        download_file(url, local_file, md5sum)

        import magic
        if magic.from_file(local_file).startswith('Microsoft Excel 20'):
            new_file = local_file + '.xlsx'
            os.rename(local_file, new_file)  # openpyxl refuses to open files without an extension
            local_file = new_file

        if localize:
            return self._localize_source(local_file)
        return local_file

    @classmethod
    def make_tester(cls, **kwargs):
        """
        Sets a flag that tells the rootdir to be deleted when the catalog is garbage collected
        :param kwargs:
        :return:
        """
        tmp = tempfile.mkdtemp()
        return cls(tmp, _test=True, **kwargs)

    """
    @classmethod
    def load_tester(cls):
        return cls(TEST_ROOT)
    """

    @property
    def _dirs(self):
        for x in (self._cache_dir, self._index_dir, self.resource_dir, self.archive_dir, self._download_dir):
            yield x

    def _make_rootdir(self):
        for x in self._dirs:
            os.makedirs(x, exist_ok=True)
        if not os.path.exists(self._contexts):
            copy2(DEFAULT_CONTEXTS, self._contexts)
        if not os.path.exists(self._flowables):
            copy2(DEFAULT_FLOWABLES, self._flowables)
        if not os.path.exists(self._reference_qtys):
            copy2(REF_QTYS, self._reference_qtys)

    def __init__(self, rootdir, _test=False, **kwargs):
        self._rootdir = os.path.abspath(rootdir)
        self._make_rootdir()  # this will be a git clone / fork;; clones reference quantities
        self._test = _test
        self._blackbook_client = None
        super(LcCatalog, self).__init__(self._rootdir, **kwargs)

    def __del__(self):
        """
        This is unreliable- temp directories tend to accumulate
        :return:
        """
        if self._blackbook_client:
            self._blackbook_client.close()
        if self._test:
            # print('tryna delete %s' % self.root)
            rmtree(self.root, ignore_errors=True)

    def save_local_changes(self):
        self._qdb.write_to_file(self._reference_qtys, characterizations=True, values=True)
        self.lcia_engine.save_flowables(self._flowables)
        self.lcia_engine.save_contexts(self._contexts)

    def restore_contexts(self, really=False):
        if really:
            print('Overwriting local contexts')
            copy2(DEFAULT_CONTEXTS, self._contexts)
        else:
            print('pass really=True if you really want to overwrite local contexts')

    def restore_qdb(self, really=False):
        if really:
            copy2(REF_QTYS, self._reference_qtys)
            print('Reference quantities restored. Please re-initialize the catalog.')

    '''
    Create + Add data resources
    '''

    def new_resource(self, reference, source, ds_type, store=True, **kwargs):
        """
        Create a new data resource by specifying its properties directly to the constructor
        :param reference:
        :param source:
        :param ds_type:
        :param store: [True] permanently store this resource
        :param kwargs: interfaces=None, priority=0, static=False; **kwargs passed to archive constructor
        :return:
        """
        source = self._localize_source(source)
        return self._resolver.new_resource(reference, source, ds_type, store=store, **kwargs)  # explicit store= for doc purposes

    def add_resource(self, resource, store=True):
        """
        Add an existing LcResource to the catalog.
        :param resource:
        :param store: [True] permanently store this resource
        :return:
        """
        self._resolver.add_resource(resource, store=store)
        # self._ensure_resource(resource)

    def delete_resource(self, resource, delete_source=None, delete_cache=True):
        """
        Removes the resource from the resolver and also removes the serialization of the resource. Also deletes the
        resource's source under the following circumstances:
         (resource is internal AND resources_with_source(resource.source) is empty AND resource.source is a file)
        This can be overridden using he delete_source param (see below)
        :param resource: an LcResource
        :param delete_source: [None] If None, follow default behavior. If True, delete the source even if it is
         not internal (source will not be deleted if other resources refer to it OR if it is not a file). If False,
         do not delete the source.
        :param delete_cache: [True] whether to delete cache files (you could keep them around if you expect to need
         them again and you don't think the contents will have changed)
        :return:
        """
        self._resolver.delete_resource(resource)
        abs_src = self.abs_path(resource.source)

        if delete_source is False or resource.source is None or not os.path.isfile(abs_src):
            return
        if len([t for t in self._resolver.resources_with_source(resource.source)]) > 0:
            return
        if resource.internal or delete_source:
            if os.path.isdir(abs_src):
                rmtree(abs_src)
            else:
                os.remove(abs_src)
        if delete_cache:
            if os.path.exists(self.cache_file(resource.source)):
                os.remove(self.cache_file(resource.source))
            if os.path.exists(self.cache_file(abs_src)):
                os.remove(self.cache_file(abs_src))

    def add_existing_archive(self, archive, interfaces=None, store=True, **kwargs):
        """
        Makes a resource record out of an existing archive.  by default, saves it in the catalog's resource dir
        :param archive:
        :param interfaces:
        :param store: [True] if False, don't save the record - use it for this session only
        :param kwargs:
        :return:
        """
        res = LcResource.from_archive(archive, interfaces, source=self._localize_source(archive.source), **kwargs)
        self._resolver.add_resource(res, store=store)

    def blackbook_authenticate(self, blackbook_url=None, username=None, password=None, token=None, save_credentials=True):
        """
        Opens an authenticated session with the designated blackbook server.  Credentials can either be provided to the
        method as arguments, or if omitted, they can be obtained through a form.  If a token is provided, it is
        used in lieu of a password workflow
        :param blackbook_url:
        :param username:
        :param password:
        :param token:
        :return:
        """
        if self._blackbook_client:
            if blackbook_url is None:
                self._blackbook_client.reauthenticate()  # or raise NoCredentials
                return
            self._blackbook_client.close()
        elif blackbook_url is None:
            raise ValueError('Must provide a URL')
        if token is None:
            client = RestClient(blackbook_url, auth_route='auth/token', save_credentials=save_credentials)
            if username is None:
                username = input('Enter username to access blackbook server at %s: ' % blackbook_url)
            if password is None:
                password = getpass.getpass('Enter password to access blackbook server at %s: ' % blackbook_url)
            try:
                client.authenticate(username, password)
            except HTTPError:
                client.close()
                raise
        else:
            client = RestClient(blackbook_url, token=token, auth_route='auth/token', save_credentials=save_credentials)
        self._blackbook_client = client

    def get_blackbook_resources(self, origin):
        """
        Use a blackbook server to obtain resources for a given origin.
        :param origin:
        :return:
        """
        res = list(self.resources(origin))
        if len(res) > 0:
            return self.refresh_xdb_tokens(origin)
        else:
            resource_dict = self._blackbook_client.get_one(dict, 'origins', origin, 'resource')
            return self._finish_get_blackbook_resources(resource_dict)

    def get_blackbook_resources_by_client(self, bb_client, username, origin):
        """
        this uses the local maintenance client rather than the REST client
        :param bb_client:
        :param username:
        :param origin:
        :return:
        """
        resource_dict = bb_client.retrieve_resource(username, origin)
        return self._finish_get_blackbook_resources(resource_dict)

    def _finish_get_blackbook_resources(self, resource_dict):

        rtn = []

        for recv_origin, res_list in resource_dict.items():
            # self._resolver.delete_origin(recv_origin)
            for res in res_list:
                if isinstance(res, ResourceSpec):
                    r = LcResource(**res.dict())
                else:
                    r = LcResource(**res)

                self.add_resource(r)
                rtn.append(r)
        return rtn

    def refresh_xdb_tokens(self, origin):
        """
        requires an active blackbook client (try blackbook_authenticate() if it has expired)
        :param origin:
        :return:
        """
        tok = self._blackbook_client.get_one(str, 'origins', origin, 'token')
        rtn = []
        for res in self._resolver.resources:
            if res.origin == origin:  # and hasattr(res.archive, 'r'):
                if res.archive is None:
                    res.init_args['token'] = tok
                    res.check(self)
                if hasattr(res.archive, 'r'):
                    res.archive.r.set_token(tok)
                rtn.append(res)
        return rtn

    '''
    Manage resources locally
     - index
     - cache
     - static archive (performs load_all())
    '''

    def _index_source(self, source, priority, force=False):
        """
        Instructs the resource to create an index of itself in the specified file; creates a new resource for the
        index
        :param source:
        :param priority:
        :param force:
        :return:
        """
        res = next(r for r in self._resolver.resources_with_source(source))
        res.check(self)
        priority = min([priority, res.priority])  # why are we doing this?? we want index to have higher priority i.e. get loaded second
        stored = self._resolver.is_permanent(res)

        # save configuration hints in derived index
        cfg = None
        if stored:
            if len(res.config['hints']) > 0:
                cfg = {'hints': res.config['hints']}

        inx_file = self._index_file(source)
        inx_local = self._localize_source(inx_file)

        if os.path.exists(inx_file):
            if not force:
                print('Not overwriting existing index. force=True to override.')
                try:
                    ex_res = next(r for r in self._resolver.resources_with_source(inx_local))
                    return ex_res.origin
                except StopIteration:
                    # index file exists, but no matching resource
                    inx = archive_from_json(inx_file)
                    self.new_resource(inx.ref, inx_local, 'json', priority=priority, store=stored,
                                      interfaces='index', _internal=True, static=True, preload_archive=inx,
                                      config=cfg)

                    return inx.ref

            print('Re-indexing %s' % source)
            # TODO: need to delete the old index resource!!
            stale_res = list(self._resolver.resources_with_source(inx_local))
            for stale in stale_res:
                # this should be postponed to after creation of new, but that fails in case of naming collision (bc YYYYMMDD)
                # so golly gee we just delete-first.
                print('deleting %s' % stale.origin)
                self.delete_resource(stale)

        the_index = res.make_index(inx_file, force=force)
        self.new_resource(the_index.ref, inx_local, 'json', priority=priority, store=stored, interfaces='index',
                          _internal=True, static=True, preload_archive=the_index, config=cfg)

        return the_index.ref

    def index_ref(self, origin, interface=None, source=None, priority=60, force=False, strict=True):
        """
        Creates an index for the specified resource.  'origin' and 'interface' must resolve to one or more LcResources
        that all have the same source specification.  That source archive gets indexed, and index resources are created
        for all the LcResources that were returned.

        Performs load_all() on the source archive, writes the archive to a compressed json file in the local index
        directory, and creates a new LcResource pointing to the JSON file.   Aborts if the index file already exists
        (override with force=True).
        :param origin:
        :param interface: [None]
        :param source: find_single_source input
        :param priority: [60] priority setting for the new index
        :param force: [False] if True, overwrite existing index
        :param strict: [True] whether to be strict
        :return:
        """
        if not force:
            try:
                ix = next(self.gen_interfaces(origin, itype='index', strict=False))
                return ix.origin
            except StopIteration:
                pass
        source = self._find_single_source(origin, interface, source=source, strict=strict)
        return self._index_source(source, priority, force=force)

    def cache_ref(self, origin, interface=None, source=None, static=False):
        source = self._find_single_source(origin, interface, source=source)
        self.create_source_cache(source, static=static)

    def create_source_cache(self, source, static=False):
        """
        Creates a cache of the named source's current contents, to speed up access to commonly used entities.
        source must be either a key present in self.sources, or a name or nickname found in self.names
        :param source:
        :param static: [False] create archives of a static archive (use to force archival of a complete database)
        :return:
        """
        res = next(r for r in self._resolver.resources_with_source(source))
        if res.static:
            if not static:
                print('Not archiving static resource %s' % res)
                return
            print('Archiving static resource %s' % res)
        res.check(self)
        res.make_cache(self.cache_file(self._localize_source(source)))

    def _background_for_origin(self, ref, strict=False):
        res = self.get_resource(ref, iface='exchange')
        inx_ref = self.index_ref(ref, interface='exchange', strict=strict)
        bk_file = self._localize_source(os.path.join(self.archive_dir, '%s_background.mat' % inx_ref))
        bk = LcResource(inx_ref, bk_file, 'Background', interfaces='background', priority=99,
                        save_after=True, _internal=True)
        bk.config = res.config
        bk.check(self)  # ImportError if antelope_background pkg not found;; also applies configs
        self.add_resource(bk)
        return bk.make_interface('background')  # when the interface is returned, it will trigger setup_bm

    def gen_interfaces(self, origin, itype=None, strict=False, ):
        """
        Override parent method to also create local backgrounds
        :param origin:
        :param itype:
        :param strict:
        :return:
        """
        for k in super(LcCatalog, self).gen_interfaces(origin, itype=itype, strict=strict):
            yield k

        if itype == 'background':
            if origin.startswith('local') or origin.startswith('test'):
                yield self._background_for_origin(origin, strict=strict)

    def create_descendant(self, origin, interface=None, source=None, force=False, signifier=None, strict=True,
                          priority=None, **kwargs):
        """

        :param origin:
        :param interface:
        :param source:
        :param force: overwrite if exists
        :param signifier: semantic descriptor for the new descendant (optional)
        :param strict:
        :param priority:
        :param kwargs:
        :return:
        """
        res = self.get_resource(origin, iface=interface, source=source, strict=strict)
        new_ref = res.archive.create_descendant(self.archive_dir, signifier=signifier, force=force)
        print('Created archive with reference %s' % new_ref)
        ar = res.archive
        prio = priority or res.priority
        self.add_existing_archive(ar, interfaces=res.interfaces, priority=prio, **kwargs)
        res.remove_archive()
