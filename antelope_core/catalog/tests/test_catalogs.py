
from .. import LcCatalog
from ...lc_resource import LcResource
from ...catalog_query import CatalogQuery, READONLY_INTERFACE_TYPES
from ...archives.tests import basic_archive_src


import os
import unittest


uslci_fg = LcResource('test.uslci', '/data/LCI/USLCI/USLCI_Processes_ecospold1.zip', 'EcospoldV1Archive',
                      interfaces='inventory',
                      priority=40,
                      static=False,
                      prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_fg_dup = LcResource('test.uslci', '/data/LCI/USLCI/USLCI_Processes_ecospold1.zip', 'EcospoldV1Archive',
                          interfaces='inventory',
                          priority=40,
                          static=False,
                          ringer=42,
                          prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_fg_bad = LcResource('test.uslci', '/data/LCI/USLCI/junk.zip', 'EcospoldV1Archive',
                          interfaces='inventory',
                          priority=40,
                          static=False,
                          prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_bg = LcResource('test.uslci.allocated', '/data/GitHub/lca-tools-datafiles/catalogs/uslci_clean_allocated.json.gz',
                      'json',
                      interfaces=READONLY_INTERFACE_TYPES,
                      priority=90,
                      static=True)


test_resource = LcResource('test.basic', basic_archive_src, 'json',
                           interfaces=('basic', 'exchange'))


class LcCatalogFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cat = LcCatalog.make_tester()
        cls._cat.add_resource(uslci_fg)
        cls._cat.add_resource(uslci_bg)
        cls._cat.add_resource(test_resource)

    def test_resolver_index(self):
        self.assertSetEqual({r for r in self._cat.origins}, {'local.qdb', 'test.uslci', 'test.uslci.allocated',
                                                                'test.basic'})

    @unittest.skip  # this doesn't work at all-- priority (and resolver generally) still need to be tested
    def test_priority(self):
        # TODO!
        q = CatalogQuery('test.uslci', catalog=self._cat)
        p = q.get('Acetic acid, at plant')
        self.assertEqual(p.origin, 'test.uslci')

    @unittest.skip  # this depends on hard-coded paths -- op is tested perfectly well in the end to end tests anyway
    def test_inventory(self):
        q = self._cat.query('test.uslci')
        inv = [x for x in q.inventory('Acetic acid, at plant')]
        self.assertEqual(len(inv), 21)

    @unittest.skip  # this is obviously not written yet
    def test_find_source(self):
        """
        Need to determine a set of testing conditions that ensure the resolver.get_resource() works properly
        :return:
        """
        pass

    def test_add_delete_resource_1(self):
        """
        This adds a resource
        :return:
        """
        r = self._cat.new_resource('test.my.dummy', '/dev/null', 'LcArchive', interfaces='basic')
        self.assertIn('basic', r.interfaces)  # we used to switch this on by default for all impls, but now we don't
        self.assertIn('test.my.dummy', self._cat.origins)
        self.assertNotIn('test.my.doofus', self._cat.origins)

    def test_add_delete_resource_2(self):
        """
        This deletes the resource
        :return:
        """
        r = self._cat.get_resource('test.my.dummy')
        self.assertEqual(r.source, '/dev/null')
        self._cat.delete_resource(r)
        self.assertNotIn('test.my.dummy', self._cat.origins)
        self.assertFalse(os.path.exists(os.path.join(self._cat.resource_dir, r.origin)))

    def test_has_resource(self):
        """
        If a resource matches one that exists, has_resource should return True
        :return:
        """
        self.assertTrue(self._cat.has_resource(uslci_fg_dup))
        self.assertFalse(self._cat.has_resource(uslci_fg_bad))

    def test_local_resource(self):
        """
        Tests the procedure of generating and deleting internal resources
        :return:
        """
        inx = self._cat.index_ref(test_resource.origin)
        self.assertIn(inx, self._cat.origins)  # index ref is known
        res = self._cat.get_resource(inx)
        self.assertTrue(self._cat.has_resource(res))  # index resource is present
        self.assertTrue(res.source.startswith('$CAT_ROOT'))  # index resource has relative source path
        abs_path = self._cat.abs_path(res.source)
        self.assertTrue(os.path.isabs(abs_path))
        self.assertEqual(self._cat._localize_source(abs_path), res.source)  # abs_path and localize_source are inverse
        self.assertEqual(self._cat._index_file(test_resource.source), abs_path)  # abs_path is true index path
        self.assertTrue(os.path.exists(abs_path))  # abs_path exists
        self._cat.delete_resource(res, delete_source=True)
        self.assertFalse(os.path.exists(abs_path))  # abs_path removed

    # def test_lcia_db(self):


class LcCatalogReplace(unittest.TestCase):
    def test_replace(self):
        cat = LcCatalog.make_tester()
        cat.add_resource(uslci_fg)
        res = cat.get_resource('test.uslci')
        self.assertIsNone(res.init_args.get('ringer'))
        cat.add_resource(uslci_fg_dup, replace=False)
        res = cat.get_resource('test.uslci')
        self.assertIsNone(res.init_args.get('ringer'))
        cat.add_resource(uslci_fg_dup, replace=True)
        res = cat.get_resource('test.uslci')
        self.assertEqual(res.init_args.get('ringer'), 42)


if __name__ == '__main__':
    unittest.main()
