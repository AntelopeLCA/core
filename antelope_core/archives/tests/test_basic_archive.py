"""
This file tests BasicArchive operations that do not involve creating or adding entities-- only basic construction
of the archive and definition of catalog names.

For testing of actual entity-containing archives, look to test_base
"""

import unittest
import os
from uuid import uuid4

from antelope import local_ref, CatalogRef
# from ..entity_store import SourceAlreadyKnown
from ..basic_archive import BasicArchive


class SourceAlreadyKnown(Exception):
    """
    a retired exception that was held over from the days when archives had to do catalogs' work
    """


WORKING_FILE = os.path.join(os.path.dirname(__file__), 'test-basic-archive.json')
conflict_file = '/dummy/conflict/file'
test_ref = 'test.basic'
test_conflict = 'test.conflict'

test_elaborated_ref = 'test.basic.my.sub.version'

archive_json = {
  "@context": "https://bkuczenski.github.io/lca-tools-datafiles/context.jsonld",
  "catalogNames": {
    test_ref: [
      WORKING_FILE
    ]
  },
  "dataReference": test_ref,
  "dataSource": WORKING_FILE,
  "dataSourceType": "BasicArchive",
  "flows": [],
  "quantities": []
}


def setUpModule():
    ar = BasicArchive(WORKING_FILE, ref=test_ref)
    ar.write_to_file(WORKING_FILE, gzip=False)


def tearDownModule():
    os.remove(WORKING_FILE)


class BasicArchiveTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ar = BasicArchive(WORKING_FILE)

    def test_rewrite_name(self):
        """
        Discovered-- if we create an archive from an existing file, but without specifying the ref, then the
        EntityStore will convert that file to a local ref and use it.  But if the file contains a ref specification,
        we want that one to win. So we use it instead.
        :return:
        """
        self.assertEqual(self.ar.ref, local_ref(WORKING_FILE))
        self.ar.load_from_dict(archive_json)
        self.assertEqual(self.ar.ref, test_ref)

    def test_catalog_ref(self):
        my_id = str(uuid4())
        self.ar.add(CatalogRef('bogus.origin', my_id, entity_type='flow'))
        self.assertEqual(self.ar[my_id].uuid, my_id)

    @unittest.skip  # SourceAlreadyKnown is retired
    def test_conflicting_ref(self):
        """
        It's an error to instantiate an existing source with a new reference (why? because the source should know its
        own reference?).  If it is desired to load a source without knowing its reference, use BasicArchive.from_file()
        :return:
        """
        a = BasicArchive(WORKING_FILE, ref=test_conflict)
        with self.assertRaises(SourceAlreadyKnown):
            a.load_from_dict(archive_json)

    @unittest.skip  # SourceAlreadyKnown is retired
    def test_generic_ref(self):
        """
        If a static resource is created with a fully-qualified ref, (base.ref.index.YYYYMMDD) and then it is later
        instantiated with a less-specific ref (base.ref), that escalation should be permitted (i.e. when the source
        file with the embedded ref is loaded, the more specific ref should be ignored)
        :return:
        """
        a = BasicArchive(WORKING_FILE, ref=test_ref)
        with self.assertRaises(SourceAlreadyKnown):
            a.add_new_source(test_conflict, WORKING_FILE)
        a.add_new_source(test_elaborated_ref, WORKING_FILE)

    def test_conflicting_src(self):
        """
        On the other hand, one ref is allowed to have multiple sources so this should not cause any issues
        why are we allowed to mix sources in the same ref? because a ref is a data synthesis.
        :return:
        """
        a = BasicArchive(conflict_file, ref=test_ref)
        a.load_from_dict(archive_json)
        self.assertSetEqual(set(k for k in a.get_sources(test_ref)), {conflict_file, WORKING_FILE})


if __name__ == '__main__':
    unittest.main()
