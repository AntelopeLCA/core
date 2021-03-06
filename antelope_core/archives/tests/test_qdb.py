import unittest

from antelope import EntityNotFound

from ...archives import Qdb
from ...entities.tests.base_testclass import BasicEntityTest

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'


class QdbTestCase(BasicEntityTest):
    """
    lots to test here
    """
    @classmethod
    def setUpClass(cls):
        super(QdbTestCase, cls).setUpClass()
        cls._qdb = Qdb.new()

    def test_0_qdb_initialization(self):
        self.assertEqual(self._qdb.count_by_type('quantity'), 25)

    def test_mass(self):
        self.assertEqual(self._qdb.query.get_canonical('mass').uuid, mass_uuid)

    def test_canonical(self):
        m = self._qdb.query.get_canonical('mass')
        self.assertEqual(m['Name'], 'Mass')

    def test_kwh_mj_conversion(self):
        elec = next(self._qdb.search('flow', Name='electricity'))
        ncv = self._qdb.query.get_canonical('net calorific value')
        self.assertEqual(ncv.cf(elec), 3.6)

    def test_make_basic_interface(self):
        bi = self._qdb.query.get('93a60a56-a3c8-11da-a746-0800200c9a66')
        self.assertEqual(bi.entity_type, 'quantity')
        self.assertEqual(bi.origin, 'elcd.3.2')

    def test_external_quantity_lookup(self):
        """
        Some quantities should lookup, others should not
        :return:
        """
        for k in self.A.query.quantities():
            if k.external_ref in ('f6811440-ee37-11de-8a39-0800200c9a66', 'e288b5d2-9fcc-4a10-b13c-440786090f43'):
                # 'energy' (unspecified) and 'emissive coolness' respectively
                with self.assertRaises(EntityNotFound):
                    self._qdb.query.get_canonical(k.external_ref)
                self._qdb.add_entity_and_children(k)  # we commented out as foolhardy the code that auto-adds quantities in get_canonical
                self.assertIs(self._qdb.query.get_canonical(k.external_ref), k)
            else:
                qc = self._qdb.query.get_canonical(k)
                self.assertEqual(qc.origin, 'local.qdb')


if __name__ == '__main__':
    unittest.main()
