"""
Change Log
2020-01-21: Bump count of flowables to 4005 bc of Ecoinvent synonym fix
"""

import unittest

from .. import LciaDb
from ...entities import LcQuantity, LcFlow


class LciaEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lcia = LciaDb.new()

    def test_0_init(self):
        self.assertEqual(len([x for x in self.lcia.query.contexts()]), 36)
        self.assertEqual(len([x for x in self.lcia.query.flowables()]), 4005)
        self.assertEqual(len([x for x in self.lcia.query.quantities()]), 25)
        self.assertEqual(len(self.lcia.tm._q_dict), 3)

    def test_1_add_characterization(self):
        rq = self.lcia.query.get_canonical('mass')
        qq = self.lcia.query.get_canonical('volume')

        cf = self.lcia.query.characterize('water', rq, qq, .001)
        self.assertEqual(cf.origin, self.lcia.ref)

    def test_2_cf(self):
        self.assertEqual(self.lcia.query.quantity_relation('water', 'mass', 'volume', None).value, .001)
        self.assertEqual(self.lcia.query.quantity_relation('water', 'volume', 'mass', None).value, 1000.0)

    def test_3_dup_mass(self):
        dummy = 'dummy_external_ref'
        dup_mass = LcQuantity(dummy, referenceUnit='kg', Name='Mass', origin='dummy.origin')
        self.lcia.tm.add_quantity(dup_mass)
        self.assertEqual(self.lcia.query.get_canonical(dummy), self.lcia.query.get_canonical('mass'))

    def test_add_flow(self):
        """
        In this case, the flow's synonyms should be added. But dummy flows' links are not added to synonym set.
        :return:
        """
        flow = LcFlow('Dummy Flow', ref_qty=self.lcia.tm.get_canonical('mass'), origin='test.origin')
        for k in ('phosphene', 'phxphn', '1234567'):
            flow._flowable.add_term(k)
        self.lcia.tm.add_flow(flow)
        self.assertEqual(self.lcia.tm._fm['1234-56-7'].name, 'test.origin/Dummy Flow')



if __name__ == '__main__':
    unittest.main()
