"""
Change Log
2020-01-21: Bump count of flowables to 4005 bc of Ecoinvent synonym fix
"""

import unittest

from .. import LciaDb
from ..lcia_engine import NUM_DEFAULT_CONTEXTS
from ...entities import LcQuantity, LcFlow
from ...contexts import Context
from ...archives.tests.test_qdb import num_ref_quantities
from ..data.update_flowables import NUM_FLOWABLES


class LciaEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lcia = LciaDb.new()

    def test_0_init(self):
        self.assertEqual(len([x for x in self.lcia.query.contexts()]), NUM_DEFAULT_CONTEXTS + 1)  # add Intermediate Flows
        self.assertEqual(len([x for x in self.lcia.query.flowables()]), NUM_FLOWABLES)  # update flowables
        self.assertEqual(len([x for x in self.lcia.query.quantities()]), num_ref_quantities)
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

    def test_add_non_matching_context(self):
        """
        This test is actually really thorny because 'unspecified', on its own, returns NullContext as a forbidden
        term, which is different from 'elementary flows', which raises a KeyError.
        :return:
        """
        cx = self.lcia.tm['emissions to air']
        c0 = Context('Elementary flows')
        c1 = Context('Emission to air', parent=c0)
        c2 = Context('unspecified', parent=c1)
        c2.add_origin('test')
        self.assertIs(self.lcia.tm[c2], cx)

    def test_add_conflicting_contexts(self):
        c1 = self.lcia.tm.add_context(('emissions', 'emissions to water'))
        c2 = self.lcia.tm.add_context(('elementary flows', 'emissions to water'))
        self.assertIs(c1, c2)
        self.assertEqual(len(c1.seq), 3)


if __name__ == '__main__':
    unittest.main()
