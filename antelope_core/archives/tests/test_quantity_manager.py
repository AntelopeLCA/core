import unittest
from ...entities import LcQuantity

from ..quantity_manager import QuantitySynonyms, QuantityManager, QuantityUnitMismatch, QuantityAlreadySet


def _dummy_q():
    return LcQuantity.new('A dummy quantity', 'dummy', Synonyms=['dumdum quantity', 'Qty Dummy'])


dummy_2 = LcQuantity.new('dumdum quantity', 'dummy', origin='test')
dummy_x = LcQuantity.new('A dummy quantity', 'dooms')


class QuantityTest(unittest.TestCase):
    def test_create_new(self):
        dummy_q = _dummy_q()
        qsyn = QuantitySynonyms.new(dummy_q)
        self.assertEqual(qsyn.unit, 'dummy')
        self.assertIs(qsyn.object, dummy_q)
        self.assertIs(qsyn.quantity, dummy_q)

    def test_assign(self):
        dummy_q = _dummy_q()
        qsyn = QuantitySynonyms()
        qsyn.quantity = dummy_q
        self.assertEqual(qsyn.name, dummy_q['Name'])

    def test_merge(self):
        dummy_q = _dummy_q()
        qsyn = QuantitySynonyms.new(dummy_q)
        qsyn2 = QuantitySynonyms.new(dummy_2)
        qsyn.add_child(qsyn2)
        self.assertTrue(qsyn.has_child(qsyn2))
        for k in qsyn2.terms:
            self.assertIn(k,  qsyn.object['Synonyms'])

    def test_already_set(self):
        dummy_q = _dummy_q()
        qsyn = QuantitySynonyms.new(dummy_q)
        with self.assertRaises(QuantityAlreadySet):
            qsyn.quantity = dummy_2

    def test_conflicting_unit(self):
        dummy_q = _dummy_q()
        qsyn = QuantitySynonyms.new(dummy_q)
        qsynx = QuantitySynonyms.new(dummy_x)
        with self.assertRaises(QuantityUnitMismatch):
            qsyn.add_child(qsynx)
        dummy_q['UnitConversion'][qsynx.unit] = 1.0
        qsyn.add_child(qsynx)
        self.assertIn(dummy_x.uuid, list(qsyn.terms))

    def test_serialize(self):
        pass

    def test_deserialize(self):
        pass


class QuantityManagerTest(unittest.TestCase):

    def test_create(self):
        dummy_q = _dummy_q()
        qmgr = QuantityManager()
        qmgr.add_quantity(dummy_q)
        self.assertIs(qmgr[dummy_q.external_ref], dummy_q)
        syns = [x for x in qmgr.synonyms(dummy_q.external_ref)]
        self.assertEqual(len(syns), 5)  # no origin --> no link

    def test_synonyms(self):
        qmgr = QuantityManager()
        qmgr.add_quantity(dummy_2)
        self.assertSetEqual(set(qmgr.synonyms(dummy_2.external_ref)),
                            {dummy_2['Name'], dummy_2.link, dummy_2.external_ref, str(dummy_2)})

    def test_child(self):
        dummy_q = _dummy_q()
        qmgr = QuantityManager()
        qmgr.add_quantity(dummy_q)
        qmgr.add_quantity(dummy_2)
        self.assertIs(qmgr[dummy_2.external_ref], dummy_q)

    def test_prune(self):
        dummy_q = _dummy_q()
        qmgr = QuantityManager()
        qmgr.add_quantity(dummy_q)
        qmgr.add_quantity(dummy_x)
        self.assertIs(qmgr[dummy_x['Name']], dummy_q)
        self.assertIs(qmgr[dummy_x.external_ref], dummy_x)

    def test_add_from_dict(self):
        pass


if __name__ == '__main__':
    unittest.main()
