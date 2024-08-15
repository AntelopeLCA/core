"""
What do we want to test here?
 flow vs flow ref, cf vs quantity_relation, .. same vs different origins?

"""
import unittest

from .. import Qdb
from ...implementations.quantity import ConversionReferenceMismatch

origin = 'test.origin'


class TestQuantityRelation(unittest.TestCase):

    f4_mj_kg = 35

    @classmethod
    def setUpClass(cls) -> None:
        qdb = Qdb.new()
        qi = qdb.make_interface('quantity')

        f4 = qdb.new_flow('Another mass flow', 'mass')
        qi.characterize(f4.link, f4.reference_entity, 'net calorific value', value=cls.f4_mj_kg)

        cls.f4 = f4
        cls.f6 = qdb.new_flow('An energetic conservation flow', 'net calorific value')
        cls.qdb = qdb

    @property
    def f4ref(self):
        return self.f4.make_ref(self.qdb.query)

    @property
    def f6ref(self):
        return self.f6.make_ref(self.qdb.query)

    def test_profile(self):
        self.assertEqual(len(list(self.f4ref.profile())), 1)

    def test_fwd_cf(self):
        self.assertEqual(self.f4.cf(self.f6.reference_entity), self.f4_mj_kg)

    def test_fwd_q_cf(self):
        mass = self.f4.reference_entity
        self.assertEqual(mass.cf(self.f6), 0.0)
        self.assertEqual(mass.cf(self.f4), 1.0)

        eng = self.f6.reference_entity
        self.assertEqual(eng.cf(self.f4), 35.0)
        self.assertEqual(eng.cf(self.f6), 1.0)

    def test_fwd_qr(self):
        mass = self.f4.reference_entity
        self.assertEqual(mass.quantity_relation(self.f4.name, self.f4.reference_entity, None).value, 1.0)
        with self.assertRaises(ConversionReferenceMismatch):
            mass.quantity_relation(self.f6.name, self.f6.reference_entity, None)

        eng = self.f6.reference_entity
        self.assertEqual(eng.quantity_relation(self.f4.name, mass, None).value, self.f4_mj_kg)
        self.assertEqual(eng.quantity_relation(self.f6.name, self.f6.reference_entity, None).value, 1.0)

    def test_rev_cf(self):
        mass = self.f4.reference_entity
        eng = self.f6.reference_entity
        self.assertEqual(mass.quantity_relation(self.f4.name, eng, None).value,
                         1.0 / self.f4_mj_kg)
        with self.assertRaises(ConversionReferenceMismatch):
            eng.quantity_relation(self.f6.name, mass, None)


