import unittest
from ...entities import LcFlow, LcQuantity
from ..lcia_engine import LciaEngine


class TestBiogenicCo2(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mass = LcQuantity.new('mass', 'kg', origin='test')

    def test_co2_name(self):
        f = LcFlow.new('carbon dioxide', ref_qty=self.mass)
        self.assertTrue(f.is_co2)
        self.assertFalse(f.quell_co2)

    def test_co2_spec(self):
        f = LcFlow.new('co2', ref_qty=self.mass, is_co2=True)
        self.assertTrue(f.is_co2)
        self.assertFalse(f.quell_co2)

    def test_co2_cas(self):
        f = LcFlow.new('carbondioxide', ref_qty=self.mass, casnumber='124389')
        self.assertTrue(f.is_co2)
        self.assertFalse(f.quell_co2)

    def test_co2_catch_cas(self):
        f = LcFlow.new('carbon 2-oxide', ref_qty=self.mass)
        self.assertFalse(f.is_co2)
        f['casnumber'] = '124389'
        self.assertTrue(f.is_co2)
        self.assertFalse(f.quell_co2)

    def test_co2_biogenic(self):
        f = LcFlow.new('co2, in air', ref_qty=self.mass)
        self.assertFalse(f.is_co2)
        self.assertFalse(f.quell_co2)
        f.is_co2 = True
        self.assertTrue(f.is_co2)
        self.assertTrue(f.quell_co2)

    def test_co2_lcia_engine(self):
        l = LciaEngine()
        co2 = LcFlow.new('co2, biotic', ref_qty=self.mass, synonyms='carbon dioxide', origin='test',
                         context='in air')
        self.assertFalse(co2.is_co2)
        l.add_flow(co2)
        self.assertTrue(co2.is_co2)
        self.assertTrue(co2.quell_co2)


if __name__ == '__main__':
    unittest.main()
