"""
This file deals with tests for auto-loading and masquerading of LCIA methods
"""
import unittest

from .. import LcCatalog
from ...data_sources.local import make_config


cat = LcCatalog.make_tester()
cfg = make_config('ipcc2007')
org = next(cfg.origins)


def setUpModule():
    if org not in cat.origins:
        cat.add_resource(next(cfg.make_resources(org)))


class QuantityRefTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gwp = next(cat.query(org).lcia_methods(Name='Global Warming'))  # original ref
        cls.gwp_can = cat.query(org).get_canonical(cls.gwp)  # canonical (masqueraded) ref
        cls.gwp_ref = cat._qdb[cls.gwp.external_ref]  # locally stored
        cls.gwp_true = cat.get_archive(cls.gwp_ref.origin).get(cls.gwp_ref.external_ref)  # authentic entity

    def test_origins(self):
        self.assertEqual(self.gwp.origin, org)
        self.assertEqual(self.gwp_can.origin, org)
        self.assertEqual(self.gwp_can._query.origin, 'local.qdb')
        self.assertIs(self.gwp, self.gwp_ref)
        res = cat.get_resource(org)
        self.assertEqual(self.gwp_true.origin, res.archive.names[res.source])

    def test_identity(self):
        self.assertTrue(self.gwp_true.is_entity)
        self.assertFalse(self.gwp.is_entity)

    def test_factors(self):
        self.assertEqual(len([k for k in self.gwp.factors()]), 91)
        self.assertEqual(len([k for k in self.gwp_ref.factors()]), 91)
        self.assertEqual(len([k for k in self.gwp_true.factors()]), 91)

    def test_properties(self):
        self.assertTrue(self.gwp.has_property('indicator'))
        self.assertTrue(self.gwp_ref.has_property('indicator'))
        self.assertTrue(self.gwp_true.has_property('indicator'))

