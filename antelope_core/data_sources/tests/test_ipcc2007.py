import unittest

from .test_aa_local import cat
from ..local import make_config, check_enabled


_run_test = check_enabled('ipcc2007')


if _run_test:
    cfg = make_config('ipcc2007')
    org = next(cfg.origins)


class GwpIpcc2007Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = cat.query(org)

    def test_resources_exist(self):
        self.assertIn(org, cat.origins)

    def test_num_entities(self):
        self.assertEqual(self.query.count('quantity'), 2)
        self.assertEqual(self.query.count('flow'), 0)

    def test_gwp(self):
        gwp = next(self.query.lcia_methods())
        self.assertEqual(gwp.name, 'Global Warming Air [kg CO2 eq] [TRACI 2.1]')
        self.assertEqual(len([k for k in gwp.factors()]), 91)


if __name__ == '__main__':
    unittest.main()
