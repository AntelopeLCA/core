import unittest


from ..local import make_config
from ... import LcCatalog
from antelope import ExchangeRef
from antelope.refs.process_ref import ProcessRef
from antelope.refs.flow_ref import FlowRef

tc = make_config('traci')
traci_origin = next(tc.origins)


class DummyQuery:
    origin = 'test.dummy'
    def validate(self):
        return True


def generate_gwp_inventory(_cat):
    dq = DummyQuery()
    node = ProcessRef('aabbccdd', dq, Name='dummy GWP process')
    mass = _cat.get_canonical('mass')
    to_air = _cat.lcia_engine['to air']
    yield ExchangeRef(node, FlowRef(1, dq, reference_entity=mass, name='Carbon Dioxide'), 'Output', 2.2,
                      termination=to_air)
    yield ExchangeRef(node, FlowRef(2, dq, reference_entity=mass, name='Carbon Dioxide (biogenic)'), 'Output', 1.1,
                      termination=to_air)
    yield ExchangeRef(node, FlowRef(2, dq, reference_entity=mass, name='methane'), 'Output', 0.5,
                      termination=to_air)


class TestTraci(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cat = LcCatalog.make_tester()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.cat.__del__()

    def test_0_register_traci(self):
        tc.register_all_resources(self.cat)
        self.assertIn(traci_origin, self.cat.origins)

    def test_1_load_traci(self):
        qs = list(self.cat.query(traci_origin).lcia_methods())
        self.assertEqual(len(qs), 10)

    def test_2_traci_factors(self):
        gwp = self.cat.query(traci_origin).get('Global Warming Air')
        cfs = list(gwp.factors())
        self.assertEqual(len(cfs), 91)

    def test_3_traci_lcia(self):
        gwp = self.cat.query(traci_origin).get('Global Warming Air')
        inv = list(generate_gwp_inventory(self.cat))
        self.assertEqual(gwp.do_lcia(inv).total(), 15.8)
        gwp['quell_biogenic_co2'] = True
        self.assertEqual(gwp.do_lcia(inv).total(), 14.7)


if __name__ == '__main__':
    unittest.main()
