import unittest

from antelope import UnknownOrigin
from .test_aa_local import cat
from ..local import make_config, check_enabled

try:
    import antelope_background
    lci = True
except ImportError:
    lci = False

etypes = ('quantity', 'flow', 'process')

_debug = True

if __name__ == '__main__':
    _run_uslci = check_enabled('uslci')
else:
    _run_uslci = check_enabled('uslci') or _debug


if _run_uslci:
    cfg = make_config('uslci')
    try:
        gwp = next(cat.query('lcia.ipcc').lcia_methods())
    except UnknownOrigin:
        gwp = False


class UsLciTestContainer(object):

    class UsLciTestBase(unittest.TestCase):
        """
        Base class Mixin contains tests common to both ecospold and olca implemenations.  We hide it inside a container
        so that it does not get run automatically by unittest.main().  Then we can add implementation-specific tests in
        the subclasses.

        Thanks SO! https://stackoverflow.com/questions/1323455

        """
        _atype = None
        _initial_count = (0, 0, 0)
        _bg_len = None
        _ex_len = None
        _test_case_lcia = 0.0
        _test_case_observed_flow = None
        _test_case_lcia_observed = 0.0

        _petro_name = 'Petroleum refining, at refinery [RNA]'

        _petro_rx_values = set()


        @property
        def reference(self):
            return '.'.join([cfg.prefix, self._atype])

        @property
        def inx_reference(self):
            return '.'.join([self.reference, 'index'])

        @property
        def query(self):
            return cat.query(self.reference)

        def test_00_resources_exist(self):
            self.assertIn(self.reference, cat.origins)

        def test_01_initial_count(self):
            ar = cat.get_archive(self.reference, strict=True)
            for i, k in enumerate(etypes):
                self.assertEqual(ar.count_by_type(k), self._initial_count[i])

        def test_10_index(self):
            inx_ref = cat.index_ref(self.reference, force=True)
            self.assertTrue(inx_ref.startswith(self.inx_reference))
            self.assertIn(inx_ref, cat.origins)

        def test_11_no_background(self):
            # forcing re-index should have deleted the prior background interface
            self.assertEqual(len(list(r for r in cat.resources(self.reference) if 'background' in r.interfaces)), 0)

        def _get_petro(self):
            return next(self.query.processes(Name='petroleum refining, at refinery'))

        def _preferred(self):
            yield self._get_petro()

        def test_12_get_petro(self):
            p = self._get_petro()
            self.assertEqual(p.name, self._petro_name)

        def test_20_inventory(self):
            p = self._get_petro()
            rx = [x for x in p.references()]
            inv = [x for x in p.inventory()]
            self.assertEqual(len(rx), len(self._petro_rx_values))
            self.assertEqual(len(inv), 51)

        def _get_fg_test_case_rx(self):
            p = next(self.query.processes(Name='Seedlings, at greenhouse, US PNW'))
            return p.reference()

        def _get_fg_test_case_lci(self):
            rx = self._get_fg_test_case_rx()
            return list(rx.process.lci(rx.flow.external_ref))

        def _get_fg_test_case_observed(self):
            rx = self._get_fg_test_case_rx()
            return rx.process.exchange_values(self._test_case_observed_flow)

        def test_21_exchange_relation(self):
            rx = self._get_fg_test_case_rx()
            k = next(self.query.flows(Name='CUTOFF Potassium fertilizer, production mix, at plant'))
            v = self.query.exchange_relation(rx.process.external_ref, rx.flow.external_ref, k.external_ref, 'Input')
            self.assertEqual(v, 0.000175)

        def test_22_petro_allocation(self):
            p = self._get_petro()
            self.assertEqual(len(p.reference_entity), len(self._petro_rx_values))
            rx_vals = set(round(p.reference_value(rx.flow), 6) for rx in p.references())
            self.assertSetEqual(rx_vals, self._petro_rx_values)

        @unittest.skipIf(lci is False, "no background")
        def test_30_bg_gen(self):
            preferred = list((rx.flow.external_ref, p.external_ref) for p in self._preferred() for rx in p.references())
            self.assertTrue(self.query.check_bg(reset=True, prefer=preferred))

        @unittest.skipIf(lci is False, "no background")
        def test_31_bg_length(self):
            self.assertEqual(len([k for k in self.query.background_flows()]), self._bg_len)
            self.assertEqual(len([k for k in self.query.exterior_flows()]), self._ex_len)

        @unittest.skipIf(lci is False, "no background")
        def test_32_lci_fg(self):
            lci = self._get_fg_test_case_lci()
            self.assertEqual(len(lci), 298 - self._bg_len)  # this works because the bg discrepancy shows up as cutoffs
            lead_vals = {1.5e-09, 2.3e-09, 0.0}
            self.assertSetEqual({round(x.value, 10) for x in lci if x.flow.name.startswith('Lead')}, lead_vals)

        @unittest.skipIf(lci is False, "no background")
        def test_40_lcia_fg(self):
            if gwp:
                lci = self._get_fg_test_case_lci()
                res = gwp.do_lcia(lci)
                self.assertAlmostEqual(res.total(), self._test_case_lcia)

        @unittest.skipIf(lci is False, "no background")
        def test_41_lcia_bg(self):
            if gwp:
                rx = self._get_fg_test_case_rx()
                res = rx.process.bg_lcia(gwp)
                self.assertAlmostEqual(res.total(), self._test_case_lcia)

        @unittest.skipIf(lci is False, "no background")
        def test_42_lcia_bg_observed(self):
            if gwp:
                rx = self._get_fg_test_case_rx()
                obs = self._get_fg_test_case_observed()
                res = rx.process.bg_lcia(gwp, observed=obs)
                self.assertAlmostEqual(res.total(), self._test_case_lcia_observed)


class UsLciEcospoldTest(UsLciTestContainer.UsLciTestBase):

    _atype = 'ecospold'
    _initial_count = (5, 97, 5)
    _bg_len = 38
    _ex_len = 3285
    _test_case_lcia = 0.0415466  # more robust bc of ocean freight??
    _test_case_observed_flow = '5233'
    _test_case_lcia_observed = 0.0247817

    _petro_rx_values = {0.037175, 0.049083, 0.051454, 0.051826, 0.059594, 0.061169, 0.112458, 0.252345, 0.570087}

    def test_get_by_id(self):
        f = self.query.get(2176)  # this flow was loaded via the config mechanism
        pvs = [k.value for k in f.profile()]
        self.assertGreaterEqual(len(pvs), 1)
        self.assertIn(11.111, pvs)


class UsLciOlcaTest(UsLciTestContainer.UsLciTestBase):

    _atype = 'olca'
    _initial_count = (8, 71, 3)  # 4 physical quantities + 4 alloc quantities
    _bg_len = 36
    _ex_len = 3680
    _test_case_lcia = .04110577
    _test_case_observed_flow = 'bc38e349-1ccf-3855-a615-a4f581ab875b'
    _test_case_lcia_observed = 0.02476284

    # volume unit is m3 in olca, versus l in ecospold
    _petro_rx_values = {4.9e-05, 5.2e-05, 0.000112, 0.000252, 0.00057, 0.037175, 0.051454, 0.059594, 0.061169}

    def _preferred(self):
        yield self._get_petro()
        yield self.query.get('cdc143eb-fff8-3618-85cd-bce83d96390f')  # veneer, at veneer mill, preferred for wood fuel


if __name__ == '__main__':
    unittest.main()
