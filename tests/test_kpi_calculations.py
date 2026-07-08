import math
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "flink-jobs" / "kpi_processor"))

from calculations.electrical import (  # noqa: E402
    apparent_power_kva,
    coefficient_of_variation,
    cumulative_delta,
    load_factor,
    phase_unbalance_percent,
    quality_flag,
    reactive_power_kvar,
)


class ElectricalCalculationTests(unittest.TestCase):
    def test_apparent_and_reactive_power(self):
        apparent = apparent_power_kva(80.0, 0.8)

        self.assertEqual(apparent, 100.0)
        self.assertEqual(reactive_power_kvar(80.0, apparent), 60.0)

    def test_cumulative_delta_handles_first_read_and_reset(self):
        self.assertEqual(cumulative_delta(1000.0, None), 0.0)
        self.assertEqual(cumulative_delta(1015.5, 1000.0), 15.5)
        self.assertEqual(cumulative_delta(7.0, 1015.5), 7.0)
        self.assertEqual(cumulative_delta(None, 1015.5), 0.0)

    def test_phase_unbalance_percent(self):
        self.assertAlmostEqual(phase_unbalance_percent([230.0, 240.0, 250.0]), 4.1666666667)
        self.assertIsNone(phase_unbalance_percent([0.0, 0.0, 0.0]))

    def test_voltage_stability_cv(self):
        self.assertEqual(coefficient_of_variation([230.0, 230.0, 230.0]), 0.0)
        self.assertTrue(math.isclose(coefficient_of_variation([230.0, 240.0, 250.0]), 0.03402069, rel_tol=1e-6))

    def test_load_factor_and_quality_flag(self):
        self.assertEqual(load_factor(50.0, 100.0), 0.5)
        self.assertIsNone(load_factor(50.0, 0.0))
        self.assertEqual(
            quality_flag(0.85, 2.0, 0.01, {"min_power_factor": 0.9}),
            "POOR_POWER_FACTOR",
        )
        self.assertEqual(
            quality_flag(0.95, 6.0, 0.01, {"max_thd_voltage": 5.0}),
            "HIGH_THD",
        )
        self.assertEqual(
            quality_flag(0.95, 2.0, 0.03, {"max_voltage_cv": 0.02}),
            "UNSTABLE_VOLTAGE",
        )


if __name__ == "__main__":
    unittest.main()
