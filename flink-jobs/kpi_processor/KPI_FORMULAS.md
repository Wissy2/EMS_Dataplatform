# EMS KPI Formula Reference

The Excel workbook referenced in the request was not available in the supplied
attachments. The implementation therefore preserves the formulas explicitly
listed in the request and labels the remaining derived metrics as additional
industrial KPIs.

## Excel / Business Formulas From Request

- Apparent power: `S = abs(P / cos_phi)`.
- Reactive power: `Q = sqrt(S^2 - P^2)`.
- Cumulative meter consumption: `delta = current - previous`; if the meter
  resets and `current < previous`, the current value is treated as the first
  post-reset delta. The first observation for a key emits zero delta.
- Energy cost: `EnergyDelta * ElectricityTariff`.
- CO2: `EnergyDelta * EmissionFactor`.
- Steam consumption: delta of `steam_totalizer`.
- Fuel consumption: delta of `fuel_totalizer`.
- Water consumption: delta of `total_water_m3`.

## Streaming Adaptations

- KPIs use event-time tumbling windows with configurable duration.
- Equipment-level windows are computed first. Area, line, plant, and
  `energy_consumption` rows reuse those equipment windows to avoid recalculating
  electrical metrics from raw samples.
- Runtime and utilization use the ratio of running samples in the window. The
  ratio is multiplied by the configured window duration, which preserves the
  business meaning even when the sampling interval is not perfectly constant.
- Daily and monthly peaks/totals are represented as additional windowed output
  fields in the current job shape; they can be backed by dedicated daily/monthly
  windows when longer-running retention queries are required.

## Additional Industrial KPIs

- Voltage stability: coefficient of variation, `stddev_pop(avg_phase_voltage) /
  avg(avg_phase_voltage)`. This gives a scale-independent measure of phase
  voltage variability.
- Voltage and current unbalance: maximum phase deviation from the phase average,
  divided by the phase average and expressed as a percentage.
- Load factor: `average_power / peak_demand`.
- Demand factor: `peak_demand / configured_max_expected_power`.
- Reactive power ratio: `reactive_power / active_power`.
- Apparent power utilization: `apparent_power / configured_max_expected_power`.
- Rolling 15-minute peak demand: hopping event-time window.
- Rolling 60-minute energy: hopping event-time window.
- Voltage quality index, power quality index, and equipment health score:
  bounded 0-100 heuristic scores derived from power factor, THD, breaker
  status, and voltage stability thresholds.
