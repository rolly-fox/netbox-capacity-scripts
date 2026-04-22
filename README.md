# netbox-capacity-scripts

NetBox custom scripts for rack capacity reporting.

## `rack_capacity_report.py`

- **Class:** `RackCapacityReport` (subclass of `extras.scripts.Script`)
- **Install:** copy into your NetBox scripts path, or register via your Data Source / Customization → Scripts workflow.
- **Behavior:** read-only; single-rack HTML report (utilization, free RU, 1U–4U mount counts, front/rear elevations) written under `MEDIA_ROOT/script-reports/`.

Requires NetBox with `dcim` and `extras.scripts` (typical NetBox custom script environment).
