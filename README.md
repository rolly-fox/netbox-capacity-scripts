# netbox-capacity-scripts

NetBox **custom scripts** maintained in Git. One repository can hold **many** unrelated scripts—each NetBox script module normally maps to **one `.py` file** here.

## Scripts (inventory)

| File | Primary class | Purpose |
|------|-----------------|--------|
| [`rack_capacity_report.py`](rack_capacity_report.py) | `RackCapacityReport` | Read-only rack capacity HTML report (utilization, free RU, mount spots, front/rear elevations). Writes under `media/script-reports/`. |
| [`rack_pdu_connectivity_audit.py`](rack_pdu_connectivity_audit.py) | `RackPduConnectivityAudit` | Read-only rack PDU audit: associated PDUs (site/location/role rules), outlet utilization, device power traces to PDU outlets. HTML + CSV under `media/script-reports/`. |

Add new rows here whenever you commit another script.

## Repository conventions

- **Placement:** Keep each script as **`*.py` in the repo root** (same level as this `README`). That keeps Data Source paths simple (`netbox-capacity-scripts/my_script.py`) and matches common NetBox uploads.
- **Naming:** Prefer **`snake_case.py`** describing what it does (e.g. `prefix_audit.py`). The **`class`** inside should match in spirit (e.g. `PrefixAudit(Script)`).
- **One module, multiple classes:** A single file may define several `Script` subclasses; optionally set `script_order = (FirstScript, SecondScript)` at module bottom per [NetBox custom scripts](https://docs.netbox.dev/customization/custom-scripts/). Prefer **one main script per file** unless they’re tightly related.
- **Secrets:** Never commit credentials; scripts should rely on NetBox auth and normal Django settings.

## Adding a new script later

1. Copy [`templates/script_stub.py`](templates/script_stub.py) to the repo root with a **new filename** (do not ship the stub unchanged to NetBox).
2. Rename the Python **class**, set **`Meta.name`** / **`Meta.description`**, implement **`run()`**.
3. **`git add`**, **`git commit`**, **`git push`** this repo.
4. In NetBox (**Customization → Scripts**), add or refresh the script module pointing at **that filename** (upload or Data Source sync).

## Existing script

### `rack_capacity_report.py`

- **Install:** Deploy the file via your NetBox scripts workflow (filesystem, upload, or Git Data Source).
- **Requires:** Standard NetBox custom script environment (`extras.scripts`, models you import).
