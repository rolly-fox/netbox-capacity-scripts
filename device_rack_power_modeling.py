"""
NetBox Enterprise / NetBox ≥4.x custom script — Rack device power modeling helper

Ensures modeled power intake by **creating missing** PowerPorts/outlets from the device type;
without templates, applies **FALLBACK_POWER_PORT_NAMES** and may **rename** legacy stub names only.
Otherwise does not edit existing components (draws, labels, …). Discovers in-rack PDU
devices, connects spare PowerPorts to available PDU outlets, and tags involved cables for audit.

Optimized for usage patterns described in repository docs; validated against NetBox v4.1 cabling.

Deploy via Customization → Scripts (upload or Git Data Source).
"""

from __future__ import annotations

import re

from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db.models import Q

from dcim.choices import CableTypeChoices, LinkStatusChoices
from dcim.models import Cable, CableTermination, Device, DeviceType, Location, PowerOutlet, PowerPort, Rack
from dcim.models import Region, Site
from extras.models import Tag
from extras.scripts import ObjectVar, Script
from utilities.exceptions import AbortScript


# -----------------------------------------------------------------------------
# Configuration (edit per deployment)
# -----------------------------------------------------------------------------
# Comma-separated DeviceRole **names or slugs** that identify rack PDUs (case-insensitive match).
# Add every spelling used in your NetBox tenant (examples below are common).
POWER_DEVICE_ROLE_NAMES = (
    "PDU,Rack PDU,rack pdu,pdu,Power Distribution,Power Strip,Vertical PDU,UPS,UPS PDU"
)

# If no devices match POWER_DEVICE_ROLE_NAMES, discover PDUs as **any other device in the same rack**
# that has at least one modeled PowerOutlet (still rack-scoped; selected device excluded).
# Set False to require a matching DeviceRole only (stricter).
PDU_DISCOVERY_FALLBACK_BY_OUTLETS = True

TAG_SLUG = "power-audit-required"
TAG_NAME = "power-audit-required"
TAG_COLOR = "ff9800"

# Defaults applied **only when instantiating new** PowerPorts (below). Existing PowerPorts are never modified.
MAX_POWER_WATTS = 500
ALLOCATED_POWER_WATTS = 400

# When the device type defines **no** power port templates, ensure these PowerPort names exist (order preserved).
# Default dual-feed naming without templates:
FALLBACK_POWER_PORT_NAMES = ("PSU A", "PSU B")

# Ports whose names match these **exact strings** may be **renamed in place** (name only; draws/types unchanged)
# to satisfy FALLBACK_POWER_PORT_NAMES in order — e.g. legacy **PS1** becomes **PSU A**, then **PSU B** is created.
# Adjust if your tenant used different placeholder names.
FALLBACK_LEGACY_POWER_PORT_NAMES = ("PS1", "PS2")

# Safety: Do **not** edit arbitrary existing DCIM components. Creation targets missing names only.
# Exception: PowerPorts listed in FALLBACK_LEGACY_POWER_PORT_NAMES may be renamed to match fallback targets.
# PowerOutlets and PowerPorts not in the legacy list are otherwise untouched (draws, labels, types, …).

# Match device PowerPorts to PDU outlets using NetBox **Cable.clean()** (both A↔B orientations).
# True fixes false “no compatible outlet” when port/outlet **type** strings differ (PowerPortType vs
# PowerOutletType enums). Set False to use legacy string equality only (usually too strict).
PDU_MATCH_OUTLETS_USING_CABLE_VALIDATION = True

# After each **new** cable is saved, set **Cable.label** to the cable's primary key as a decimal string
# (e.g. cable PK 58478 → label `"58478"`). Requires a second save so the PK exists.
SET_CABLE_LABEL_FROM_PK = True
# If True, replace an existing label when updating from PK; if False, only set when label is empty.
CABLE_LABEL_OVERWRITE_EXISTING = True

# PSU **A** → PDU device whose **name** contains PDU_RED_NAME_SUBSTRING; PSU **B** → PDU matching
# PDU_BLUE_NAME_SUBSTRING (case-insensitive). Both feeds use the **same PowerOutlet.name** on RED vs BLUE (parity).
# Disable to restore legacy “first available outlet across all PDUs” behavior.
PDU_RED_BLUE_PARITY_CABLING = True
PDU_RED_NAME_SUBSTRING = "red"
PDU_BLUE_NAME_SUBSTRING = "blue"

# Typical rack naming: ``DC-1415_PDU RED 01`` / ``DC-1415_PDU BLUE 01`` — includes **pdu** and **red**/**blue**.
# When True, only PDUs whose **name** contains PDU_DEVICE_NAME_TOKEN **and** the color token are considered first.
# If that yields no RED or no BLUE candidate, the script automatically falls back to **color substring only**
# (same PDU_RED_NAME_SUBSTRING / PDU_BLUE_NAME_SUBSTRING), so shorter names still work.
PDU_RED_BLUE_PREFER_PDU_TOKEN_IN_NAME = True
PDU_DEVICE_NAME_TOKEN = "pdu"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def normalized_fallback_power_port_names() -> tuple[str, ...]:
    """Dedupe while preserving order; empty entries skipped."""
    raw = FALLBACK_POWER_PORT_NAMES
    if isinstance(raw, str):
        raw = (raw,)
    seen: dict[str, None] = {}
    out: list[str] = []
    for n in raw:
        s = (n or "").strip()
        if not s or s in seen:
            continue
        seen[s] = None
        out.append(s)
    return tuple(out)


def normalized_legacy_power_port_names() -> frozenset[str]:
    raw = FALLBACK_LEGACY_POWER_PORT_NAMES
    if isinstance(raw, str):
        raw = (raw,)
    return frozenset((n or "").strip() for n in raw if (n or "").strip())


def rename_power_port_name(pp: PowerPort, new_name: str, *, commit: bool) -> None:
    """Rename port in place (changelog snapshot when supported). Caller ensures uniqueness on device."""
    if hasattr(pp, "snapshot"):
        pp.snapshot()
    pp.name = new_name
    pp.full_clean()
    if commit:
        pp.save()


def parse_role_tokens(power_role_names: str) -> list[str]:
    if not power_role_names:
        return []
    return [t.strip().lower() for t in power_role_names.split(",") if t.strip()]


def iter_power_port_templates(device_type: DeviceType):
    """NetBox versions differ on RelatedManager attribute name (underscore vs lowercase)."""
    mgr = getattr(device_type, "power_port_templates", None)
    if mgr is not None:
        yield from mgr.all()
        return
    mgr = getattr(device_type, "powerporttemplates", None)
    if mgr is not None:
        yield from mgr.all()


def iter_power_outlet_templates(device_type: DeviceType):
    mgr = getattr(device_type, "power_outlet_templates", None)
    if mgr is not None:
        yield from mgr.all()
        return
    mgr = getattr(device_type, "poweroutlettemplates", None)
    if mgr is not None:
        yield from mgr.all()


def resolve_obj(model, value):
    """ObjectVar passes a model instance (UI) or pk (API)."""
    if isinstance(value, model):
        return value
    return model.objects.get(pk=value)


def peer_power_outlet(port: PowerPort) -> PowerOutlet | None:
    peers = getattr(port, "link_peers", []) or []
    for p in peers:
        if isinstance(p, PowerOutlet):
            return p
    return None


def types_compatible_string(port_type: str, outlet_type: str) -> bool:
    """Legacy: same string only. PowerPort and PowerOutlet types often use different choice enums."""
    pt = (port_type or "").strip()
    ot = (outlet_type or "").strip()
    if not pt or not ot:
        return True
    return pt == ot


def build_validated_power_cable(pp: PowerPort, outlet: PowerOutlet) -> Cable | None:
    """
    Return a Cable instance that passes full_clean(), or None.

    Tries power port on side A then B — some deployments accept only one orientation.
    """
    for a_terminations, b_terminations in (([pp], [outlet]), ([outlet], [pp])):
        cable = Cable(
            type=CableTypeChoices.TYPE_POWER,
            status=LinkStatusChoices.STATUS_CONNECTED,
            a_terminations=a_terminations,
            b_terminations=b_terminations,
        )
        try:
            cable.full_clean()
            return cable
        except ValidationError:
            continue
    return None


def pop_first_connectable_outlet(
    pp: PowerPort,
    outlet_pool: list[PowerOutlet],
    *,
    use_cable_validation: bool,
) -> tuple[Cable, PowerOutlet] | None:
    """Remove and return the first outlet that can be cabled to this port (deterministic pool order)."""
    for idx, cand in enumerate(outlet_pool):
        if not use_cable_validation:
            if not types_compatible_string(pp.type, cand.type):
                continue
            cable = build_validated_power_cable(pp, cand)
        else:
            cable = build_validated_power_cable(pp, cand)
        if cable is None:
            continue
        outlet = outlet_pool.pop(idx)
        return cable, outlet
    return None


def set_cable_label_from_pk(cable: Cable, *, overwrite_existing: bool) -> bool:
    """
    Set Cable.label to str(cable.pk). Returns True if the row was updated.

    Uses snapshot() before edit when available so change logging stays correct.
    """
    if not cable.pk:
        return False
    new_label = str(int(cable.pk))
    current = (getattr(cable, "label", None) or "").strip()
    if current and not overwrite_existing:
        return False
    if new_label == current:
        return False
    if hasattr(cable, "snapshot"):
        cable.snapshot()
    cable.label = new_label
    cable.full_clean()
    cable.save()
    return True


def cables_touching_power_port(pp: PowerPort) -> list[Cable]:
    ct = ContentType.objects.get_for_model(PowerPort)
    qs = CableTermination.objects.filter(
        termination_type=ct,
        termination_id=pp.pk,
    ).select_related("cable")
    return list({t.cable for t in qs if t.cable_id})


def sort_pdus_deterministic(pdus: list[Device]) -> list[Device]:
    return sorted(pdus, key=lambda d: ((d.name or "").lower(), d.pk))


def pdu_outlets_by_name(pdu: Device) -> dict[str, PowerOutlet]:
    return {o.name: o for o in pdu.poweroutlets.all()}


def outlet_name_sort_key(name: str):
    n = name or ""
    try:
        return (0, int(str(n).strip()))
    except ValueError:
        return (1, n.lower())


def pick_red_blue_pdu_devices(
    pdus: list[Device],
    red_substring: str,
    blue_substring: str,
    *,
    prefer_pdu_token: bool = True,
    pdu_name_token: str = "pdu",
) -> tuple[Device | None, Device | None, str]:
    """
    Pick RED then BLUE PDU devices.

    Prefers names like ``SITE_PDU RED 01`` (contains **pdu** + **red**). If no match for a leg,
    falls back to red/blue substring only (still case-insensitive).

    Returns (red_pdu, blue_pdu, short note for logging).
    """
    rl = (red_substring or "").strip().lower()
    bl = (blue_substring or "").strip().lower()
    tok = (pdu_name_token or "").strip().lower()
    if not rl or not bl:
        return None, None, ""

    def lowered(d: Device) -> str:
        return (d.name or "").lower()

    loose_reds = [d for d in pdus if rl in lowered(d)]
    loose_blues = [d for d in pdus if bl in lowered(d)]

    if prefer_pdu_token and tok:
        strict_reds = [d for d in loose_reds if tok in lowered(d)]
        strict_blues = [d for d in loose_blues if tok in lowered(d)]
    else:
        strict_reds = []
        strict_blues = []

    reds = strict_reds if strict_reds else loose_reds
    blues = strict_blues if strict_blues else loose_blues

    parts: list[str] = []
    if strict_reds:
        parts.append("RED=PDU-style")
    elif loose_reds:
        parts.append("RED=substring-only")
    if strict_blues:
        parts.append("BLUE=PDU-style")
    elif loose_blues:
        parts.append("BLUE=substring-only")

    if not reds or not blues:
        return None, None, "; ".join(parts) if parts else ""

    note = "; ".join(parts)
    return (
        sort_pdus_deterministic(reds)[0],
        sort_pdus_deterministic(blues)[0],
        note,
    )


def partition_psu_ab_ports(
    ports: list[PowerPort],
) -> tuple[PowerPort | None, PowerPort | None, list[str]]:
    """
    Map two rack PowerPorts to A/B for RED/BLUE parity.

    Prefer names **PSU A** / **PSU B** (flexible spacing). Otherwise use the first two ports in
    deterministic name/pk order (e.g. PS1 + PS2).
    """
    warns: list[str] = []
    pa = pb = None
    for p in ports:
        n = (p.name or "").strip().lower()
        if re.fullmatch(r"psu\s*a", n) or n == "psu-a":
            pa = p
        elif re.fullmatch(r"psu\s*b", n) or n == "psu-b":
            pb = p
    if pa is not None and pb is not None:
        return pa, pb, warns
    ordered = sorted(ports, key=lambda x: ((x.name or "").lower(), x.pk))
    if len(ordered) >= 2:
        if len(ordered) > 2:
            warns.append(
                f"More than two PowerPorts on device — parity uses **{ordered[0].name}** (A) and "
                f"**{ordered[1].name}** (B); name others **PSU A** / **PSU B** to control selection."
            )
        return ordered[0], ordered[1], warns
    if len(ordered) == 1:
        return ordered[0], None, warns
    return None, None, warns


def sorted_common_outlet_names(red_pdu: Device, blue_pdu: Device) -> list[str]:
    rm = pdu_outlets_by_name(red_pdu)
    bm = pdu_outlets_by_name(blue_pdu)
    common = set(rm.keys()) & set(bm.keys())
    return sorted(common, key=outlet_name_sort_key)


def outlet_available(o: PowerOutlet) -> bool:
    return not getattr(o, "_occupied", False)


def ensure_audit_tag(
    *,
    slug: str,
    name: str,
    color: str,
    commit: bool,
) -> Tag:
    if commit:
        tag, _ = Tag.objects.get_or_create(
            slug=slug,
            defaults={"name": name, "color": color},
        )
        return tag
    return Tag(slug=slug, name=name, color=color)


def apply_audit_tag_to_cable(cable: Cable, tag: Tag, *, commit: bool, tag_slug: str) -> bool:
    """Return True if the tag was missing and would be / was added."""
    if tag.pk:
        exists = cable.tags.filter(pk=tag.pk).exists()
    else:
        exists = cable.tags.filter(slug=tag_slug).exists()
    if exists:
        return False
    if commit:
        cable.tags.add(tag)
    return True


def discover_in_rack_pdus_by_role(rack: Rack, tokens: list[str]) -> list[Device]:
    """PDU candidates by DeviceRole name/slug."""
    if not tokens:
        return []
    role_q = Q()
    for t in tokens:
        role_q |= Q(role__name__iexact=t) | Q(role__slug__iexact=t)
    qs = (
        Device.objects.filter(rack_id=rack.pk)
        .filter(role_q)
        .select_related("role", "device_type")
        .prefetch_related("poweroutlets")
    )
    return sort_pdus_deterministic(list(qs))


def discover_in_rack_pdus_by_outlets(rack: Rack, *, exclude_device_id: int) -> list[Device]:
    """PDU candidates: other rack devices that supply power (have PowerOutlet components)."""
    qs = (
        Device.objects.filter(rack_id=rack.pk)
        .exclude(pk=exclude_device_id)
        .filter(poweroutlets__isnull=False)
        .distinct()
        .select_related("role", "device_type")
        .prefetch_related("poweroutlets")
    )
    return sort_pdus_deterministic(list(qs))


def resolve_rack_pdus(
    rack: Rack,
    *,
    role_tokens: list[str],
    exclude_device_id: int,
    fallback_by_outlets: bool,
) -> tuple[list[Device], str]:
    """
    Returns (sorted PDU devices, short description of how they were discovered).
    """
    pdus = discover_in_rack_pdus_by_role(rack, role_tokens)
    if pdus:
        return pdus, "device role matches POWER_DEVICE_ROLE_NAMES"

    if fallback_by_outlets:
        pdus = discover_in_rack_pdus_by_outlets(rack, exclude_device_id=exclude_device_id)
        if pdus:
            return (
                pdus,
                "fallback: rack devices with modeled PowerOutlets (POWER_DEVICE_ROLE_NAMES had no match)",
            )

    return [], ""


def build_sorted_outlet_pool(pdus: list[Device]) -> list[PowerOutlet]:
    pool: list[PowerOutlet] = []
    for pdu in pdus:
        outlets = sorted(
            pdu.poweroutlets.all(),
            key=lambda o: ((o.name or "").lower(), o.pk),
        )
        pool.extend(outlets)
    return pool


class DeviceRackPowerModeling(Script):
    """Wizard-style selection + automatic PDU cabling for rack-mounted gear."""

    class Meta(Script.Meta):
        name = "Device rack power modeling (PDU attach + audit tags)"
        description = (
            "Pick Region → Site → Location → Rack → Device, ensure PowerPorts/outlets exist, "
            "connect to in-rack PDU outlets, tag cables power-audit-required."
        )
        commit_default = False
        field_order = ("region", "site", "location", "rack", "device")

    region = ObjectVar(model=Region, description="Geographic / organizational region")

    site = ObjectVar(
        model=Site,
        query_params={"region_id": "$region"},
        description="Site within the selected region",
    )

    location = ObjectVar(
        model=Location,
        query_params={"site_id": "$site"},
        description="Location within the selected site (racks must reference this location)",
    )

    rack = ObjectVar(
        model=Rack,
        query_params={
            "site_id": "$site",
            "location_id": "$location",
        },
        description="Rack within the selected site and location",
    )

    device = ObjectVar(
        model=Device,
        query_params={
            "rack_id": "$rack",
        },
        description="Device installed in the selected rack",
    )

    def run(self, data, commit):
        summary_lines: list[str] = []
        warnings: list[str] = []
        # Current DB port name → intended display name (after planned legacy renames in no-template fallback).
        fallback_rename_display: dict[str, str] = {}

        region = resolve_obj(Region, data["region"])
        site = resolve_obj(Site, data["site"])
        location = resolve_obj(Location, data["location"])
        rack = resolve_obj(Rack, data["rack"])
        device = resolve_obj(Device, data["device"])

        role_tokens = parse_role_tokens(POWER_DEVICE_ROLE_NAMES)

        # --- Validation (strict path) ---
        if site.region_id != region.pk:
            raise AbortScript(
                f"Site “{site}” is not in region “{region}” (site.region_id={site.region_id}, "
                f"expected {region.pk})."
            )
        if location.site_id != site.pk:
            raise AbortScript(f"Location “{location}” does not belong to site “{site}”.")

        if rack.site_id != site.pk:
            raise AbortScript(f"Rack “{rack}” does not belong to site “{site}”.")
        if rack.location_id != location.pk:
            raise AbortScript(
                f"Rack “{rack}” is not in location “{location}” "
                f"(rack.location_id={rack.location_id}, expected {location.pk})."
            )

        try:
            device.refresh_from_db()
        except Device.DoesNotExist:
            raise AbortScript("Selected device no longer exists.")

        if device.rack_id != rack.pk:
            raise AbortScript(
                f"Device “{device.name}” is not installed in rack “{rack}” "
                f"(device.rack_id={device.rack_id}, expected {rack.pk})."
            )
        if device.site_id != site.pk:
            raise AbortScript(f"Device site mismatch (device.site_id={device.site_id}).")

        if device.location_id is not None and device.location_id != location.pk:
            raise AbortScript(
                f"Device location_id={device.location_id} does not match selected location pk={location.pk}."
            )

        self.log_info(f"Selected device: **{device.name}** (ID {device.pk}) in rack **{rack}**.")

        pdus, pdu_discovery_desc = resolve_rack_pdus(
            rack,
            role_tokens=role_tokens,
            exclude_device_id=device.pk,
            fallback_by_outlets=PDU_DISCOVERY_FALLBACK_BY_OUTLETS,
        )
        pdu_ids = {p.pk for p in pdus}

        if not pdus:
            raise AbortScript(
                "No rack PDU devices found in this rack. "
                "Fix one of: (1) Set **POWER_DEVICE_ROLE_NAMES** to match your PDU **DeviceRole** "
                "(Organization → Device Roles — use exact name or slug); "
                "(2) Model **PowerOutlets** on PDU device(s) in this rack; "
                "(3) Enable **PDU_DISCOVERY_FALLBACK_BY_OUTLETS** if outlet-based fallback is acceptable "
                f"(currently {PDU_DISCOVERY_FALLBACK_BY_OUTLETS})."
            )

        if pdu_discovery_desc.startswith("fallback"):
            self.log_warning(
                "PDU discovery: **no DeviceRole matched** POWER_DEVICE_ROLE_NAMES — using **outlet fallback** "
                "(other rack devices with PowerOutlets). Add your PDU role string(s) to the script config for "
                "explicit matching."
            )

        pdu_names = ", ".join(d.name or f"#{d.pk}" for d in pdus)
        summary_lines.append(f"- **PDU discovery:** {pdu_discovery_desc}")
        summary_lines.append(f"- **PDU(s) identified ({len(pdus)}):** {pdu_names}")

        tag = ensure_audit_tag(
            slug=TAG_SLUG,
            name=TAG_NAME,
            color=TAG_COLOR,
            commit=commit,
        )

        dt = device.device_type
        ports_saved = 0
        ports_planned = 0
        ports_renamed = 0
        rename_planned = 0
        outlets_saved = 0
        outlets_planned = 0

        # --- Ensure PowerPorts from templates (+ fallback); legacy renames only in no-template fallback ---
        existing_by_name = {p.name: p for p in device.powerports.all()}
        ppt = list(iter_power_port_templates(dt))

        if ppt:
            for t in ppt:
                if t.name in existing_by_name:
                    # Already modeled: preserve all fields (power, label, type, description, …).
                    continue
                self.log_info(f"Would create PowerPort **{t.name}** from device type template.")
                if commit:
                    pp = PowerPort(
                        device=device,
                        name=t.name,
                        label=getattr(t, "label", "") or "",
                        type=t.type or "",
                        maximum_draw=MAX_POWER_WATTS,
                        allocated_draw=ALLOCATED_POWER_WATTS,
                    )
                    pp.full_clean()
                    pp.save()
                    ports_saved += 1
                    existing_by_name[t.name] = pp
                else:
                    ports_planned += 1
        else:
            fb_names = normalized_fallback_power_port_names()
            if fb_names:
                legacy_allow = normalized_legacy_power_port_names()
                desired_set = set(fb_names)
                ports_list = list(device.powerports.all())
                by_name_fb = {p.name: p for p in ports_list}
                needed = [n for n in fb_names if n not in by_name_fb]
                legacy_candidates = sorted(
                    [
                        p
                        for p in ports_list
                        if p.name in legacy_allow and p.name not in desired_set
                    ],
                    key=lambda p: ((p.name or "").lower(), p.pk),
                )
                take = min(len(needed), len(legacy_candidates))
                for i in range(take):
                    tgt = needed[i]
                    lp = legacy_candidates[i]
                    src = lp.name
                    fallback_rename_display[src] = tgt
                    if commit:
                        rename_power_port_name(lp, tgt, commit=True)
                        ports_renamed += 1
                        self.log_info(f"Renamed PowerPort **{src}** → **{tgt}** (no template fallback).")
                    else:
                        rename_planned += 1
                        self.log_info(f"Would rename PowerPort **{src}** → **{tgt}** (no template fallback).")

                if commit:
                    by_name_fb = {p.name: p for p in device.powerports.all()}
                    needed_after = [n for n in fb_names if n not in by_name_fb]
                else:
                    sim_names = set(by_name_fb.keys())
                    for i in range(take):
                        sim_names.discard(legacy_candidates[i].name)
                        sim_names.add(needed[i])
                    needed_after = [n for n in fb_names if n not in sim_names]

                if needed_after:
                    self.log_info(
                        "No power port templates on device type; "
                        f"would create fallback PowerPort(s): **{', '.join(needed_after)}** "
                        "(see **FALLBACK_POWER_PORT_NAMES**)."
                    )
                    for pname in needed_after:
                        if commit:
                            pp = PowerPort(
                                device=device,
                                name=pname,
                                label="",
                                type="",
                                maximum_draw=MAX_POWER_WATTS,
                                allocated_draw=ALLOCATED_POWER_WATTS,
                            )
                            pp.full_clean()
                            pp.save()
                            ports_saved += 1
                            existing_by_name[pname] = pp
                        else:
                            ports_planned += 1

        # --- Ensure PowerOutlets only when templates exist on device type; never update existing outlets ---
        po_templates = list(iter_power_outlet_templates(dt))
        existing_o = {o.name: o for o in device.poweroutlets.all()}
        for t in po_templates:
            if t.name in existing_o:
                continue
            parent_pp = None
            if getattr(t, "power_port_id", None):
                tpl_pp = t.power_port
                parent_pp = PowerPort.objects.filter(device=device, name=tpl_pp.name).first()
            self.log_info(f"Would create PowerOutlet **{t.name}** from template.")
            if commit:
                outlet = PowerOutlet(
                    device=device,
                    name=t.name,
                    label=getattr(t, "label", "") or "",
                    type=t.type or "",
                    feed_leg=t.feed_leg or "",
                    power_port=parent_pp,
                )
                outlet.full_clean()
                outlet.save()
                outlets_saved += 1
                existing_o[t.name] = outlet
            else:
                outlets_planned += 1

        power_ports = sorted(
            PowerPort.objects.filter(device=device),
            key=lambda p: ((p.name or "").lower(), p.pk),
        )

        fb_expect = len(normalized_fallback_power_port_names())
        if (
            not commit
            and not ppt
            and fb_expect
            and len(power_ports) < fb_expect
        ):
            self.log_warning(
                "**Dry run:** only "
                f"**{len(power_ports)}** PowerPort row(s) exist until **Commit** saves renames/creates — "
                f"cabling preview may stop short of **{fb_expect}** (**FALLBACK_POWER_PORT_NAMES**)."
            )

        if not power_ports:
            if ports_planned and not commit:
                self.log_warning(
                    "**Dry run:** power ports would be created from templates on commit — "
                    "no cabling simulation until those ports exist in the database. "
                    "Re-run with **Commit** enabled (or run again after ports exist)."
                )
                summary_lines.append("- **Power ports:** none in DB yet (planned creations only in dry run).")
                summary_lines.append("- **Outcome:** no changes (dry run; ports not created)")
                summary_lines.append(f"- **Commit:** {commit}")
                text = "\n".join(summary_lines)
                self.log_info(text)
                return text
            raise AbortScript(
                "Device still has no PowerPorts after modeling attempts; check device type templates "
                "or **FALLBACK_POWER_PORT_NAMES** configuration."
            )

        # Refresh outlet pool after possible device-type outlet creation (rare for target devices)
        all_pdu_outlets_ordered = build_sorted_outlet_pool(pdus)
        total_pdu_outlets_ct = len(all_pdu_outlets_ordered)
        outlet_pool = [o for o in all_pdu_outlets_ordered if outlet_available(o)]
        free_outlets_initial = len(outlet_pool)

        # --- Connection pass ---
        cables_tagged = 0
        cables_saved = 0
        cables_labeled_from_pk = 0
        assignments: list[str] = []
        use_red_blue_parity = False

        if not commit:
            self.log_info(
                "**Dry run:** **Commit** is unchecked — no cables or tags will be written; "
                "messages below are **planned** changes only."
            )

        fully_connected_before = True
        for pp in power_ports:
            if not getattr(pp, "_occupied", False):
                fully_connected_before = False
                break
            peer = peer_power_outlet(pp)
            if not peer or peer.device_id not in pdu_ids or peer.device.rack_id != rack.pk:
                fully_connected_before = False
                break

        if (
            fully_connected_before
            and ports_saved == 0
            and outlets_saved == 0
            and ports_planned == 0
            and outlets_planned == 0
            and ports_renamed == 0
            and rename_planned == 0
        ):
            self.log_info(
                f"All power ports are cabled to in-rack PDU outlets; still checking **{TAG_SLUG}** on cables."
            )
            summary_lines.append(
                "- **Connections:** no new port-to-PDU links required (already connected in this rack)."
            )

        # Tag power cables first (existing or planned)
        for pp in power_ports:
            for cab in cables_touching_power_port(pp):
                added = apply_audit_tag_to_cable(
                    cab, tag, commit=commit, tag_slug=TAG_SLUG
                )
                if commit and added:
                    cables_tagged += 1

        red_pdu: Device | None = None
        blue_pdu: Device | None = None
        pa: PowerPort | None = None
        pb: PowerPort | None = None
        if PDU_RED_BLUE_PARITY_CABLING and len(power_ports) >= 2:
            red_pdu, blue_pdu, pdu_pick_note = pick_red_blue_pdu_devices(
                pdus,
                PDU_RED_NAME_SUBSTRING,
                PDU_BLUE_NAME_SUBSTRING,
                prefer_pdu_token=PDU_RED_BLUE_PREFER_PDU_TOKEN_IN_NAME,
                pdu_name_token=PDU_DEVICE_NAME_TOKEN,
            )
            if red_pdu and blue_pdu:
                pa, pb, parity_partition_warns = partition_psu_ab_ports(power_ports)
                warnings.extend(parity_partition_warns)
                if pa and pb:
                    use_red_blue_parity = True
                    self.log_info(
                        f"**RED/BLUE parity:** PSU A → **{red_pdu.name}**, PSU B → **{blue_pdu.name}** "
                        "(same outlet **name** on both PDUs)."
                    )
                    if pdu_pick_note:
                        self.log_info(
                            f"PDU selection: {pdu_pick_note} "
                            f"({PDU_DEVICE_NAME_TOKEN!r}+color preferred, then color substring only if needed)."
                        )
                else:
                    raise AbortScript(
                        "RED/BLUE parity requires **two** PowerPorts on this device "
                        "(use **PSU A** / **PSU B**, or exactly two ports — see script messages)."
                    )
            else:
                raise AbortScript(
                    "RED/BLUE parity cabling requires one PDU whose name contains "
                    f"{PDU_RED_NAME_SUBSTRING!r} and one containing {PDU_BLUE_NAME_SUBSTRING!r}. "
                    f"Found PDU devices: {', '.join(d.name or f'#{d.pk}' for d in pdus)}."
                )

        def finalize_power_cable(pp: PowerPort, outlet: PowerOutlet, cable: Cable) -> None:
            nonlocal cables_saved, cables_tagged, cables_labeled_from_pk
            port_title = fallback_rename_display.get(pp.name, pp.name)
            pdu_nm = outlet.device.name if getattr(outlet, "device", None) else ""
            plan_msg = (
                f"**Would connect** **{port_title}** on {device.name} → outlet **{outlet.name}** "
                f"on PDU {pdu_nm}."
                if not commit
                else (
                    f"Connecting **{port_title}** on {device.name} → outlet **{outlet.name}** "
                    f"on PDU {pdu_nm}."
                )
            )
            self.log_info(plan_msg)
            if commit:
                cable.save()
                cables_saved += 1
                if SET_CABLE_LABEL_FROM_PK:
                    if set_cable_label_from_pk(
                        cable,
                        overwrite_existing=CABLE_LABEL_OVERWRITE_EXISTING,
                    ):
                        cables_labeled_from_pk += 1
                        self.log_info(
                            f"Cable #{cable.pk}: set **Label** to `{cable.label}` (matches cable ID)."
                        )
                for cab in cables_touching_power_port(pp):
                    if apply_audit_tag_to_cable(cab, tag, commit=True, tag_slug=TAG_SLUG):
                        cables_tagged += 1
            assignments.append(
                f"{fallback_rename_display.get(pp.name, pp.name)} → outlet {outlet.name} ({pdu_nm})"
            )

        if use_red_blue_parity and red_pdu and blue_pdu and pa and pb:
            rm = pdu_outlets_by_name(red_pdu)
            bm = pdu_outlets_by_name(blue_pdu)
            for orphan in [p for p in power_ports if p.pk not in (pa.pk, pb.pk)]:
                warnings.append(
                    f"PowerPort **{orphan.name}** skipped — parity mode only cables **PSU A**/**PSU B** pair."
                )

            pa_occ = getattr(pa, "_occupied", False)
            pb_occ = getattr(pb, "_occupied", False)

            if pa_occ:
                pra = peer_power_outlet(pa)
                if pra and pra.device_id != red_pdu.pk:
                    warnings.append(
                        "Policy: **PSU A** must terminate on the **RED** PDU "
                        f"({PDU_RED_NAME_SUBSTRING}); current peer is on another device."
                    )
            if pb_occ:
                prb = peer_power_outlet(pb)
                if prb and prb.device_id != blue_pdu.pk:
                    warnings.append(
                        "Policy: **PSU B** must terminate on the **BLUE** PDU "
                        f"({PDU_BLUE_NAME_SUBSTRING}); current peer is on another device."
                    )

            if pa_occ and pb_occ:
                pra = peer_power_outlet(pa)
                prb = peer_power_outlet(pb)
                if pra and prb and pra.name != prb.name:
                    warnings.append(
                        f"RED/BLUE **parity** mismatch: PSU A outlet **{pra.name}** ≠ PSU B outlet **{prb.name}**."
                    )

            elif not pa_occ and not pb_occ:
                paired_ok = False
                for nm in sorted_common_outlet_names(red_pdu, blue_pdu):
                    ro = rm.get(nm)
                    bo = bm.get(nm)
                    if not ro or not bo or not outlet_available(ro) or not outlet_available(bo):
                        continue
                    ca = build_validated_power_cable(pa, ro)
                    cb = build_validated_power_cable(pb, bo)
                    if ca and cb:
                        finalize_power_cable(pa, ro, ca)
                        finalize_power_cable(pb, bo, cb)
                        paired_ok = True
                        break
                if not paired_ok:
                    warnings.append(
                        "No **free parity** pair: need the same outlet **name** available on both "
                        f"**{red_pdu.name}** and **{blue_pdu.name}** with valid PSU A/B cable validation."
                    )

            elif pa_occ and not pb_occ:
                pra = peer_power_outlet(pa)
                if pra and pra.device_id == red_pdu.pk:
                    nm = pra.name
                    bo = bm.get(nm)
                    if not bo:
                        warnings.append(
                            f"PSU B: no outlet **{nm}** on **{blue_pdu.name}** to match PSU A parity."
                        )
                    elif not outlet_available(bo):
                        warnings.append(
                            f"PSU B: outlet **{nm}** on BLUE is occupied; cannot match PSU A outlet **{nm}**."
                        )
                    else:
                        cb = build_validated_power_cable(pb, bo)
                        if cb:
                            finalize_power_cable(pb, bo, cb)
                        else:
                            warnings.append(
                                f"PSU B: NetBox rejected cable to BLUE outlet **{nm}** (types/feeds)."
                            )
                elif pra:
                    warnings.append("PSU A is not on the RED PDU — cannot complete parity for PSU B.")

            elif not pa_occ and pb_occ:
                prb = peer_power_outlet(pb)
                if prb and prb.device_id == blue_pdu.pk:
                    nm = prb.name
                    ro = rm.get(nm)
                    if not ro:
                        warnings.append(
                            f"PSU A: no outlet **{nm}** on **{red_pdu.name}** to match PSU B parity."
                        )
                    elif not outlet_available(ro):
                        warnings.append(
                            f"PSU A: outlet **{nm}** on RED is occupied; cannot match PSU B outlet **{nm}**."
                        )
                    else:
                        ca = build_validated_power_cable(pa, ro)
                        if ca:
                            finalize_power_cable(pa, ro, ca)
                        else:
                            warnings.append(
                                f"PSU A: NetBox rejected cable to RED outlet **{nm}** (types/feeds)."
                            )
                elif prb:
                    warnings.append("PSU B is not on the BLUE PDU — cannot complete parity for PSU A.")

        else:
            # Legacy: first-available outlet pool across PDUs (single PSU or parity disabled / one port only)
            for pp in power_ports:
                if getattr(pp, "_occupied", False):
                    peer = peer_power_outlet(pp)
                    if peer:
                        if peer.device_id not in pdu_ids:
                            warnings.append(
                                f"PowerPort **{pp.name}** connects to outlet on device pk={peer.device_id} "
                                "(not an identified in-rack PDU); left unchanged."
                            )
                        elif peer.device.rack_id != rack.pk:
                            warnings.append(
                                f"PowerPort **{pp.name}** connects to outlet not in this rack; left unchanged."
                            )
                    else:
                        warnings.append(
                            f"PowerPort **{pp.name}** is marked connected but peer is not a PowerOutlet; skipped."
                        )
                    continue

                found = pop_first_connectable_outlet(
                    pp,
                    outlet_pool,
                    use_cable_validation=PDU_MATCH_OUTLETS_USING_CABLE_VALIDATION,
                )
                if found is None:
                    if free_outlets_initial == 0:
                        warnings.append(
                            f"No **free** PDU outlet for **{pp.name}**: all **{total_pdu_outlets_ct}** modeled "
                            "PDU outlets appear in use (or marked occupied)."
                        )
                    elif len(outlet_pool) == 0:
                        warnings.append(
                            f"No free PDU outlet left for **{pp.name}** — another PowerPort in this run already "
                            "consumed the last available outlet(s)."
                        )
                    else:
                        warnings.append(
                            f"No valid cable for **{pp.name}**: **{len(outlet_pool)}** free outlet(s) remain but "
                            "NetBox rejected every port↔outlet pair (try fixing types/feeds in DCIM). "
                            "**PDU_MATCH_OUTLETS_USING_CABLE_VALIDATION** = False narrows outlets by identical type "
                            "strings before cable validation (often blocks valid pairs; not recommended)."
                        )
                    continue

                cable, outlet = found
                finalize_power_cable(pp, outlet, cable)

        if use_red_blue_parity:
            summary_lines.append(
                "- **Cabling policy:** PSU A → RED PDU, PSU B → BLUE PDU, **matching outlet name** on both."
            )

        # --- Summary ---
        if commit:
            summary_lines.append(
                f"- **Power ports:** found {len(power_ports)}, created {ports_saved}, renamed {ports_renamed} "
                f"(new ports use **{MAX_POWER_WATTS} W** max / **{ALLOCATED_POWER_WATTS} W** allocated)."
            )
            summary_lines.append(f"- **Power outlets created (from templates):** {outlets_saved}")
        else:
            summary_lines.append(
                f"- **Power ports:** found {len(power_ports)}; would create {ports_planned}, "
                f"would rename {rename_planned} legacy port(s) "
                f"(**{MAX_POWER_WATTS} W** max / **{ALLOCATED_POWER_WATTS} W** on new ports)."
            )
            summary_lines.append(f"- **Power outlets (templates):** would create {outlets_planned}")
        summary_lines.append(
            f"- **PDU ports assigned this run:** {len(assignments)} — "
            + ("; ".join(assignments) if assignments else "none")
        )
        if commit:
            summary_lines.append(
                f"- **Cables:** created {cables_saved}; audit tag `{TAG_SLUG}` applied to {cables_tagged} cable(s)."
            )
            if SET_CABLE_LABEL_FROM_PK and cables_saved:
                summary_lines.append(
                    f"- **Cable labels:** PK copied to **Label** on **{cables_labeled_from_pk}** / {cables_saved} "
                    f"new cable(s) (see **SET_CABLE_LABEL_FROM_PK**)."
                )
        else:
            dry_txt = (
                f"would create **{len(assignments)}** cable(s) and apply tag `{TAG_SLUG}` "
                "to each — enable **Commit** to save."
            )
            if SET_CABLE_LABEL_FROM_PK and assignments:
                dry_txt += (
                    " Would set **Label** on each new cable to its numeric cable ID after creation."
                )
            summary_lines.append(f"- **Dry run:** {dry_txt}")
        if warnings:
            summary_lines.append("- **Warnings:**")
            summary_lines.extend(f"  - {w}" for w in warnings)

        planned_any = (
            bool(assignments)
            or ports_planned
            or outlets_planned
            or rename_planned
        )
        db_any = bool(
            cables_saved
            or ports_saved
            or ports_renamed
            or outlets_saved
            or cables_tagged
        )

        if warnings:
            status = "partial success" if (planned_any or db_any) else "failure"
        elif not commit:
            status = "preview only (no database writes)" if planned_any else "no changes"
        elif db_any:
            status = "success"
        else:
            status = "no changes"

        summary_lines.insert(0, f"- **Outcome:** {status}")
        summary_lines.insert(1, f"- **Commit:** {commit}")

        self.log_info("\n".join(summary_lines))

        if status == "no changes":
            self.log_success("Finished — no database changes required.")
        elif status == "preview only (no database writes)":
            self.log_success(
                "**Dry run complete.** Enable **Commit** and re-run to write cables and tags to the database."
            )
        elif status == "success":
            self.log_success("Finished successfully.")
        elif status == "partial success":
            self.log_warning("Finished with warnings (see summary).")
        else:
            self.log_failure("Finished with errors/warnings (see summary).")

        return "\n".join(summary_lines)


# Optional: control script ordering if multiple classes per module.
script_order = (DeviceRackPowerModeling,)
