"""
NetBox custom script — Rack PDU Connectivity Audit

Read-only audit for a single rack: associated PDUs (unracked in site/location and/or PDUs in this rack),
modeled power traces from rack devices to PDU outlets, outlet utilization, HTML report plus CSV.
Writes under MEDIA_ROOT/script-reports/. Not a planning or load calculator.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from django.conf import settings
from django.db.models import Q
from django.utils.html import escape

from dcim.models import Device, PowerFeed, PowerOutlet, PowerPort, Rack
from extras.scripts import BooleanVar, ObjectVar, Script, StringVar

# -----------------------------------------------------------------------------
# Status labels (must match specification)
# -----------------------------------------------------------------------------

STATUS_CONNECTED = "CONNECTED"
STATUS_PARTIAL = "PARTIAL"
STATUS_NOT_CONNECTED = "NOT CONNECTED"
STATUS_NO_POWER_PORTS = "NO POWER PORTS"
STATUS_UNKNOWN = "UNKNOWN"

# CSV / logical connection_target_type values
TARGET_ASSOCIATED = "associated_rack_pdu"
TARGET_NON_ASSOCIATED = "non_associated_pdu"
TARGET_UNKNOWN = "unknown"
TARGET_NONE = "none"
# PDU device’s own intake traces to upstream PowerFeed (CSV / device audit)
TARGET_UPSTREAM_POWERFEED = "upstream_powerfeed"

# PDU summary card order when device names contain “red” / “blue” (grid reads left→right).
# True: red PDU card before blue. False: blue before red.
PDU_SUMMARY_RED_BEFORE_BLUE = True


def _resolve_rack(rack: Rack | int) -> Rack:
    """ObjectVar may pass a model instance (UI) or pk (API); normalize to Rack."""
    if isinstance(rack, Rack):
        return rack
    return Rack.objects.get(pk=rack)


def sanitize_filename_component(name: str, *, max_len: int = 120) -> str:
    """Filesystem-safe fragment from rack name (deterministic, conservative)."""
    if not name or not str(name).strip():
        return "rack"
    s = str(name).strip()
    s = re.sub(r"[^\w\-\.\s]", "_", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return (s[:max_len] if s else "rack")


def parse_power_role_tokens(power_role_names: str) -> list[str]:
    """Split comma-separated role tokens (compare case-insensitively to name and slug)."""
    if not power_role_names:
        return []
    return [t.strip().lower() for t in power_role_names.split(",") if t.strip()]


def is_power_role(device: Device, tokens: Iterable[str]) -> bool:
    if not tokens or not device.role_id:
        return False
    role = device.role
    name = (role.name or "").lower()
    slug = (role.slug or "").lower()
    return any(tok == name or tok == slug for tok in tokens)


def get_rack_devices(rack: Rack):
    """Devices installed in the rack (NetBox modeled placement)."""
    return (
        Device.objects.filter(rack=rack)
        .select_related("role", "device_type")
        .prefetch_related("powerports")
        .order_by("position", "name", "pk")
    )


def get_associated_pdus(rack: Rack, power_role_names: str) -> list[Device]:
    """
    PDUs associated with this rack's power environment (conservative default rules).

    Includes:

    A) **Unracked PDUs** (original rule): same site; if the rack has a location, same location;
       role matches configured power role tokens.

    B) **PDUs installed in this rack**: devices mounted on the selected rack whose role matches
       the power role tokens (typical vertical strip PDUs / red-blue pairs modeled in-rack).

    Environment-specific extensions (e.g. name-only association) belong in
    optional_associated_pdus_by_name_pattern() — keep them isolated.
    """
    tokens = parse_power_role_tokens(power_role_names)
    if not tokens:
        return []

    role_q = Q()
    for t in tokens:
        role_q |= Q(role__name__iexact=t) | Q(role__slug__iexact=t)

    unracked_site_role = Q(site_id=rack.site_id) & role_q & Q(rack__isnull=True)
    if rack.location_id:
        unracked_site_role &= Q(location_id=rack.location_id)

    in_this_rack = Q(site_id=rack.site_id) & role_q & Q(rack_id=rack.pk)

    qs = (
        Device.objects.filter(unracked_site_role | in_this_rack)
        .select_related("role", "device_type")
        .prefetch_related("poweroutlets", "powerports", "powerports__cable")
    )

    pdus = list(qs)

    # Optional future/env-specific additions (disabled by default).
    pdus.extend(
        optional_associated_pdus_by_name_pattern(rack, tokens, existing_ids={d.pk for d in pdus})
    )

    return pdus


def sort_associated_pdus_for_display(pdus: list[Device]) -> list[Device]:
    """
    Order PDU summary cards for left-to-right reading.

    Uses **PDU_SUMMARY_RED_BEFORE_BLUE** so red/blue strips can be ordered explicitly
    (plain name sort often puts “blue” before “red”). Other PDUs follow alphabetically.
    """
    red_rank, blue_rank = (0, 1) if PDU_SUMMARY_RED_BEFORE_BLUE else (1, 0)

    def key(d: Device) -> tuple:
        n = (d.name or "").lower()
        if "red" in n:
            return (red_rank, d.name or "", d.pk)
        if "blue" in n:
            return (blue_rank, d.name or "", d.pk)
        return (2, d.name or "", d.pk)

    return sorted(pdus, key=key)


def optional_associated_pdus_by_name_pattern(
    rack: Rack,
    tokens: list[str],
    *,
    existing_ids: set[int],
) -> list[Device]:
    """
    Optional hook for environment-specific PDU association (e.g. rack/PDU naming).

    **Default:** returns no extra devices. Copy this repo and implement your convention here
    if default site/location/role rules are insufficient — do not scatter one-off logic elsewhere.

    Parameters are intentionally minimal; query Device/Rack as needed inside your edit.
    """
    # `_tokens` / `_rack` kept for convenient breakpoints when customizing this deployment.
    _ = (rack, tokens)
    return []


def describe_pdu_power_in(pdu: Device) -> str:
    """
    Summarize modeled **power input** for this PDU: its PowerPort(s) and immediate cable peer.

    Read-only; uses the same link_peers semantics as the rest of this script.
    """
    ports = list(pdu.powerports.all())
    if not ports:
        return "No power ports modeled on this PDU."

    parts: list[str] = []
    for pp in ports:
        label = pp.name or "Power"
        peers = getattr(pp, "link_peers", []) or []
        if not peers:
            if getattr(pp, "mark_connected", False):
                parts.append(f"{label}: marked connected (no traceable cable)")
            else:
                parts.append(f"{label}: not connected")
            continue
        if len(peers) > 1:
            parts.append(f"{label}: multiple peers ({len(peers)})")
            continue
        peer = peers[0]
        if isinstance(peer, PowerFeed):
            panel = getattr(peer.power_panel, "name", "") or ""
            bits = [f"PowerFeed {peer.name}"]
            if panel:
                bits.append(f"panel {panel}")
            parts.append(f"{label} → " + ", ".join(bits))
        elif isinstance(peer, PowerOutlet):
            dev = getattr(peer.device, "name", "") or "?"
            parts.append(f"{label} → outlet on {dev}")
        else:
            parts.append(f"{label} → {peer.__class__.__name__}")
    return " · ".join(parts)


PHASE_THREE_PHASE = "three-phase"


def collect_upstream_power_feeds(pdu: Device) -> list[PowerFeed]:
    """Distinct PowerFeeds reached from this PDU device’s intake PowerPorts (cable link_peers)."""
    seen: dict[int, PowerFeed] = {}
    for pp in pdu.powerports.all():
        for peer in getattr(pp, "link_peers", []) or []:
            if isinstance(peer, PowerFeed):
                seen[peer.pk] = peer
    return list(seen.values())


def sum_allocated_draw_from_outlets(outlets: Iterable) -> int:
    """
    Sum NetBox **allocated_draw** (W) on downstream device PowerPorts cabled to these PDU outlets.

    Ports with null allocated_draw are skipped (conservative sum of modeled values only).
    """
    total = 0
    for outlet in outlets:
        for peer in getattr(outlet, "link_peers", []) or []:
            if isinstance(peer, PowerPort):
                ad = getattr(peer, "allocated_draw", None)
                if ad is not None:
                    total += int(ad)
    return total


def summarize_pdu_leg_power(pdu: Device) -> dict[str, Any]:
    """
    Per feed leg (A/B/C): outlet count, summed allocated draw from downstream gear, available capacity.

    **Available** uses ``PowerFeed.available_power`` when exactly one upstream feed exists:
    three-phase feeds split evenly across legs (``available_power // 3``). Single-phase feeds
    expose total capacity only (`upstream_feed_available_total_w`); per-leg available is not split.

    Outlets without **feed_leg** set are rolled into “Unassigned”.
    """
    feeds = collect_upstream_power_feeds(pdu)
    feed_total_avail: int | None = None
    feed_phase: str | None = None
    notes: list[str] = []

    if len(feeds) == 1:
        f0 = feeds[0]
        feed_total_avail = getattr(f0, "available_power", None)
        feed_phase = getattr(f0, "phase", None)
    elif len(feeds) > 1:
        notes.append("Multiple upstream PowerFeeds; per-leg available capacity not computed.")

    buckets: dict[str, list] = {"A": [], "B": [], "C": [], "_other": []}
    for o in pdu.poweroutlets.all():
        leg = getattr(o, "feed_leg", None)
        if leg in ("A", "B", "C"):
            buckets[leg].append(o)
        else:
            buckets["_other"].append(o)

    per_leg_avail: int | None = None
    if len(feeds) == 1 and feed_total_avail is not None and feed_phase == PHASE_THREE_PHASE:
        per_leg_avail = feed_total_avail // 3

    if len(feeds) == 1 and feed_phase and feed_phase != PHASE_THREE_PHASE and feed_total_avail is not None:
        notes.append(
            "Single-phase upstream: total feed capacity is modeled; per-leg available is not attributed."
        )

    row: dict[str, Any] = {
        "upstream_feed_count": len(feeds),
        "upstream_feed_available_total_w": feed_total_avail,
        "upstream_feed_phase": feed_phase or "",
        "power_availability_note": " ".join(notes).strip(),
    }

    total_alloc = 0
    for leg in ("A", "B", "C"):
        outs = buckets[leg]
        n_out = len(outs)
        alloc = sum_allocated_draw_from_outlets(outs)
        total_alloc += alloc
        row[f"outlets_leg_{leg.lower()}"] = n_out
        row[f"allocated_leg_{leg.lower()}_w"] = alloc
        row[f"available_leg_{leg.lower()}_w"] = per_leg_avail

    other = buckets["_other"]
    other_n = len(other)
    other_alloc = sum_allocated_draw_from_outlets(other)
    total_alloc += other_alloc
    row["outlets_unassigned_leg"] = other_n
    row["allocated_unassigned_w"] = other_alloc
    row["total_allocated_modeled_w"] = total_alloc

    rem = None
    if feed_total_avail is not None:
        rem = feed_total_avail - total_alloc
    row["remaining_vs_feed_total_w"] = rem

    return row


def format_watts_html(val: Any, *, empty: str = "—") -> str:
    """Format a watt value for HTML; ``None`` → em dash (allows negative remainder)."""
    if val is None:
        return empty
    try:
        v = int(val)
        if v < 0:
            return f"−{abs(v):,} W"
        return f"{v:,} W"
    except (TypeError, ValueError):
        return escape(str(val))


def summarize_pdu_outlets(pdu: Device) -> tuple[int, int, int, float]:
    """
    Outlet counts for one PDU device: total, used, free, utilization percent.

    Used = modeled occupation (cable attached or mark_connected), per NetBox DCIM semantics.
    """
    outlets = list(pdu.poweroutlets.all())
    total = len(outlets)
    if total == 0:
        return 0, 0, 0, 0.0
    used = sum(1 for o in outlets if getattr(o, "_occupied", False))
    free = total - used
    pct = (used / total) * 100.0
    return total, used, free, pct


def pdu_upstream_capacity_ok(device: Device) -> tuple[bool, str]:
    """
    True when intake PowerPort(s) on this device trace to at least one PowerFeed that has
    modeled ``available_power`` (NetBox-computed VA/W per feed).

    Used to flag rack PDUs that look cabled but have no usable upstream capacity in the model.
    """
    feeds = collect_upstream_power_feeds(device)
    if not feeds:
        return (
            False,
            "No PowerFeed traced from PDU intake PowerPort(s); cannot confirm upstream capacity.",
        )
    problems: list[str] = []
    for pf in feeds:
        name = getattr(pf, "name", "") or str(pf.pk)
        ap = getattr(pf, "available_power", None)
        if ap is None:
            problems.append(f"{name}: PowerFeed.available_power not populated")
        elif ap == 0:
            problems.append(f"{name}: PowerFeed.available_power is zero")
    if problems:
        return False, "Upstream capacity not modeled on feed(s): " + "; ".join(problems)
    return True, ""


def trace_pdu_intake_power_port(power_port: PowerPort) -> tuple[str, str, str]:
    """
    Trace a **rack PDU device’s intake** PowerPort.

    Expected topology: intake ``PowerPort`` → cable → **PowerFeed** (not another device’s outlet).
    Returns trace codes ``upstream_feed``, ``upstream_via_outlet``, ``none``, ``unknown``.
    """
    peers = getattr(power_port, "link_peers", []) or []

    if not peers:
        if getattr(power_port, "mark_connected", False):
            return (
                "unknown",
                "",
                "PDU intake marked connected without a cable trace to PowerFeed.",
            )
        return ("none", "", "PDU intake PowerPort has no cable.")

    if len(peers) > 1:
        return (
            "unknown",
            "",
            f"PDU intake port has multiple cable peers ({len(peers)}); cannot trace uniquely.",
        )

    peer = peers[0]

    if isinstance(peer, PowerFeed):
        pname = getattr(peer, "name", "") or ""
        return ("upstream_feed", pname, f"PDU intake traces to PowerFeed “{pname}”.")

    if isinstance(peer, PowerOutlet):
        on = getattr(peer.device, "name", "") if getattr(peer, "device", None) else ""
        return (
            "upstream_via_outlet",
            on,
            f"PDU intake connects to PowerOutlet on “{on}” (unexpected vs upstream PowerFeed).",
        )

    peer_label = getattr(peer, "name", None) or peer.__class__.__name__
    return (
        "unknown",
        str(peer_label),
        f"PDU intake peer is {peer.__class__.__name__}; expected PowerFeed for rack PDU intake.",
    )


def trace_power_port_for_audit(
    power_port: PowerPort,
    device: Device,
    associated_pdu_ids: set[int],
) -> tuple[str, str, str]:
    """Dispatch: associated PDU **devices** use intake feed tracing; everything else uses outlet tracing."""
    if device.pk in associated_pdu_ids:
        return trace_pdu_intake_power_port(power_port)
    return trace_power_port_to_pdu(power_port, associated_pdu_ids)


def trace_power_port_to_pdu(
    power_port: PowerPort,
    associated_pdu_ids: set[int],
) -> tuple[str, str, str]:
    """
    Trace one power port along modeled cable peers only.

    Returns:
        (trace_code, pdu_display_name_or_empty, reason_fragment)

    trace_code:
        associated | non_associated_pdu | none | unknown
    """
    peers = getattr(power_port, "link_peers", []) or []

    if not peers:
        if getattr(power_port, "mark_connected", False):
            return (
                "unknown",
                "",
                "Power port marked connected without a cable; cannot trace to a PDU outlet.",
            )
        return ("none", "", "No cable on power port.")

    if len(peers) > 1:
        return (
            "unknown",
            "",
            f"Multiple immediate cable peers ({len(peers)}); path not uniquely traceable.",
        )

    peer = peers[0]

    if isinstance(peer, PowerOutlet):
        pdu = peer.device
        pname = pdu.name if pdu else ""
        pid = pdu.pk if pdu else None
        if pid is None:
            return ("unknown", "", "Power outlet has no parent device in the model.")
        if pid in associated_pdu_ids:
            return ("associated", pname, "")
        return ("non_associated_pdu", pname, f"Outlet on PDU “{pname}” (not in associated PDU set).")

    # PowerFeed or any other termination type — not a PDU outlet hop for this audit
    peer_label = getattr(peer, "name", None) or peer.__class__.__name__
    return (
        "unknown",
        str(peer_label),
        f"Immediate peer is {peer.__class__.__name__}, not a PowerOutlet; cannot validate PDU path.",
    )


@dataclass
class DeviceAuditRow:
    rack_name: str
    site_name: str
    device_name: str
    device_role: str
    device_type: str
    rack_position: str
    face: str
    power_port_count: int
    connected_power_port_count: int
    connected_pdu_names: str
    connection_target_type: str
    status: str
    reason: str


def _fmt_position(device: Device) -> str:
    pos = getattr(device, "position", None)
    if pos is None:
        return ""
    return str(pos)


def _fmt_face(device: Device) -> str:
    face = getattr(device, "face", "") or ""
    try:
        return device.get_face_display()
    except Exception:
        return str(face)


def evaluate_associated_pdu_as_rack_device(
    device: Device,
    *,
    strict_mode: bool,
) -> DeviceAuditRow:
    """
    Classify a device that **is** one of this rack’s associated PDUs (red/blue strips, etc.).

    Intake ``PowerPort`` → **PowerFeed** is the normal modeled path (not PowerOutlet).
    ``CONNECTED`` requires every intake port to trace to a PowerFeed **and** usable
    ``PowerFeed.available_power`` on those feeds.
    """
    rack = device.rack
    rack_name = rack.name if rack else ""
    site_name = rack.site.name if rack and rack.site_id else ""

    ports = list(device.powerports.all())
    power_port_count = len(ports)
    connected_power_port_count = sum(
        1 for p in ports if getattr(p, "_occupied", False)
    )

    if power_port_count == 0:
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=0,
            connected_power_port_count=0,
            connected_pdu_names="",
            connection_target_type=TARGET_NONE,
            status=STATUS_NO_POWER_PORTS,
            reason="PDU device has no intake PowerPort modeled.",
        )

    traces = [trace_pdu_intake_power_port(p) for p in ports]

    if strict_mode and any(t[0] == "unknown" for t in traces):
        detail = "; ".join(t[2] for t in traces if t[0] == "unknown")
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names="",
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_UNKNOWN,
            reason=detail or "Strict mode: ambiguous PDU intake trace.",
        )

    kinds = [t[0] for t in traces]
    feed_names = sorted({t[1] for t in traces if t[0] == "upstream_feed" and t[1]})
    has_feed = any(k == "upstream_feed" for k in kinds)
    has_via_outlet = any(k == "upstream_via_outlet" for k in kinds)
    has_none = any(k == "none" for k in kinds)
    has_unk = any(k == "unknown" for k in kinds)

    reasons = []
    if has_unk:
        reasons.extend(t[2] for t in traces if t[0] == "unknown")
    if has_via_outlet:
        reasons.extend(t[2] for t in traces if t[0] == "upstream_via_outlet")

    joined = "; ".join(reasons) if reasons else ""
    capacity_ok, cap_msg = pdu_upstream_capacity_ok(device)
    feeds_named = ", ".join(feed_names)

    # Intake ports only to PowerFeed — confirm upstream capacity in NetBox.
    if kinds and all(k == "upstream_feed" for k in kinds):
        if capacity_ok:
            return DeviceAuditRow(
                rack_name=rack_name,
                site_name=site_name,
                device_name=device.name,
                device_role=device.role.name if device.role_id else "",
                device_type=device.device_type.model if device.device_type_id else "",
                rack_position=_fmt_position(device),
                face=_fmt_face(device),
                power_port_count=power_port_count,
                connected_power_port_count=connected_power_port_count,
                connected_pdu_names=feeds_named,
                connection_target_type=TARGET_UPSTREAM_POWERFEED,
                status=STATUS_CONNECTED,
                reason=(
                    "Rack PDU: intake PowerPort(s) trace to modeled PowerFeed(s); "
                    "upstream capacity present on feed(s)."
                ),
            )
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names=feeds_named,
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_NOT_CONNECTED,
            reason=(
                "PDU intake traces to PowerFeed but upstream capacity check failed. "
                + cap_msg
            ),
        )

    if has_feed and (has_none or has_via_outlet or has_unk):
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names=feeds_named,
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_PARTIAL,
            reason=(
                "PDU intake partly traces to PowerFeed; other intake path(s) are open, outlet-fed, "
                "or unresolved. "
                + (joined + " " if joined else "")
                + (cap_msg if not capacity_ok else "")
            ).strip(),
        )

    if not has_feed and has_unk and not has_none:
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names="",
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_UNKNOWN,
            reason=joined or "Could not classify PDU intake paths.",
        )

    if has_via_outlet and not has_feed:
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names="",
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_PARTIAL,
            reason=joined or "PDU intake uses outlet path, not PowerFeed.",
        )

    if has_none and not has_feed:
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names="",
            connection_target_type=TARGET_NONE,
            status=STATUS_NOT_CONNECTED,
            reason="PDU intake PowerPort(s) have no cable to a PowerFeed.",
        )

    return DeviceAuditRow(
        rack_name=rack_name,
        site_name=site_name,
        device_name=device.name,
        device_role=device.role.name if device.role_id else "",
        device_type=device.device_type.model if device.device_type_id else "",
        rack_position=_fmt_position(device),
        face=_fmt_face(device),
        power_port_count=power_port_count,
        connected_power_port_count=connected_power_port_count,
        connected_pdu_names="",
        connection_target_type=TARGET_UNKNOWN,
        status=STATUS_UNKNOWN,
        reason=joined or "Unexpected PDU intake classification.",
    )


def evaluate_device_connectivity(
    device: Device,
    associated_pdu_ids: set[int],
    *,
    strict_mode: bool,
) -> DeviceAuditRow:
    """Classify one rack device; conservative — only CONNECTED with explicit outlet path to associated PDU."""
    rack = device.rack
    rack_name = rack.name if rack else ""
    site_name = rack.site.name if rack and rack.site_id else ""

    ports = list(device.powerports.all())
    power_port_count = len(ports)

    connected_power_port_count = sum(
        1 for p in ports if getattr(p, "_occupied", False)
    )

    if device.pk in associated_pdu_ids:
        return evaluate_associated_pdu_as_rack_device(device, strict_mode=strict_mode)

    if power_port_count == 0:
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=0,
            connected_power_port_count=0,
            connected_pdu_names="",
            connection_target_type=TARGET_NONE,
            status=STATUS_NO_POWER_PORTS,
            reason="No power ports modeled on this device.",
        )

    traces: list[tuple[str, str, str]] = [
        trace_power_port_for_audit(p, device, associated_pdu_ids) for p in ports
    ]

    if strict_mode and any(t[0] == "unknown" for t in traces):
        detail = "; ".join(t[2] for t in traces if t[0] == "unknown")
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names="",
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_UNKNOWN,
            reason=detail or "Strict mode: ambiguous or incomplete trace on at least one power port.",
        )

    kinds = [t[0] for t in traces]
    assoc_names = sorted({t[1] for t in traces if t[0] == "associated" and t[1]})
    has_assoc = any(k == "associated" for k in kinds)
    has_non = any(k == "non_associated_pdu" for k in kinds)
    has_none = any(k == "none" for k in kinds)
    has_unk = any(k == "unknown" for k in kinds)

    reasons: list[str] = []
    if has_unk:
        reasons.extend(t[2] for t in traces if t[0] == "unknown")
    if has_non:
        reasons.extend(t[2] for t in traces if t[0] == "non_associated_pdu")
    if has_none:
        reasons.append("One or more power ports have no cable.")

    connected_pdu_names = ", ".join(assoc_names)
    joined_reasons = "; ".join(reasons) if reasons else ""

    # CONNECTED: every port resolves exclusively to an associated PDU outlet (clear path, no gaps).
    if has_assoc and not has_non and not has_none and not has_unk:
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names=connected_pdu_names,
            connection_target_type=TARGET_ASSOCIATED,
            status=STATUS_CONNECTED,
            reason="All modeled power ports trace to outlets on associated PDUs.",
        )

    # PARTIAL: at least one good association, but another port is open, elsewhere, or unresolved.
    if has_assoc and (has_non or has_none or has_unk):
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names=connected_pdu_names,
            connection_target_type=TARGET_UNKNOWN,
            status=STATUS_PARTIAL,
            reason=(
                "At least one power port reaches an associated PDU outlet, but another port is "
                "unconnected, unresolved, or connected to a non-associated PDU."
                + (" " + joined_reasons if joined_reasons else "")
            ),
        )

    # No associated PDU path on any port
    if not has_assoc:
        if has_unk and not has_non and not has_none:
            return DeviceAuditRow(
                rack_name=rack_name,
                site_name=site_name,
                device_name=device.name,
                device_role=device.role.name if device.role_id else "",
                device_type=device.device_type.model if device.device_type_id else "",
                rack_position=_fmt_position(device),
                face=_fmt_face(device),
                power_port_count=power_port_count,
                connected_power_port_count=connected_power_port_count,
                connected_pdu_names="",
                connection_target_type=TARGET_UNKNOWN,
                status=STATUS_UNKNOWN,
                reason=joined_reasons or "Could not resolve modeled cable traces for any power port.",
            )
        if has_unk:
            return DeviceAuditRow(
                rack_name=rack_name,
                site_name=site_name,
                device_name=device.name,
                device_role=device.role.name if device.role_id else "",
                device_type=device.device_type.model if device.device_type_id else "",
                rack_position=_fmt_position(device),
                face=_fmt_face(device),
                power_port_count=power_port_count,
                connected_power_port_count=connected_power_port_count,
                connected_pdu_names="",
                connection_target_type=TARGET_UNKNOWN,
                status=STATUS_PARTIAL,
                reason=(
                    "No trace to an associated PDU outlet; mixed unresolved or non-associated paths."
                    + (" " + joined_reasons if joined_reasons else "")
                ),
            )
        if has_non and has_none:
            return DeviceAuditRow(
                rack_name=rack_name,
                site_name=site_name,
                device_name=device.name,
                device_role=device.role.name if device.role_id else "",
                device_type=device.device_type.model if device.device_type_id else "",
                rack_position=_fmt_position(device),
                face=_fmt_face(device),
                power_port_count=power_port_count,
                connected_power_port_count=connected_power_port_count,
                connected_pdu_names="",
                connection_target_type=TARGET_UNKNOWN,
                status=STATUS_PARTIAL,
                reason="Mix of open power ports and outlets on non-associated PDUs.",
            )
        if has_non:
            non_reasons = "; ".join(t[2] for t in traces if t[0] == "non_associated_pdu")
            return DeviceAuditRow(
                rack_name=rack_name,
                site_name=site_name,
                device_name=device.name,
                device_role=device.role.name if device.role_id else "",
                device_type=device.device_type.model if device.device_type_id else "",
                rack_position=_fmt_position(device),
                face=_fmt_face(device),
                power_port_count=power_port_count,
                connected_power_port_count=connected_power_port_count,
                connected_pdu_names="",
                connection_target_type=TARGET_NON_ASSOCIATED,
                status=STATUS_NOT_CONNECTED,
                reason=(
                    "Power ports reach PDU outlets, but none belong to the associated PDU set for this rack."
                    + (" " + non_reasons if non_reasons else "")
                ),
            )
        return DeviceAuditRow(
            rack_name=rack_name,
            site_name=site_name,
            device_name=device.name,
            device_role=device.role.name if device.role_id else "",
            device_type=device.device_type.model if device.device_type_id else "",
            rack_position=_fmt_position(device),
            face=_fmt_face(device),
            power_port_count=power_port_count,
            connected_power_port_count=connected_power_port_count,
            connected_pdu_names="",
            connection_target_type=TARGET_NONE,
            status=STATUS_NOT_CONNECTED,
            reason="No power port traces to an associated PDU outlet (unconnected ports only).",
        )

    raise RuntimeError("PDU connectivity classification reached an unexpected branch; audit the script logic.")


def build_output_basename(rack_name: str, report_key: str, timestamp: str) -> str:
    """
    Deterministic filename stem (no directory, includes extension-style suffix via caller).

    Example: ``build_output_basename("Rack A", "pdu_connectivity_audit", ts)`` →
    ``Rack_A_pdu_connectivity_audit_<ts>`` then add ``.html`` / ``.csv``.
    """
    stem = sanitize_filename_component(rack_name)
    return f"{stem}_{report_key}_{timestamp}"


def _media_paths(rel_under_media: str) -> tuple[str, Path]:
    """Return (web path beginning with MEDIA_URL, absolute Path under MEDIA_ROOT)."""
    dest = Path(settings.MEDIA_ROOT) / rel_under_media
    mu = settings.MEDIA_URL
    if not mu.endswith("/"):
        mu = f"{mu}/"
    web = f"{mu}{rel_under_media}"
    if not web.startswith("/"):
        web = f"/{web}"
    return web, dest


def _absolute_uri_best_effort(script_request, path: str) -> str:
    """Prefer absolute https URL for job log links (same approach as other scripts in this repo)."""
    if not path.startswith("/"):
        path = "/" + path
    if script_request is not None:
        if hasattr(script_request, "build_absolute_uri"):
            try:
                return script_request.build_absolute_uri(path)
            except Exception:
                pass
        meta = getattr(script_request, "META", None) or {}
        host = meta.get("HTTP_X_FORWARDED_HOST") or meta.get("HTTP_HOST")
        if host:
            host = host.split(",")[0].strip()
            proto = (
                meta.get("HTTP_X_FORWARDED_PROTO")
                or meta.get("wsgi.url_scheme")
                or "https"
            )
            proto = proto.split(",")[0].strip()
            return f"{proto}://{host}{path}"
    try:
        from django.contrib.sites.models import Site

        domain = Site.objects.get_current().domain
        if domain:
            return f"https://{domain.rstrip('/')}{path}"
    except Exception:
        pass
    return path


def write_device_csv(rows: list[DeviceAuditRow], dest: Path) -> None:
    fieldnames = [
        "rack_name",
        "site_name",
        "device_name",
        "device_role",
        "device_type",
        "rack_position",
        "face",
        "power_port_count",
        "connected_power_port_count",
        "connected_pdu_names",
        "connection_target_type",
        "status",
        "reason",
    ]
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "rack_name": r.rack_name,
                    "site_name": r.site_name,
                    "device_name": r.device_name,
                    "device_role": r.device_role,
                    "device_type": r.device_type,
                    "rack_position": r.rack_position,
                    "face": r.face,
                    "power_port_count": r.power_port_count,
                    "connected_power_port_count": r.connected_power_port_count,
                    "connected_pdu_names": r.connected_pdu_names,
                    "connection_target_type": r.connection_target_type,
                    "status": r.status,
                    "reason": r.reason,
                }
            )


def write_pdu_csv(
    rack: Rack,
    pdu_rows: list[dict[str, Any]],
    dest: Path,
) -> None:
    fieldnames = [
        "rack_name",
        "site_name",
        "pdu_name",
        "pdu_role",
        "power_in",
        "upstream_feed_count",
        "upstream_feed_available_total_w",
        "upstream_feed_phase",
        "power_availability_note",
        "outlets_leg_a",
        "allocated_leg_a_w",
        "available_leg_a_w",
        "outlets_leg_b",
        "allocated_leg_b_w",
        "available_leg_b_w",
        "outlets_leg_c",
        "allocated_leg_c_w",
        "available_leg_c_w",
        "outlets_unassigned_leg",
        "allocated_unassigned_w",
        "total_allocated_modeled_w",
        "remaining_vs_feed_total_w",
        "total_outlets",
        "used_outlets",
        "free_outlets",
        "outlet_utilization_percent",
    ]
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in pdu_rows:
            w.writerow(row)


def _util_bar_style(pct: float) -> tuple[str, str]:
    if pct >= 90:
        return ("#c0392b", "high")
    if pct >= 75:
        return ("#d68910", "elevated")
    if pct >= 50:
        return ("#2980b9", "moderate")
    return ("#1e8449", "ok")


def build_html_report(
    rack: Rack,
    ts_display: str,
    pdu_summary_rows: list[dict[str, Any]],
    device_rows: list[DeviceAuditRow],
    *,
    strict_mode: bool,
    power_role_names: str,
) -> str:
    """Single-page dark-theme HTML (inline CSS, no JavaScript)."""
    rack_name = escape(rack.name or "—")
    site = escape(rack.site.name) if rack.site_id else "—"
    location = escape(rack.location.name) if rack.location_id else "—"
    roles_esc = escape(power_role_names)
    total_devices = len(device_rows)
    total_pdus = len(pdu_summary_rows)
    flagged = [r for r in device_rows if r.status != STATUS_CONNECTED]
    n_flagged = len(flagged)

    # PDU cards HTML
    pdu_blocks: list[str] = []
    if not pdu_summary_rows:
        pdu_blocks.append(
            '<p class="empty">No associated PDUs matched the configured rules '
            "(PDUs <strong>in this rack</strong>, or unracked with same site"
            + (", same location when unracked" if rack.location_id else "")
            + f', roles matching <strong>{roles_esc}</strong>). '
            "Ensure PDU devices use a DeviceRole listed here (name/slug); adjust "
            "<strong>Power role names</strong> or use the optional name hook if needed.</p>"
        )
    else:
        for pr in pdu_summary_rows:
            pct = float(pr["outlet_utilization_percent"])
            accent, band = _util_bar_style(pct)
            pct_s = f"{pct:.1f}"
            pin = escape(str(pr.get("power_in", "—")))
            fed_tot = pr.get("upstream_feed_available_total_w")
            fed_tot_s = format_watts_html(fed_tot)
            tot_alloc = int(pr.get("total_allocated_modeled_w") or 0)
            rem_feed = pr.get("remaining_vs_feed_total_w")
            panote = escape(str(pr.get("power_availability_note", "") or "").strip())

            leg_rows_html = []
            for leg_lab, leg_key in (("A", "a"), ("B", "b"), ("C", "c")):
                ocnt = int(pr.get(f"outlets_leg_{leg_key}", 0) or 0)
                aw = int(pr.get(f"allocated_leg_{leg_key}_w") or 0)
                vw = pr.get(f"available_leg_{leg_key}_w")
                alloc_s = "—" if ocnt == 0 else format_watts_html(aw)
                avail_s = format_watts_html(vw)
                rem_s = "—"
                if ocnt > 0 and vw is not None:
                    rem_s = format_watts_html(int(vw) - aw)
                leg_rows_html.append(
                    "<tr>"
                    f"<td><strong>Leg {leg_lab}</strong></td>"
                    f"<td>{ocnt}</td>"
                    f"<td>{alloc_s}</td>"
                    f"<td>{avail_s}</td>"
                    f"<td>{rem_s}</td>"
                    "</tr>"
                )

            ou_other = int(pr.get("outlets_unassigned_leg", 0) or 0)
            oa_other = pr.get("allocated_unassigned_w")
            if ou_other > 0 or (oa_other is not None and oa_other > 0):
                leg_rows_html.append(
                    "<tr>"
                    '<td><strong>Unassigned leg</strong></td>'
                    f"<td>{ou_other}</td>"
                    f"<td>{format_watts_html(oa_other)}</td>"
                    "<td>—</td>"
                    "<td>—</td>"
                    "</tr>"
                )

            feed_line = ""
            if fed_tot is not None:
                feed_line = (
                    f'<div class="pdu-feed-total"><label>Upstream feed capacity (PowerFeed)</label>'
                    f"<span>{fed_tot_s}</span></div>"
                )

            summary_line = (
                f'<div class="pdu-power-sum">'
                f"<span><label>Total allocated (modeled)</label>{format_watts_html(tot_alloc)}</span>"
                f"<span><label>Remaining vs feed total</label>{format_watts_html(rem_feed)}</span>"
                f"</div>"
            )

            pdu_card = (
                f'<div class="pdu-card">'
                f'<div class="pdu-title">{escape(pr["pdu_name"])}</div>'
                f'<div class="pdu-meta"><span>{escape(pr["pdu_role"])}</span></div>'
                f'<div class="pdu-power-in"><label>Power in</label>'
                f'<span class="pdu-power-in-text">{pin}</span></div>'
                f"{feed_line}"
                '<div class="pdu-leg-table-wrap">'
                '<table class="pdu-leg-table" aria-label="Per-leg outlets and power">'
                "<colgroup>"
                '<col class="pdu-col-leg" />'
                '<col class="pdu-col-outlets" />'
                '<col class="pdu-col-power" />'
                '<col class="pdu-col-power" />'
                '<col class="pdu-col-power" />'
                "</colgroup>"
                '<thead><tr>'
                '<th scope="col" title="Feed leg">Leg</th>'
                '<th scope="col" title="Outlet count">#</th>'
                '<th scope="col" title="Allocated power (modeled, W)">Alloc</th>'
                '<th scope="col" title="Available power (W)">Avail</th>'
                '<th scope="col" title="Remaining (W)">Rem</th>'
                "</tr></thead><tbody>"
                + "".join(leg_rows_html)
                + "</tbody></table></div>"
                + (f'<p class="pdu-mini-note">{panote}</p>' if panote else "")
                + summary_line
                + (
                    f'<div class="stats-mini stats-mini--compact">'
                    f'<span><label>Total outlets</label><b>{pr["total_outlets"]}</b></span>'
                    f'<span><label>Used outlets</label><b>{pr["used_outlets"]}</b></span>'
                    f'<span><label>Free outlets</label><b>{pr["free_outlets"]}</b></span>'
                    f'<span><label>Outlet utilization</label><b>{pct_s}%</b></span>'
                    f"</div>"
                    f'<div class="meter-label"><span>Outlet usage</span><span class="band {escape(band)}">{escape(band)}</span></div>'
                    f'<div class="meter" role="progressbar"><div class="meter-fill" style="width:min(100%,{pct_s}%);background:{accent}"></div></div>'
                    f"</div>"
                )
            )
            pdu_blocks.append(pdu_card)

    def device_table(rows: list[DeviceAuditRow], table_id: str) -> str:
        out = [
            f'<table id="{escape(table_id)}">',
            "<thead><tr>"
            "<th>Device</th><th>Role</th><th>Type</th><th>RU</th><th>Face</th>"
            "<th>Power ports</th><th>Connected ports</th><th>Associated PDU names</th>"
            "<th>Target type</th><th>Status</th><th>Reason</th>"
            "</tr></thead><tbody>",
        ]
        for r in rows:
            st_class = "st-ok" if r.status == STATUS_CONNECTED else "st-warn"
            out.append(
                "<tr>"
                f"<td>{escape(r.device_name)}</td>"
                f"<td>{escape(r.device_role)}</td>"
                f"<td>{escape(r.device_type)}</td>"
                f"<td>{escape(r.rack_position)}</td>"
                f"<td>{escape(r.face)}</td>"
                f"<td>{r.power_port_count}</td>"
                f"<td>{r.connected_power_port_count}</td>"
                f"<td>{escape(r.connected_pdu_names)}</td>"
                f"<td>{escape(r.connection_target_type)}</td>"
                f'<td class="{st_class}">{escape(r.status)}</td>'
                f"<td>{escape(r.reason)}</td>"
                "</tr>"
            )
        if not rows:
            out.append('<tr><td colspan="11" class="empty">No rows.</td></tr>')
        out.append("</tbody></table>")
        return "\n".join(out)

    pdu_section = '\n<div class="pdu-grid">\n' + "\n".join(pdu_blocks) + "\n</div>\n"

    strict_esc = "on" if strict_mode else "off"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark">
  <title>Rack PDU Connectivity Audit — {rack_name}</title>
  <style>
    :root {{
      --bg: #0f1419;
      --card: #1a2332;
      --text: #e8eef5;
      --muted: #8b9cb3;
      --radius: 12px;
      --font: "Segoe UI", system-ui, -apple-system, sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    html {{ background: #0f1419; color-scheme: dark; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: var(--font);
      background: linear-gradient(160deg, #0a0e14 0%, #151d2a 50%, #0f1419 100%);
      color: var(--text);
      padding: 2rem 1rem 3rem;
      line-height: 1.45;
      font-size: 0.92rem;
    }}
    .wrap {{ max-width: 72rem; margin: 0 auto; }}
    header {{ margin-bottom: 1.5rem; }}
    .eyebrow {{
      font-size: 0.72rem;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 0.35rem;
    }}
    h1 {{ font-size: 1.55rem; font-weight: 600; margin: 0 0 0.35rem; }}
    .meta {{ font-size: 0.84rem; color: var(--muted); }}
    .meta span {{ margin-right: 1rem; }}
    .banner-stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 0.75rem;
      margin-top: 1rem;
    }}
    .banner-stats div {{
      background: rgba(0,0,0,0.22);
      border-radius: 10px;
      padding: 0.65rem 0.85rem;
    }}
    .banner-stats label {{
      display: block;
      font-size: 0.65rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
    }}
    .banner-stats b {{ font-size: 1.2rem; }}
    section {{
      margin-top: 1.75rem;
      background: var(--card);
      border-radius: var(--radius);
      padding: 1.35rem 1.25rem;
      box-shadow: 0 8px 32px rgba(0,0,0,0.35), 0 0 0 1px rgba(255,255,255,0.04);
    }}
    section h2 {{
      margin: 0 0 1rem;
      font-size: 1.05rem;
      font-weight: 600;
      letter-spacing: -0.02em;
    }}
    .hint {{
      font-size: 0.78rem;
      color: var(--muted);
      margin: 0 0 1rem;
    }}
    .pdu-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 1rem;
      align-items: start;
    }}
    .pdu-card {{
      background: rgba(0,0,0,0.2);
      border-radius: 10px;
      padding: 1rem;
      min-width: 0;
    }}
    .pdu-title {{ font-weight: 600; font-size: 0.98rem; }}
    .pdu-meta {{ font-size: 0.75rem; color: var(--muted); margin: 0.2rem 0 0.5rem; }}
    .pdu-power-in {{
      font-size: 0.78rem;
      line-height: 1.4;
      margin: 0 0 0.6rem;
      padding: 0.5rem 0.55rem;
      border-radius: 8px;
      background: rgba(0,0,0,0.25);
    }}
    .pdu-power-in label {{
      display: block;
      font-size: 0.62rem;
      text-transform: uppercase;
      letter-spacing: 0.07em;
      color: var(--muted);
      margin-bottom: 0.25rem;
    }}
    .pdu-power-in-text {{ color: var(--text); word-break: break-word; }}
    .pdu-feed-total {{
      font-size: 0.76rem;
      margin: 0 0 0.55rem;
      padding: 0.45rem 0.55rem;
      border-radius: 8px;
      background: rgba(30,132,73,0.12);
      border: 1px solid rgba(30,132,73,0.25);
    }}
    .pdu-feed-total label {{
      display: block;
      font-size: 0.62rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
      margin-bottom: 0.2rem;
    }}
    .pdu-leg-table-wrap {{
      margin: 0 0 0.5rem;
      max-width: 100%;
    }}
    .pdu-leg-table {{
      margin: 0;
      font-size: clamp(0.62rem, 0.85vw + 0.55rem, 0.74rem);
      width: 100%;
      max-width: 100%;
      table-layout: fixed;
      border-collapse: collapse;
    }}
    .pdu-leg-table col.pdu-col-leg {{ width: 16%; }}
    .pdu-leg-table col.pdu-col-outlets {{ width: 9%; }}
    .pdu-leg-table col.pdu-col-power {{ width: 25%; }}
    .pdu-leg-table th,
    .pdu-leg-table td {{
      padding: 0.35rem 0.28rem;
      vertical-align: top;
      overflow-wrap: anywhere;
      hyphens: none;
    }}
    .pdu-leg-table td:first-child,
    .pdu-leg-table th:first-child {{
      text-align: left;
    }}
    .pdu-leg-table td:nth-child(2),
    .pdu-leg-table th:nth-child(2) {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .pdu-leg-table td:nth-child(n+3),
    .pdu-leg-table th:nth-child(n+3) {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .pdu-mini-note {{
      font-size: 0.72rem;
      color: var(--muted);
      margin: 0 0 0.55rem;
      line-height: 1.4;
    }}
    .pdu-power-sum {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem 1rem;
      font-size: 0.78rem;
      margin-bottom: 0.65rem;
      padding: 0.45rem 0.5rem;
      border-radius: 8px;
      background: rgba(0,0,0,0.2);
    }}
    .pdu-power-sum label {{
      display: block;
      font-size: 0.62rem;
      text-transform: uppercase;
      color: var(--muted);
      letter-spacing: 0.05em;
    }}
    .stats-mini {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.35rem 0.75rem;
      font-size: 0.78rem;
      margin-bottom: 0.65rem;
    }}
    .stats-mini label {{ display: block; color: var(--muted); font-size: 0.62rem; text-transform: uppercase; }}
    .stats-mini--compact {{
      margin-top: 0.35rem;
      margin-bottom: 0.45rem;
      font-size: 0.72rem;
      opacity: 0.92;
    }}
    .meter {{
      height: 8px;
      border-radius: 999px;
      background: rgba(255,255,255,0.08);
      overflow: hidden;
    }}
    .meter-fill {{ height: 100%; border-radius: 999px; }}
    .meter-label {{
      display: flex;
      justify-content: space-between;
      font-size: 0.72rem;
      color: var(--muted);
      margin-bottom: 0.35rem;
    }}
    .band {{ text-transform: uppercase; font-size: 0.62rem; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.8rem;
    }}
    th, td {{
      border-bottom: 1px solid rgba(255,255,255,0.06);
      padding: 0.45rem 0.5rem;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .st-ok {{ color: #52be80; font-weight: 600; }}
    .st-warn {{ color: #f5b041; font-weight: 600; }}
    .empty {{ color: var(--muted); padding: 1rem 0; }}
    footer {{
      margin-top: 2rem;
      font-size: 0.74rem;
      color: var(--muted);
      border-top: 1px solid rgba(255,255,255,0.06);
      padding-top: 1rem;
    }}
    @media print {{
      body {{ padding: 0; background: #0f1419 !important; print-color-adjust: exact; -webkit-print-color-adjust: exact; }}
      section {{ break-inside: avoid; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="eyebrow">NetBox · Rack PDU connectivity audit</div>
      <h1>{rack_name}</h1>
      <div class="meta">
        <span><strong>Site</strong> {site}</span>
        <span><strong>Location</strong> {location}</span>
      </div>
      <div class="meta" style="margin-top:0.35rem">Generated {escape(ts_display)}</div>
      <div class="banner-stats">
        <div><label>Rack devices</label><b>{total_devices}</b></div>
        <div><label>Associated PDUs</label><b>{total_pdus}</b></div>
        <div><label>Flagged devices</label><b>{n_flagged}</b></div>
      </div>
    </header>

    <section aria-labelledby="sec-pdu">
      <h2 id="sec-pdu">PDU summary</h2>
      <p class="hint">PDU cards: red/blue order via <code>PDU_SUMMARY_RED_BEFORE_BLUE</code>.
      Per leg: <strong>Allocated</strong> sums downstream <strong>PowerPort.allocated_draw</strong> (W) on gear cabled to outlets on that feed leg.
      <strong>Available</strong> per leg uses upstream <strong>PowerFeed.available_power</strong> when one three-phase feed is modeled (capacity ÷ 3).
      Whole-feed remainder = feed total − total allocated (see summary row). Outlet bar = occupied outlets / total outlets.
      Role filter: <strong>{roles_esc}</strong>. Strict mode (device audit): <strong>{strict_esc}</strong>.</p>
      {pdu_section}
    </section>

    <section aria-labelledby="sec-dev">
      <h2 id="sec-dev">Device connectivity audit</h2>
      <p class="hint"><strong>Servers/switches:</strong> <strong>CONNECTED</strong> only when each power port traces by cable to a <strong>PowerOutlet</strong> on an associated PDU.
      <strong>Rack PDU devices</strong> (same associated set): intake <strong>PowerPort</strong> → <strong>PowerFeed</strong> is expected; we then require modeled <strong>PowerFeed.available_power</strong>. PDUs without usable upstream capacity stay flagged.</p>
      {device_table(device_rows, "audit-all")}
    </section>

    <section aria-labelledby="sec-flag">
      <h2 id="sec-flag">Flagged devices</h2>
      <p class="hint">Statuses other than CONNECTED require review.</p>
      {device_table(flagged, "audit-flagged")}
    </section>

    <footer>
      Power figures use modeled NetBox fields (<strong>allocated_draw</strong> on device power ports,
      <strong>available_power</strong> on upstream PowerFeeds). Per-leg availability is split only for a single three-phase feed.
    </footer>
  </div>
</body>
</html>
"""


class RackPduConnectivityAudit(Script):
    """Single-rack PDU outlet utilization and device-to-PDU trace audit (read-only)."""

    class Meta(Script.Meta):
        name = "Rack PDU Connectivity Audit"
        description = (
            "Read-only rack audit: associated PDUs (role/site/location rules), outlet utilization, "
            "and which rack devices trace to those PDU outlets. Outputs HTML + CSV under media/script-reports/."
        )
        commit_default = False

    rack = ObjectVar(model=Rack, description="Rack to audit")

    power_role_names = StringVar(
        description=(
            "Comma-separated DeviceRole names/slugs treated as PDUs for association "
            '(default identifies roles named or slugged "power" or "pdu").'
        ),
        required=False,
        default="power,pdu",
    )

    strict_mode = BooleanVar(
        description=(
            "If enabled, any ambiguous cable trace on a power port forces device status UNKNOWN "
            "(instead of classifying using conservative assumptions)."
        ),
        required=False,
        default=False,
    )

    def run(self, data, commit):
        rack = _resolve_rack(data["rack"])
        role_names = data.get("power_role_names") or "power,pdu"
        strict_mode = bool(data.get("strict_mode"))

        ts_token = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        ts_display = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        html_name = build_output_basename(rack.name or "rack", "pdu_connectivity_audit", ts_token)
        dev_name = build_output_basename(rack.name or "rack", "device_audit", ts_token)
        pdu_name = build_output_basename(rack.name or "rack", "pdu_summary", ts_token)
        rel_html = f"script-reports/{html_name}.html"
        rel_dev = f"script-reports/{dev_name}.csv"
        rel_pdu = f"script-reports/{pdu_name}.csv"

        web_html, path_html = _media_paths(rel_html)
        web_dev, path_dev = _media_paths(rel_dev)
        web_pdu, path_pdu = _media_paths(rel_pdu)

        pdus_raw = get_associated_pdus(rack, role_names)
        pdus = sort_associated_pdus_for_display(pdus_raw)
        associated_pdu_ids = {d.pk for d in pdus}

        pdu_summary_rows: list[dict[str, Any]] = []
        for pdu in pdus:
            total, used, free, pct = summarize_pdu_outlets(pdu)
            leg_power = summarize_pdu_leg_power(pdu)
            pdu_summary_rows.append(
                {
                    "rack_name": rack.name or "",
                    "site_name": rack.site.name if rack.site_id else "",
                    "pdu_name": pdu.name,
                    "pdu_role": pdu.role.name if pdu.role_id else "",
                    "power_in": describe_pdu_power_in(pdu),
                    **leg_power,
                    "total_outlets": total,
                    "used_outlets": used,
                    "free_outlets": free,
                    "outlet_utilization_percent": round(pct, 2),
                }
            )

        devices = get_rack_devices(rack)
        device_rows: list[DeviceAuditRow] = []
        for dev in devices:
            device_rows.append(
                evaluate_device_connectivity(
                    dev,
                    associated_pdu_ids,
                    strict_mode=strict_mode,
                )
            )

        req = getattr(self, "request", None)

        html_doc = build_html_report(
            rack,
            ts_display,
            pdu_summary_rows,
            device_rows,
            strict_mode=strict_mode,
            power_role_names=role_names,
        )

        try:
            path_html.parent.mkdir(parents=True, exist_ok=True)
            path_html.write_text(html_doc, encoding="utf-8")
            write_device_csv(device_rows, path_dev)
            write_pdu_csv(rack, pdu_summary_rows, path_pdu)
        except OSError as exc:
            self.log_failure(
                f"Could not write report files under MEDIA_ROOT ({exc}). "
                "Ensure the worker can write to MEDIA_ROOT/script-reports/."
            )
            self.log_info("```html\n" + html_doc[:8000] + "\n… (truncated)\n```")
            return

        u_html = _absolute_uri_best_effort(req, web_html)
        u_dev = _absolute_uri_best_effort(req, web_dev)
        u_pdu = _absolute_uri_best_effort(req, web_pdu)

        self.log_success(
            f"**HTML report:** [Open]({u_html}) — `{rel_html}`\n\n"
            f"**Device audit CSV:** [Download]({u_dev}) — `{rel_dev}`\n\n"
            f"**PDU summary CSV:** [Download]({u_pdu}) — `{rel_pdu}`"
        )

        self.log_info(
            f"**Associated PDUs:** {len(pdus)} · **Rack devices:** {len(device_rows)} · "
            f"**Flagged:** {sum(1 for r in device_rows if r.status != STATUS_CONNECTED)}"
        )
