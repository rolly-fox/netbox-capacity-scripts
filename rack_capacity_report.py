"""
NetBox custom script — Rack Capacity Report

Read-only report for a single rack: utilization from NetBox, approximate free RU, distinct
1U–4U mount opportunities, and front/rear elevations (REST API SVG / iframe, same source as the
rack UI). Optional inlined SVG for offline HTML when the worker can fetch the API.

Deploy via Customization → Scripts (NetBox Enterprise / standard docs). Writes HTML under
``MEDIA_ROOT/script-reports/`` and logs a ``/media/...`` link to the job output.
"""

import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from django.conf import settings
from django.utils.html import escape

from dcim.models import Rack
from extras.scripts import ObjectVar, Script


def _resolve_rack(rack):
    """ObjectVar may pass a model instance (UI) or pk (API); normalize to Rack."""
    if isinstance(rack, Rack):
        return rack
    return Rack.objects.get(pk=rack)


def _report_file_paths(rack: Rack):
    """Return (download_filename, web_path, filesystem Path) for this report."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"rack-capacity-report-{rack.pk}-{ts}.html"
    dest = Path(settings.MEDIA_ROOT) / "script-reports" / filename
    rel = f"script-reports/{filename}"
    mu = settings.MEDIA_URL
    if not mu.endswith("/"):
        mu = f"{mu}/"
    web_path = f"{mu}{rel}"
    if not web_path.startswith("/"):
        web_path = f"/{web_path}"
    return filename, web_path, dest


# Rack elevations embed the REST API SVG URLs (see NetBox /api/dcim/racks/<id>/elevation/?render=svg).
# The browser loads those like the rack UI; server-side get_elevation_svg() in scripts often fails
# on RQ workers missing collected static CSS, which produced blank elevations.


def _approx_free_ru(total_ru: int, utilization_pct: float) -> float:
    """Rough free RU from utilization % (uniform estimate; fragmentation may reduce usable space)."""
    return round(max(0.0, float(total_ru) * (100.0 - float(utilization_pct)) / 100.0), 1)


def _absolute_uri_best_effort(request, path: str) -> str:
    """
    NetBox UI pages use paths like /api/... and /media/...
    If the saved HTML is opened via file://, relative paths break (browser looks on disk).
    Build https://host/... when possible so links and iframes still target NetBox.
    """
    if not path.startswith("/"):
        path = "/" + path
    if request is not None:
        if hasattr(request, "build_absolute_uri"):
            try:
                return request.build_absolute_uri(path)
            except Exception:
                pass
        meta = getattr(request, "META", None) or {}
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


def _elevation_api_url(request, rack: Rack, face: str) -> str:
    q = urlencode(
        {"face": face, "render": "svg", "include_images": "true"}
    )
    try:
        from django.urls import reverse

        base = reverse("dcim-api:rack-elevation", kwargs={"pk": rack.pk})
        path = f"{base}?{q}"
    except Exception:
        path = f"/api/dcim/racks/{rack.pk}/elevation/?{q}"
    return _absolute_uri_best_effort(request, path)


def _strip_xml_declaration(svg: str) -> str:
    s = svg.strip()
    if s.startswith("<?xml"):
        end = s.find("?>")
        if end != -1:
            return s[end + 2 :].strip()
    return svg


def _fetch_elevation_svg_via_request(request, rack: Rack, face: str):
    """
    GET the same API SVG the iframe uses, reusing the runner's session cookie when possible.
    If this succeeds, SVG can be embedded in the saved HTML for offline viewing.
    """
    if not request:
        return None
    url = _elevation_api_url(request, rack, face)
    if url.startswith("/"):
        return None
    headers = {}
    cookie = getattr(request, "META", {}).get("HTTP_COOKIE")
    if cookie:
        headers["Cookie"] = cookie
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
    except (urllib.error.URLError, OSError, UnicodeDecodeError, ValueError):
        return None
    if "svg" not in body.lower():
        return None
    return body


def _placement_opportunities(rack: Rack):
    """
    Count valid starting rack-unit positions per device height (NetBox get_available_units).
    These are not additive; each is independent under NetBox placement rules.
    """
    kw = {}
    try:
        rack.get_available_units(u_height=1, ignore_excluded_devices=True)
        kw["ignore_excluded_devices"] = True
    except TypeError:
        pass

    out = {}
    for h in (1, 2, 3, 4):
        try:
            if kw:
                lst = rack.get_available_units(u_height=h, **kw)
            else:
                lst = rack.get_available_units(u_height=h)
            out[h] = len(lst)
        except Exception:
            out[h] = None
    return out


def _utilization_style(pct: float):
    """Return (accent color, label class) for utilization meter."""
    if pct >= 90:
        return ("#c0392b", "critical")
    if pct >= 75:
        return ("#d68910", "high")
    if pct >= 50:
        return ("#2980b9", "moderate")
    return ("#1e8449", "ok")


def _build_report_html(
    rack: Rack,
    total_ru: int,
    utilization_pct: float,
    report_href=None,
    download_filename=None,
    svg_front_inline=None,
    svg_rear_inline=None,
    http_request=None,
) -> str:
    """Build the rack capacity report HTML (escaped metadata; iframe + optional inlined API SVG)."""
    site = escape(rack.site.name) if rack.site else "—"
    location = escape(rack.location.name) if rack.location else "—"
    name = escape(rack.name)
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    accent, band = _utilization_style(utilization_pct)
    pct_safe = f"{utilization_pct:.1f}"
    approx_free = _approx_free_ru(total_ru, utilization_pct)
    approx_free_safe = f"{approx_free:.1f}"
    placements = _placement_opportunities(rack)
    placement_li_parts = []
    for h in (1, 2, 3, 4):
        n = placements.get(h)
        if n is not None:
            placement_li_parts.append(
                f"<li><strong>{n}</strong> distinct spots where a <strong>{h}U</strong> device "
                "could be mounted (bottom U of the install)</li>"
            )
    placement_list_html = (
        "<ul class=\"placement-ul\">" + "".join(placement_li_parts) + "</ul>"
        if placement_li_parts
        else "<p>—</p>"
    )
    has_baked_svg = bool(svg_front_inline or svg_rear_inline)

    if report_href and download_filename:
        href_esc = escape(report_href)
        fn_esc = escape(download_filename)
        dl_hint_extra = ""
        if has_baked_svg:
            dl_hint_extra = (
                " Bundled SVG copies are under each elevation (expand "
                "<strong>Copy saved in this HTML file</strong>) so diagrams work offline."
            )
        dl_section = (
            f'<p class="dl-row"><a class="dl-btn" href="{href_esc}" download="{fn_esc}">'
            "Download rack report (HTML)</a></p>"
            '<p class="dl-hint">Or use your browser’s <strong>File → Save Page As</strong>.'
            f"{dl_hint_extra}</p>"
        )
    else:
        dl_section = (
            '<p class="dl-hint">Export this page from your browser (<strong>Save Page As</strong>) '
            "for an offline copy.</p>"
        )

    # Absolute https URLs so Save As → open from disk still loads /api/... from NetBox (not file:///api/...).
    fu_raw = _elevation_api_url(http_request, rack, "front")
    ru_raw = _elevation_api_url(http_request, rack, "rear")
    front_src = escape(fu_raw)
    rear_src = escape(ru_raw)
    url_warning = ""
    if not (fu_raw.startswith("http://") or fu_raw.startswith("https://")):
        url_warning = (
            " <strong>Note:</strong> Could not build absolute elevation URLs (no host on request). "
            "Open this page while logged into NetBox over <strong>https</strong>, not from a saved "
            "<code>file://</code> copy, or live iframes will break."
        )

    front_baked = ""
    if svg_front_inline:
        front_baked = (
            '<details class="elev-pack"><summary>Copy saved in this HTML file (offline)</summary>'
            '<div class="svg-inline-host">'
            f"{_strip_xml_declaration(svg_front_inline)}"
            "</div></details>"
        )
    rear_baked = ""
    if svg_rear_inline:
        rear_baked = (
            '<details class="elev-pack"><summary>Copy saved in this HTML file (offline)</summary>'
            '<div class="svg-inline-host">'
            f"{_strip_xml_declaration(svg_rear_inline)}"
            "</div></details>"
        )

    if has_baked_svg:
        baked_note = (
            " The collapsed blocks below embed the same SVG in this file so elevations survive "
            "<strong>Save As</strong> when live iframes cannot reload."
        )
    else:
        baked_note = (
            " Downloaded HTML usually cannot reload iframe elevations (no login). "
            "Use <strong>Print → Save as PDF</strong> while viewing this page in NetBox, or run the "
            "script when the server can bundle SVG (see collapsed sections when present)."
        )

    if has_baked_svg:
        file_banner_inner = (
            "If you opened this file from disk (<code>file://</code>), your browser blocks live NetBox "
            "pages inside iframes (security). The previews below are auto-hidden; bundled SVG is expanded."
        )
    else:
        file_banner_inner = (
            "If you opened this file from disk, live elevations cannot load (browser blocks embedded "
            "API pages). Open this report from NetBox’s <code>/media/…</code> URL while logged in, or "
            "use <strong>Print → Save as PDF</strong> from NetBox."
        )

    # Match NetBox’s embedded elevation page height (~linear in U). +54px ≈ 3U breathing room at 18px/U.
    elev_iframe_px = min(2200, max(480, total_ru * 18 + 194))

    elevation_block = (
        '<div class="elev-print-sheet">'
        '<div class="card card-elev" id="rack-elevations">'
        f'<p class="print-only elevation-print-head">Rack capacity report · {name} · {gen}</p>'
        '<div class="elev-header screen-only">Rack elevation</div>'
        f'<p id="elev-file-banner" class="elev-banner" hidden>{file_banner_inner}</p>'
        '<div class="elevation-grid">'
        '<div class="elevation-panel"><h2>Front</h2><div class="svg-host">'
        f'<iframe class="elevation-embed" src="{front_src}" title="Front elevation" '
        'loading="lazy" referrerpolicy="same-origin"></iframe>'
        '<p class="elev-iframe-replacement" hidden>Live preview is for viewing this page on NetBox '
        "over HTTPS. In a saved file, use the bundled SVG below.</p>"
        f"{front_baked}"
        '</div></div>'
        '<div class="elevation-panel"><h2>Rear</h2><div class="svg-host">'
        f'<iframe class="elevation-embed" src="{rear_src}" title="Rear elevation" '
        'loading="lazy" referrerpolicy="same-origin"></iframe>'
        '<p class="elev-iframe-replacement" hidden>Live preview is for viewing this page on NetBox '
        "over HTTPS. In a saved file, use the bundled SVG below.</p>"
        f"{rear_baked}"
        '</div></div>'
        "</div>"
        '<p class="elev-footnote screen-only">On NetBox over HTTPS, live views use the API (session). '
        "Saved <code>file://</code> copies cannot embed those pages in iframes (browser policy)."
        f"{url_warning}{baked_note}</p>"
        '<p class="elev-footnote elev-footnote--abbr print-only">'
        "Diagrams use your NetBox session over HTTPS; a saved HTML file may not show live iframe elevations.</p>"
        "</div>"
        "</div>"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark">
  <title>Rack capacity report - {name}</title>
  <style>
    :root {{
      --bg: #0f1419;
      --card: #1a2332;
      --text: #e8eef5;
      --muted: #8b9cb3;
      --accent: {accent};
      --radius: 12px;
      --font: "Segoe UI", system-ui, -apple-system, sans-serif;
      --elev-iframe-h: {elev_iframe_px}px;
    }}
    * {{ box-sizing: border-box; }}
    html {{
      background: #0f1419;
      color-scheme: dark;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: var(--font);
      background: linear-gradient(160deg, #0a0e14 0%, #151d2a 50%, #0f1419 100%);
      background-color: #0f1419;
      color: var(--text);
      padding: 2.5rem 1.25rem 3rem;
      line-height: 1.5;
    }}
    .wrap {{
      max-width: 56rem;
      margin: 0 auto;
    }}
    header {{
      margin-bottom: 1.75rem;
    }}
    .eyebrow {{
      font-size: 0.75rem;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 0.35rem;
    }}
    h1 {{
      font-size: 1.65rem;
      font-weight: 600;
      margin: 0 0 0.5rem;
      letter-spacing: -0.02em;
    }}
    .meta {{
      font-size: 0.875rem;
      color: var(--muted);
    }}
    .meta span {{ margin-right: 1rem; }}
    .card {{
      background: var(--card);
      border-radius: var(--radius);
      padding: 1.75rem 1.5rem;
      box-shadow: 0 8px 32px rgba(0,0,0,0.35), 0 0 0 1px rgba(255,255,255,0.04);
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 1.25rem;
      margin-bottom: 1.25rem;
    }}
    @media (max-width: 720px) {{
      .stats {{ grid-template-columns: 1fr; }}
    }}
    .stat {{
      background: rgba(0,0,0,0.2);
      border-radius: 10px;
      padding: 1rem 1.1rem;
    }}
    .stat label {{
      display: block;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 0.35rem;
    }}
    .stat .value {{
      font-size: 1.75rem;
      font-weight: 700;
      letter-spacing: -0.03em;
    }}
    .stat .unit {{ font-size: 1rem; font-weight: 500; color: var(--muted); margin-left: 0.15rem; }}
    .meter-wrap {{
      margin-top: 0.25rem;
    }}
    .meter-label {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      font-size: 0.8rem;
      color: var(--muted);
      margin-bottom: 0.5rem;
    }}
    .meter {{
      height: 10px;
      border-radius: 999px;
      background: rgba(255,255,255,0.08);
      overflow: hidden;
    }}
    .meter-fill {{
      height: 100%;
      width: min(100%, {pct_safe}%);
      border-radius: 999px;
      background: var(--accent);
      box-shadow: inset 0 0 12px rgba(255,255,255,0.15);
      transition: width 0.4s ease;
    }}
    .capacity-extra {{
      margin-top: 1rem;
      padding: 1rem 1.1rem;
      border-radius: 10px;
      background: rgba(0,0,0,0.18);
      font-size: 0.84rem;
      color: var(--text);
      line-height: 1.55;
    }}
    .capacity-extra strong {{ color: var(--muted); font-weight: 600; }}
    .capacity-extra .placement-ul {{
      margin: 0.45rem 0 0;
      padding-left: 1.15rem;
    }}
    .capacity-extra .placement-ul li {{ margin: 0.2rem 0; }}
    .capacity-extra .hint {{
      margin: 0.65rem 0 0;
      font-size: 0.74rem;
      color: var(--muted);
    }}
    .band {{ font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--accent); }}
    footer {{
      margin-top: 1.5rem;
      padding-top: 1.25rem;
      border-top: 1px solid rgba(255,255,255,0.06);
      font-size: 0.78rem;
      color: var(--muted);
      line-height: 1.55;
    }}
    footer > p:first-of-type {{ margin-top: 0; }}
    .dl-row {{ margin: 1rem 0 0.35rem; }}
    .dl-btn {{
      display: inline-flex;
      align-items: center;
      padding: 0.55rem 1.1rem;
      border-radius: 10px;
      background: rgba(255,255,255,0.09);
      border: 1px solid rgba(255,255,255,0.14);
      color: var(--text);
      text-decoration: none;
      font-size: 0.88rem;
      font-weight: 600;
      letter-spacing: 0.02em;
    }}
    .dl-btn:hover {{ background: rgba(255,255,255,0.14); }}
    .dl-hint {{ margin: 0.5rem 0 0; font-size: 0.75rem; opacity: 0.92; }}
    .card-elev {{
      margin-top: 1.25rem;
      padding: 1.5rem 1.25rem 1.25rem;
    }}
    .elev-header {{
      font-size: 0.75rem;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 1rem;
    }}
    .elevation-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1.25rem;
      align-items: start;
    }}
    @media (max-width: 900px) {{
      .elevation-grid {{ grid-template-columns: 1fr; }}
    }}
    .elevation-panel {{
      background: rgba(0,0,0,0.18);
      border-radius: 10px;
      padding: 0.85rem 0.75rem 1rem;
      overflow: visible;
      min-width: 0;
    }}
    .elevation-panel h2 {{
      font-size: 0.68rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--muted);
      margin: 0 0 0.65rem;
    }}
    .svg-host {{
      /* Avoid nested scroll: iframe height matches rack elevation document (scaled by U count). */
      overflow: visible;
    }}
    .svg-host iframe.elevation-embed {{
      display: block;
      width: 100%;
      height: var(--elev-iframe-h);
      min-height: var(--elev-iframe-h);
      max-height: none;
      border: none;
      border-radius: 8px;
      background: rgba(0,0,0,0.35);
    }}
    .elev-banner {{
      margin: 0 0 1rem;
      padding: 0.75rem 1rem;
      border-radius: 8px;
      background: rgba(220, 53, 69, 0.12);
      border: 1px solid rgba(220, 53, 69, 0.28);
      font-size: 0.82rem;
      line-height: 1.45;
    }}
    .elev-banner code {{ font-size: 0.78em; }}
    .elev-iframe-replacement {{
      margin: 0.5rem 0 0;
      font-size: 0.78rem;
      color: var(--muted);
      line-height: 1.4;
    }}
    .elev-footnote {{
      margin: 1rem 0 0;
      font-size: 0.72rem;
      color: var(--muted);
      line-height: 1.5;
    }}
    .elev-pack {{
      margin-top: 0.65rem;
      font-size: 0.78rem;
      color: var(--muted);
    }}
    .elev-pack summary {{
      cursor: pointer;
      color: var(--accent);
      font-weight: 600;
    }}
    .svg-inline-host {{
      margin-top: 0.5rem;
      overflow: visible;
      max-width: 100%;
      border-radius: 8px;
      background: rgba(0,0,0,0.25);
    }}
    .svg-inline-host svg {{
      display: block;
      width: 100%;
      max-width: 100%;
      height: auto;
    }}
    .print-only {{
      display: none;
    }}
    @media print {{
      @page {{
        size: letter portrait;
        margin: 10mm 8mm 12mm 8mm;
      }}
      body {{
        background: linear-gradient(160deg, #0a0e14 0%, #151d2a 50%, #0f1419 100%) !important;
        background-color: #0f1419 !important;
        color: var(--text) !important;
        padding: 0 !important;
        print-color-adjust: exact;
        -webkit-print-color-adjust: exact;
      }}
      /* Loose letter-spacing + uppercase looks broken in PDF engines (\"N E T B O X\") */
      .eyebrow,
      h1,
      .meta,
      .elev-header,
      .elevation-panel h2,
      .band {{
        letter-spacing: normal !important;
        word-spacing: normal !important;
        font-kerning: normal;
      }}
      .screen-only {{
        display: none !important;
      }}
      .print-only {{
        display: block;
      }}
      .wrap {{
        max-width: none !important;
        margin: 0 !important;
      }}
      .card,
      .card-elev,
      .elevation-panel {{
        background: var(--card) !important;
        box-shadow: none !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
      }}
      .stat {{
        background: rgba(0,0,0,0.25) !important;
      }}
      .capacity-extra {{
        background: rgba(0,0,0,0.22) !important;
        color: var(--text) !important;
      }}
      footer {{
        border-top-color: rgba(255,255,255,0.08) !important;
        color: var(--muted) !important;
      }}
      footer code {{
        color: var(--muted) !important;
      }}
      .dl-row,
      .dl-hint {{
        display: none !important;
      }}
      /* avoid-page here often causes an extra blank sheet in Chrome print-to-PDF */
      .report-summary {{
        break-after: auto;
        page-break-after: auto;
      }}
      .report-elevations {{
        break-before: page;
        page-break-before: always;
      }}
      /*
        Front | rear stay side-by-side for PDF (screen matches @media max-width breakpoint).
        Do not use zoom() here — Chromium print often clips the TOP of scaled blocks (drops RU1–RU14 style).
        Tall elevations may continue on the next page instead of shrinking to one sheet.
      */
      .report-elevations .elevation-grid {{
        grid-template-columns: 1fr 1fr !important;
        gap: 0.35rem;
        align-items: start;
        page-break-after: avoid;
        break-after: avoid-page;
      }}
      .report-elevations .elev-footnote.elev-footnote--abbr {{
        page-break-before: avoid;
        break-before: avoid-page;
        margin-top: 0.35rem !important;
      }}
      .report-elevations,
      .report-elevations .elev-print-sheet,
      .report-elevations .card-elev,
      .report-elevations .elevation-grid,
      .report-elevations .elevation-panel,
      .report-elevations .svg-host {{
        overflow: visible !important;
      }}
      .report-elevations .card-elev {{
        padding: 0.65rem 0.5rem !important;
        margin-top: 0.35rem !important;
      }}
      .elevation-panel {{
        break-inside: auto;
        page-break-inside: auto;
      }}
      .elev-footnote {{
        font-size: 7pt;
        line-height: 1.35;
      }}
      .report-elevations .card-elev .elevation-print-head {{
        margin: 0 0 6px !important;
        padding-bottom: 4px !important;
      }}
      .svg-host iframe.elevation-embed {{
        height: var(--elev-iframe-h);
        min-height: var(--elev-iframe-h);
        max-height: none;
        background: rgba(0,0,0,0.35) !important;
      }}
      .elevation-print-head {{
        font-size: 9pt;
        font-weight: 600;
        margin: 0 0 10px;
        padding-bottom: 6px;
        border-bottom: 1px solid rgba(255,255,255,0.12);
        color: var(--text) !important;
      }}
      .elev-banner {{
        background: rgba(220, 53, 69, 0.14) !important;
        border-color: rgba(220, 53, 69, 0.35) !important;
        color: var(--text) !important;
      }}
      a.dl-btn {{
        border: 1px solid rgba(255,255,255,0.14) !important;
        color: var(--text) !important;
        background: rgba(255,255,255,0.06) !important;
      }}
      .elev-footnote,
      .elev-footnote--abbr {{
        color: var(--muted) !important;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="report-summary">
    <header>
      <div class="eyebrow">NetBox · Rack capacity report</div>
      <h1>{name}</h1>
      <div class="meta">
        <span><strong>Site</strong> {site}</span>
        <span><strong>Location</strong> {location}</span>
      </div>
      <div class="meta" style="margin-top:0.35rem">Generated {gen}</div>
    </header>
    <div class="card">
      <div class="stats">
        <div class="stat">
          <label>Total height</label>
          <div class="value">{total_ru}<span class="unit">U</span></div>
        </div>
        <div class="stat">
          <label>Utilization</label>
          <div class="value">{pct_safe}<span class="unit">%</span></div>
        </div>
        <div class="stat">
          <label>Approx. free RU</label>
          <div class="value">{approx_free_safe}<span class="unit">U</span></div>
        </div>
      </div>
      <div class="capacity-extra">
        <p style="margin:0"><strong>How many more devices can this rack take?</strong></p>
        <p style="margin:0.45rem 0 0">NetBox counts separate install opportunities by device height (physical “could we put a new XU box starting here?”):</p>
        {placement_list_html}
        <p class="hint"><strong>Approx. free RU ({approx_free_safe} U)</strong> is a rough total from utilization; real gaps may be split across the rack. Each line above is independent—after you mount gear, these numbers change; spots for different heights overlap in the same real estate.</p>
      </div>
      <div class="meter-wrap">
        <div class="meter-label">
          <span>Capacity used</span>
          <span class="band">{band}</span>
        </div>
        <div class="meter" role="progressbar" aria-valuenow="{pct_safe}" aria-valuemin="0" aria-valuemax="100">
          <div class="meter-fill"></div>
        </div>
      </div>
      <footer>
        <p>Utilization follows NetBox <code>Rack.get_utilization()</code>: occupied and reserved units
        count toward the percentage.</p>
        {dl_section}
        <p class="screen-only print-tip">When you <strong>Print → Save as PDF</strong>, set
        <strong>Headers and footers</strong> to <strong>off</strong> (Chrome/Edge) so the browser does not add
        date, title, URL, and page numbers on every page—those stack on top of the report layout.</p>
        <p class="screen-only print-tip"><strong>PDF workaround (pagination):</strong> If extra blank or awkward
        sheets appear, use <strong>More settings → Pages → Custom</strong> and print only what you need—for example
        odd pages (<code>1,3,5,7</code>) if that matches your layout—or use your OS/printer <strong>Odd pages only</strong>
        when available. We can tighten layout later; this avoids reprinting wasted pages for now.</p>
      </footer>
    </div>
    </div>
    <div class="report-elevations">
    {elevation_block}
    </div>
  </div>
  <script>
  (function () {{
    try {{
      if (location.protocol !== "file:") return;
      var ban = document.getElementById("elev-file-banner");
      if (ban) ban.hidden = false;
      document.querySelectorAll("iframe.elevation-embed").forEach(function (el) {{
        el.style.display = "none";
      }});
      document.querySelectorAll(".elev-iframe-replacement").forEach(function (el) {{
        el.hidden = false;
      }});
      document.querySelectorAll("details.elev-pack").forEach(function (d) {{
        d.open = true;
      }});
    }} catch (e) {{}}
  }})();
  </script>
</body>
</html>
"""


class RackCapacityReport(Script):
    """Single-rack capacity and utilization report (read-only; writes HTML under media)."""

    class Meta(Script.Meta):
        name = "Rack capacity report"
        description = (
            "Read-only report for one rack: utilization %, approximate free RU, distinct 1U–4U mount "
            "opportunities, dark-theme HTML with side-by-side front/rear elevations and optional offline "
            "SVG copies. Output: media/script-reports/."
        )
        commit_default = False

    selected_rack = ObjectVar(
        model=Rack,
        description="Rack for this report",
    )

    def run(self, data, commit):
        rack = _resolve_rack(data["selected_rack"])

        total_ru = rack.u_height
        utilization_pct = rack.get_utilization()

        self.log_info(f"**Rack:** {rack.name}")
        if rack.site:
            self.log_info(f"**Site:** {rack.site.name}")
        if rack.location:
            self.log_info(f"**Location:** {rack.location.name}")

        self.log_info(f"**Total height (U):** {total_ru}")
        self.log_success(f"**Utilization (NetBox):** {utilization_pct:.1f}%")
        af = _approx_free_ru(total_ru, utilization_pct)
        self.log_info(f"**Approx. free RU:** {af:.1f} U")
        pl = _placement_opportunities(rack)
        for h in (1, 2, 3, 4):
            n = pl.get(h)
            if n is not None:
                self.log_info(f"**New {h}U installs possible (distinct spots):** {n}")

        self.log_info(
            "_Percentage follows `Rack.get_utilization()` (occupied and reserved units count "
            "toward utilization)._"
        )

        self.log_info("---")
        self.log_info(
            "_NetBox often strips `data:` URLs from script logs for security, so this script "
            "writes an HTML file under **media** and links to it instead._"
        )

        req = getattr(self, "request", None)
        svg_front_inline = _fetch_elevation_svg_via_request(req, rack, "front")
        svg_rear_inline = _fetch_elevation_svg_via_request(req, rack, "rear")
        if svg_front_inline or svg_rear_inline:
            self.log_info(
                "_Bundled SVG copies embedded in the HTML for offline viewing (when API fetch succeeded)._"
            )

        filename, web_path, dest = _report_file_paths(rack)
        report_url = _absolute_uri_best_effort(req, web_path)
        html_doc = _build_report_html(
            rack,
            total_ru,
            utilization_pct,
            report_href=report_url,
            download_filename=filename,
            svg_front_inline=svg_front_inline,
            svg_rear_inline=svg_rear_inline,
            http_request=req,
        )

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(html_doc, encoding="utf-8")
            # NetBox job workers often use NetBoxFakeRequest — no build_absolute_uri().
            download_url = _absolute_uri_best_effort(req, web_path)
            self.log_success(
                f"**Rack capacity report:** [Open or download]({download_url}) "
                "_(relative `/media/...` links resolve on your NetBox server; **Save As…** for a local copy)._"
            )
        except OSError as exc:
            self.log_failure(
                f"Could not write HTML under `MEDIA_ROOT` ({exc}). "
                "Ensure the NetBox worker can write to `MEDIA_ROOT/script-reports/`, or copy from below."
            )
            fallback_html = _build_report_html(
                rack,
                total_ru,
                utilization_pct,
                svg_front_inline=svg_front_inline,
                svg_rear_inline=svg_rear_inline,
                http_request=req,
                report_href=_absolute_uri_best_effort(req, web_path),
                download_filename=filename,
            )
            self.log_info("```html\n" + fallback_html + "\n```")
