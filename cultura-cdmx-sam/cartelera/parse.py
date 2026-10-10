"""Builds Parquet-ready records from cartelera listing items + detail payloads.

Replicates the fields the Playwright scraper produced (kept identical so the
existing dashboard query keeps working), plus extra columns now available
directly from the site's JSON payloads.
"""

from datetime import date, datetime, timedelta, timezone

from bs4 import BeautifulSoup

from client import detail_url

# Mexico City runs on a fixed UTC-6 offset (DST abolished in 2022).
MEXICO_TZ = timezone(timedelta(hours=-6))

WEEKDAYS = ("Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo")
MONTHS = (
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
)


def mexico_today() -> date:
    return datetime.now(MEXICO_TZ).date()


def spanish_date_label(d: date) -> str:
    """Moment.js LLL-style label without time, e.g. 'Viernes, 9 De Octubre De 2026'."""
    return f"{WEEKDAYS[d.weekday()]}, {d.day} De {MONTHS[d.month - 1]} De {d.year}"


def _parse_day(raw) -> date:
    try:
        return datetime.strptime(str(raw).strip(), "%d/%m/%Y").date()
    except (ValueError, TypeError):
        return None


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_schedule(schedule_map, today: date) -> dict:
    """Replicates the site's displayed schedule: the earliest event_date >=
    today across all schedule entries, with that entry's hours joined by ' '."""
    best = None
    for entry in (schedule_map or {}).values():
        if not isinstance(entry, dict):
            continue
        hours = " ".join(h for h in (entry.get("event_hours") or []) if h)
        for raw in entry.get("event_dates") or []:
            d = _parse_day(raw)
            if d is None or d < today:
                continue
            if best is None or d < best[0]:
                best = (d, hours)
    if best is None:
        return {"date": None, "hour": None}
    return {"date": spanish_date_label(best[0]), "hour": best[1]}


def parse_costs(costs_map) -> list:
    costs = []
    for entry in (costs_map or {}).values():
        if not isinstance(entry, dict):
            continue
        name = (entry.get("cost_name") or "").strip()
        price = (entry.get("cost_price") or "").strip()
        label = ": ".join(part for part in (name, price) if part)
        if label:
            costs.append(label)
    return costs


def parse_description(description_html) -> list:
    """Text of each <p> in the event_description HTML fragment (empties dropped)."""
    if not description_html:
        return []
    soup = BeautifulSoup(description_html, "html.parser")
    return [
        text
        for text in (p.get_text().strip() for p in soup.find_all("p"))
        if text
    ]


def build_info(event_info: dict, costs: list) -> list:
    items = [
        "Duración: " + str(event_info.get("event_duration") or ""),
        "Edad: " + str(event_info.get("event_audience") or ""),
        ("Accesibilidad: " + str(event_info.get("event_accesibility") or "")).strip(),
        "Costos: " + "; ".join(costs) if costs else "Costos:",
    ]
    items.extend(costs)
    items.append("Evento publicado por: " + str(event_info.get("event_entity") or ""))
    return items


def all_future_dates(schedule_map, today: date) -> list:
    """All distinct event_dates >= today, sorted, ISO formatted."""
    dates = set()
    for entry in (schedule_map or {}).values():
        if not isinstance(entry, dict):
            continue
        for raw in entry.get("event_dates") or []:
            d = _parse_day(raw)
            if d is not None and d >= today:
                dates.add(d)
    return [d.isoformat() for d in sorted(dates)]


def build_record(listing_item: dict, event_info, page_number: int, card_index: int, today: date) -> dict:
    """Build one output record. Pass event_info=None to emit a failure record
    (listing-derived fields filled, detail-derived fields null)."""
    event_id = str(listing_item.get("event_id") or "")
    located = listing_item.get("event_date_located") or ""
    located_day = _parse_day(located)

    record = {
        "page_number": page_number,
        "card_index": card_index,
        "detail_url": detail_url(event_id, located, listing_item.get("event_name")),
        "description": None,
        "info": None,
        "schedule": None,
        "location": None,
        "banner_url": None,
        "evento": listing_item.get("event_name"),
        "recinto": listing_item.get("event_venue"),
        "event_id": event_id or None,
        "event_type": listing_item.get("event_type"),
        "fecha_iso": located_day.isoformat() if located_day else None,
        "lat": _to_float(listing_item.get("event_lat")),
        "lon": _to_float(listing_item.get("event_lon")),
        "venue_id": None,
        "event_url": None,
        "costs": None,
        "all_dates": None,
    }
    if not event_info:
        return record

    costs = parse_costs(event_info.get("costs"))
    location = event_info.get("event_location") or {}
    venue_id = event_info.get("venue_id_id")
    record.update(
        {
            "description": parse_description(event_info.get("event_description")),
            "info": build_info(event_info, costs),
            "schedule": build_schedule(event_info.get("schedule"), today),
            "location": location.get("address"),
            "banner_url": event_info.get("event_image"),
            "evento": event_info.get("event_name") or record["evento"],
            "recinto": event_info.get("venue_name") or record["recinto"],
            "lat": _to_float(location.get("lat")) or record["lat"],
            "lon": _to_float(location.get("lng")) or record["lon"],
            "venue_id": str(venue_id) if venue_id is not None else None,
            "event_url": event_info.get("event_url"),
            "costs": costs,
            "all_dates": all_future_dates(event_info.get("schedule"), today),
        }
    )
    return record
