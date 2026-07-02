"""
metadata.py
Loads the ems.* metadata tables into memory once at Flink job startup.
Provides O(1) lookup: device_id → (tag_id, area_id, line_id, plant_id)
Also resolves area names → area_id for aggregate messages.

Why in-memory?
  - Metadata tables are small (tens to low hundreds of rows)
  - Every Kafka message needs a lookup — a DB round-trip per message would
    be the bottleneck; a local dict is ~100ns
  - Metadata changes rarely; the job can be restarted if new equipment is added
"""

import logging
import psycopg2
from typing import Dict, Optional, Tuple

import config
from models import ResolvedIds

log = logging.getLogger(__name__)


class MetadataCache:
    """
    Thread-safe read-only cache populated once at startup.
    Keys:
      device_id  → ResolvedIds       (for PM and PV messages)
      area_name  → area_id           (for energy aggregate messages)
      line_code  → line_id           (for water/steam aggregate messages)
    """

    def __init__(self):
        # device_id → ResolvedIds
        self._by_device: Dict[str, ResolvedIds] = {}
        # lowercase area_name → area_id  (e.g. "extraction" → 2)
        self._by_area: Dict[str, int] = {}
        # lowercase line_code → line_id  (e.g. "line-1" → 1)
        self._by_line: Dict[str, int] = {}

    def load(self) -> None:
        """
        Pull all metadata from TimescaleDB.
        Called once before env.execute().
        """
        conn = psycopg2.connect(
            host     = config.TIMESCALE_HOST,
            port     = config.TIMESCALE_PORT,
            dbname   = config.TIMESCALE_DB,
            user     = config.TIMESCALE_USER,
            password = config.TIMESCALE_PASSWORD,
        )
        try:
            with conn.cursor() as cur:
                self._load_equipment(cur)
                self._load_areas(cur)
                self._load_lines(cur)
        finally:
            conn.close()

        log.info(
            "MetadataCache loaded — %d devices, %d areas, %d lines",
            len(self._by_device), len(self._by_area), len(self._by_line)
        )

    def _load_equipment(self, cur) -> None:
        """
        Join equipment → areas → production_lines → plants
        to build the full ID chain for each device_id.
        """
        cur.execute("""
            SELECT
                e.device_id,
                e.tag_id,
                a.area_id,
                pl.line_id,
                p.plant_id
            FROM ems.equipment e
            JOIN ems.areas           a  ON a.area_id  = e.area_id
            JOIN ems.production_lines pl ON pl.line_id = a.line_id
            JOIN ems.plants           p  ON p.plant_id = pl.plant_id
        """)
        for row in cur.fetchall():
            device_id, tag_id, area_id, line_id, plant_id = row
            self._by_device[device_id] = ResolvedIds(
                plant_id = plant_id,
                line_id  = line_id,
                area_id  = area_id,
                tag_id   = tag_id,
            )

    def _load_areas(self, cur) -> None:
        """area_name (lowercased) → area_id for energy aggregates."""
        cur.execute("""
            SELECT LOWER(a.area_name), a.area_id
            FROM ems.areas a
        """)
        for area_name, area_id in cur.fetchall():
            self._by_area[area_name] = area_id

    def _load_lines(self, cur) -> None:
        """line_code (lowercased) → line_id for water/steam aggregates."""
        cur.execute("""
            SELECT LOWER(pl.line_code), pl.line_id
            FROM ems.production_lines pl
        """)
        for line_code, line_id in cur.fetchall():
            self._by_line[line_code] = line_id

    # ── Lookup API ────────────────────────────────────────────────────────────

    def resolve_device(self, device_id: str) -> Optional[ResolvedIds]:
        """Returns ResolvedIds or None if device_id is not registered."""
        ids = self._by_device.get(device_id)
        if ids is None:
            log.warning("Unknown device_id: %s — not in ems.equipment", device_id)
        return ids

    def resolve_area(self, area_name: str) -> Optional[int]:
        """
        Resolve area name from payload to area_id.
        Handles: 'EXTRACTION', 'Extraction', 'extraction' → same result.
        """
        return self._by_area.get(area_name.lower())

    def resolve_line(self, line_str: str) -> Optional[int]:
        """
        Resolve line string from payload to line_id.
        Handles: 'LINE-1', 'Line-1', 'L1' variants.
        Normalisation: tries exact lowercase first, then 'line-X' pattern.
        """
        key = line_str.lower()
        if key in self._by_line:
            return self._by_line[key]
        # Normalise 'L1' → 'line-1'
        if key.startswith("l") and key[1:].isdigit():
            key = f"line-{key[1:]}"
            return self._by_line.get(key)
        return None

    def unknown_devices(self) -> list:
        """For diagnostics."""
        return list(self._by_device.keys())


# ── Singleton ─────────────────────────────────────────────────────────────────
# One instance per Flink job. Populated in main() before env.execute().
CACHE = MetadataCache()
