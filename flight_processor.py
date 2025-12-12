# flight_processor.py - OPTIMIZED VERSION
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

import pandas as pd

from config import config
from database import FlightRepository

logger = logging.getLogger(__name__)

# Constants moved to module level for one-time computation
FLIGHT_KEYS = tuple(f"{config.FLIGHT_NUMBER_PREFIX}{i}" for i in range(1, config.MAX_FLIGHT_ENTRIES + 1))
DATE_KEYS = tuple(f"{config.DEPARTURE_DATE_PREFIX}{i}" for i in range(1, config.MAX_FLIGHT_ENTRIES + 1))
HOURS_THRESHOLD = config.HOURS_THRESHOLD
SECONDS_TO_HOURS = 0.000277778  # 1/3600


@dataclass
class FlightEntry:
    date: datetime
    flight_number: str


@dataclass
class FlightGroup:
    entries: List[FlightEntry]


class FlightProcessorError(Exception):
    pass


class FlightProcessor:
    """
    Optimized FlightProcessor with batch processing and vectorized operations.
    """

    def __init__(self, repo: Optional[FlightRepository] = None):
        if repo is None:
            repo = FlightRepository()
        self.repo = repo
    
    @staticmethod
    def extract_entries_vectorized(row: pd.Series) -> List[FlightEntry]:
        """Optimized entry extraction using vectorized operations."""
        entries: List[FlightEntry] = []
        
        # Batch get all flight numbers and dates at once
        flight_nums = [row.get(key) for key in FLIGHT_KEYS]
        date_strs = [row.get(key) for key in DATE_KEYS]
        
        for flight_num, date_str in zip(flight_nums, date_strs):
            # Skip invalid entries early
            if not flight_num or flight_num == "NULL":
                continue
            if not date_str or date_str == "NULL":
                continue

             # Convert safely — avoid overflow errors
            dt = pd.to_datetime(date_str, errors="coerce")
            if pd.isna(dt):
                continue  # skip bad / overflow dates

            entries.append(FlightEntry(date=dt, flight_number=str(flight_num)))
        
        return entries
    
    @classmethod
    def group_by_time_window(cls, entries: List[FlightEntry]) -> List[FlightGroup]:
        #print("ENTRIES==>", entries)
        """
        Group flights by time window with day-aware logic:
        - If day difference is 0 (same day): group if within HOURS_THRESHOLD (24h)
        - If day difference is 1 (next day): group if within 36 hours
        - If day difference > 1: create new group
        """
        if not entries:
            return []

        # Sort once (optimized with key function stored)
        entries.sort(key=lambda e: e.date)
        
        groups: List[FlightGroup] = []
        current: List[FlightEntry] = [entries[0]]
        
        prev_date = entries[0].date
        
        for entry in entries[1:]:
            curr_date = entry.date
            
            # Calculate day difference (using date only, not time)
            day_diff = (curr_date.date() - prev_date.date()).days
            
            # Calculate time difference in hours
            diff_hours = (curr_date - prev_date).total_seconds() * SECONDS_TO_HOURS
            
            # Grouping logic based on day difference
            should_group = False
            
            if day_diff == 0:
                # Same day: use standard threshold (24 hours)
                should_group = diff_hours <= HOURS_THRESHOLD
            elif day_diff == 1:
                # Next day: allow up to 36 hours
                should_group = diff_hours <= 36
            # else: day_diff > 1, should_group remains False
            
            if should_group:
                current.append(entry)
            else:
                groups.append(FlightGroup(entries=current))
                current = [entry]
            
            prev_date = curr_date
        
        groups.append(FlightGroup(entries=current))            
        return groups
    
    @staticmethod
    def detect_duplicates(group: FlightGroup) -> Tuple[List[datetime], List[str]]:
        """Optimized duplicate detection with single pass."""
        if len(group.entries) <= 1:
            return [], []
        for grp in group.entries:
            if grp.flight_number.endswith("000"):
                return [], []
        # Use Counter for efficient counting
        date_counts = Counter(e.date for e in group.entries)
        flight_counts = Counter(e.flight_number for e in group.entries)
        
        # List comprehension is faster than loop append
        duplicate_dates = [d for d, c in date_counts.items() if c > 1]
        duplicate_flights = [f for f, c in flight_counts.items() if c > 1]
        
        return duplicate_dates, duplicate_flights

    def build_insert_rows(self, original_row: Dict[str, Any], groups: List[FlightGroup]) -> Tuple[List[Dict[str, Any]], List[datetime], List[str]]:
        """
        Optimized row building that detects duplicates in a single pass
        and filters them out while building insert rows.
        
        Returns: (insert_list, duplicate_dates, duplicate_flights)
        """
        insert_list: List[Dict[str, Any]] = []
        dup_dates_all: List[datetime] = []
        dup_flights_all: List[str] = []
        
        for group in groups:
            # Detect duplicates for this group
            d_dates, d_flights = self.detect_duplicates(group)
            
            if d_dates:
                dup_dates_all.extend(d_dates)
            if d_flights:
                dup_flights_all.extend(d_flights)
            
            # Convert to sets for O(1) lookup instead of O(n) list search
            dup_dates_set = set(d_dates) if d_dates else set()
            dup_flights_set = set(d_flights) if d_flights else set()
            
            # Shallow copy is faster for large dicts
            new_row = original_row.copy()
            
            # Clear flight/date fields in batch
            for f_key, d_key in zip(FLIGHT_KEYS, DATE_KEYS):
                new_row[f_key] = None
                new_row[d_key] = None
            
            # Fill with group entries, excluding duplicates
            insert_idx = 0
            for entry in group.entries:
                # Skip duplicates using set lookup (O(1) vs O(n))
                if entry.flight_number not in dup_flights_set and entry.date not in dup_dates_set:
                    new_row[FLIGHT_KEYS[insert_idx]] = entry.flight_number
                    new_row[DATE_KEYS[insert_idx]] = entry.date
                    insert_idx += 1
            if new_row.get(FLIGHT_KEYS[0]) is not None and new_row.get(FLIGHT_KEYS[0]) != "NULL":
                insert_list.append(new_row)
            #insert_list.append(new_row)        
        return insert_list, dup_dates_all, dup_flights_all

    @staticmethod
    def build_duplicate_row(original_row: Dict[str, Any], dup_dates: List[datetime], dup_flights: List[str]) -> Dict[str, Any]:
        new_row = original_row.copy()
        
        # Clear in batch
        for f_key, d_key in zip(FLIGHT_KEYS, DATE_KEYS):
            new_row[f_key] = None
            new_row[d_key] = None
        
        # Fill duplicates
        for i, (d, f) in enumerate(zip(dup_dates, dup_flights)):
            if i >= len(FLIGHT_KEYS):
                break
            new_row[FLIGHT_KEYS[i]] = f
            new_row[DATE_KEYS[i]] = d
        
        return new_row
    
def create_flight_processor(repo: Optional[FlightRepository] = None) -> FlightProcessor:
    return FlightProcessor(repo)