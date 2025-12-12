# flight_processor.py - OPTIMIZED VERSION
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

import pandas as pd
import numpy as np

from config import config
from models import FlightRow, ProcessingResult
from database import FlightRepository, DatabaseError

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

    # ------------------ OPTIMIZED: Vectorized Extraction ------------------
    @staticmethod
    def extract_entries_vectorized(row: pd.Series) -> List[FlightEntry]:
        """Optimized entry extraction using vectorized operations."""
        entries: List[FlightEntry] = []
        
        # Batch get all flight numbers and dates at once
        flight_nums = [row.get(key) for key in FLIGHT_KEYS]
        date_strs = [row.get(key) for key in DATE_KEYS]
        
        for flight_num, date_str in zip(flight_nums, date_strs):
            # Skip invalid entries early
            if not flight_num or flight_num == "NULL" or str(flight_num).endswith("000"):
                continue
            if not date_str or date_str == "NULL":
                continue
            
            # Fast datetime conversion
            try:
                dt = pd.to_datetime(date_str)
                entries.append(FlightEntry(date=dt, flight_number=str(flight_num)))
            except Exception:
                continue
        
        return entries

    # ------------------ OPTIMIZED: Grouping with pre-allocated lists ------------------
    @classmethod
    def group_by_time_window(cls, entries: List[FlightEntry]) -> List[FlightGroup]:
        if not entries:
            return []

        # Sort once (optimized with key function stored)
        entries.sort(key=lambda e: e.date)
        
        groups: List[FlightGroup] = []
        current: List[FlightEntry] = [entries[0]]
        
        prev_date = entries[0].date
        
        for entry in entries[1:]:
            # Calculate time difference in hours directly
            diff_hours = (entry.date - prev_date).total_seconds() * SECONDS_TO_HOURS
            
            if diff_hours <= HOURS_THRESHOLD:
                current.append(entry)
            else:
                groups.append(FlightGroup(entries=current))
                current = [entry]
            
            prev_date = entry.date
        
        groups.append(FlightGroup(entries=current))
        return groups

    # ------------------ OPTIMIZED: Single-pass duplicate detection ------------------
    @staticmethod
    def detect_duplicates(group: FlightGroup) -> Tuple[List[datetime], List[str]]:
        """Optimized duplicate detection with single pass."""
        if len(group.entries) <= 1:
            return [], []
        
        # Use Counter for efficient counting
        date_counts = Counter(e.date for e in group.entries)
        flight_counts = Counter(e.flight_number for e in group.entries)
        
        # List comprehension is faster than loop append
        duplicate_dates = [d for d, c in date_counts.items() if c > 1]
        duplicate_flights = [f for f, c in flight_counts.items() if c > 1]
        
        return duplicate_dates, duplicate_flights

    # ------------------ OPTIMIZED: Build rows with duplicate filtering ------------------
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
            
            insert_list.append(new_row)
        
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

    # ------------------ OPTIMIZED: Master processing with single duplicate pass ------------------
    def process_flight_row(self, row_data: Dict[str, Any]) -> ProcessingResult:
        try:
            # Use optimized extraction
            entries = self.extract_entries_vectorized(pd.Series(row_data))
            
            if not entries:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=[],
                    success=True,
                    message="No flight data found"
                )

            groups = self.group_by_time_window(entries)

            # Build insert rows and detect duplicates in one pass
            insert_rows, dup_dates_all, dup_flights_all = self.build_insert_rows(row_data, groups)

            # Handle duplicates if any exist
            if dup_dates_all and dup_flights_all:
                dup_row = self.build_duplicate_row(row_data, dup_dates_all, dup_flights_all)
                try:
                    self.repo.insert_flight(dup_row)
                except DatabaseError as e:
                    logger.exception("Failed to insert duplicate row: %s", e)
                    return ProcessingResult(
                        original_row=FlightRow.from_dataframe_row(row_data),
                        groups=[[e.date for e in g.entries] for g in groups],
                        success=False,
                        message=f"Failed to insert duplicate row: {e}"
                    )

            # Insert the cleaned rows
            if insert_rows:
                for r in insert_rows:
                    try:
                        self.repo.insert_flight(r)
                    except DatabaseError as e:
                        logger.exception("Failed to insert row: %s", e)
                        return ProcessingResult(
                            original_row=FlightRow.from_dataframe_row(row_data),
                            groups=[[e.date for e in g.entries] for g in groups],
                            success=False,
                            message=f"Failed to insert row: {e}"
                        )
                
                msg = "Single group inserted" if len(groups) == 1 else "Row processed and database updated"
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=[[e.date for e in g.entries] for g in groups],
                    success=True,
                    message=msg
                )

            return ProcessingResult(
                original_row=FlightRow.from_dataframe_row(row_data),
                groups=[[e.date for e in g.entries] for g in groups],
                success=True,
                message="No database changes needed"
            )

        except Exception as e:
            logger.exception("Processing failed: %s", e)
            return ProcessingResult(
                original_row=FlightRow.from_dataframe_row(row_data),
                groups=[],
                success=False,
                message=f"Processing failed: {e}"
            )


def create_flight_processor(repo: Optional[FlightRepository] = None) -> FlightProcessor:
    return FlightProcessor(repo)