# main.py - OPTIMIZED VERSION
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import sys
from typing import NoReturn, Dict, Any, NamedTuple, List

from logging_config import setup_logging
from database import create_flight_repository, FlightRepository
from flight_processor import create_flight_processor, FlightProcessor
from database import DatabaseError

logger = logging.getLogger(__name__)


class ProcessingSummary(NamedTuple):
    total: int
    processed: int
    successes: int
    errors: int


def build_app(db_path: str | None = None) -> Dict[str, Any]:
    """Create and wire core application dependencies."""
    repo: FlightRepository = create_flight_repository(db_path=db_path)
    processor: FlightProcessor = create_flight_processor(repo)
    return {"repo": repo, "processor": processor}


def process_all_flights_optimized(
    repo: FlightRepository, 
    processor: FlightProcessor, 
    logger: logging.Logger | None = None,
    batch_size: int = 100000
) -> ProcessingSummary:

    lg = logger or logging.getLogger(__name__)

    df = repo.get_all_flights()
    if df is None or df.empty:
        #lg.info("No flight records found.")
        return ProcessingSummary(total=0, processed=0, successes=0, errors=0)

    total = len(df)
    processed = 0
    successes = 0
    errors = 0
    
    # Batch collection for inserts
    insert_batch: List[Dict[str, Any]] = []
    duplicate_batch: List[Dict[str, Any]] = []
    
#    lg.info("Starting optimized batch processing of %d records (batch_size=%d)", total, batch_size)

    # Process in chunks for better memory management
    for start_idx in range(0, total, batch_size):
        end_idx = min(start_idx + batch_size, total)
        chunk = df.iloc[start_idx:end_idx]
        
        for idx, row in chunk.iterrows():
            try:
                row_dict = row.to_dict()
                
                # Extract and process entries
                entries = processor.extract_entries_vectorized(row)
                
                if not entries:
                    processed += 1
                    successes += 1
                    continue
                
                groups = processor.group_by_time_window(entries)                
                # Build insert rows and detect duplicates in one optimized pass
                insert_rows, dup_dates_all, dup_flights_all = processor.build_insert_rows(row_dict, groups)                
                # Add duplicate row if needed
                if dup_dates_all and dup_flights_all:
                    dup_row = processor.build_duplicate_row(row_dict, dup_dates_all, dup_flights_all)
                    duplicate_batch.append(dup_row)
                
                # Add regular insert rows
                if insert_rows:
                    insert_batch.extend(insert_rows)
                
                processed += 1
                successes += 1
                
            except Exception as e:
                errors += 1
                lg.error("Error processing row %d: %s", idx, e)
        
        # Batch insert after each chunk (separate duplicates from regular inserts)
        total_inserts = len(duplicate_batch) + len(insert_batch)
        
        if total_inserts > 0:
            try:
#                lg.info("Batch inserting %d rows (%d duplicates, %d regular) - processed %d/%d", 
#                       total_inserts, len(duplicate_batch), len(insert_batch), processed, total)
                
                # Insert duplicates first
                if duplicate_batch:
                    repo.insert_flights_batch(duplicate_batch)
                    duplicate_batch.clear()
                
                # Then insert regular rows                                
                if insert_batch:                    
                    repo.insert_flights_batch(insert_batch)
                    insert_batch.clear()
                    
            except DatabaseError as e:
                lg.exception("Batch insert failed: %s", e)
                errors += total_inserts
                duplicate_batch.clear()
                insert_batch.clear()
    
    # Final batch insert for any remaining rows
    total_remaining = len(duplicate_batch) + len(insert_batch)
    if total_remaining > 0:
        try:
#            lg.info("Final batch insert of %d rows (%d duplicates, %d regular)", 
#                   total_remaining, len(duplicate_batch), len(insert_batch))
            
            if duplicate_batch:
                repo.insert_flights_batch(duplicate_batch)
            
            if insert_batch:
                repo.insert_flights_batch(insert_batch)
                
        except DatabaseError as e:
            lg.exception("Final batch insert failed: %s", e)
            errors += total_remaining

 #   lg.info("Processing finished: total=%d processed=%d successes=%d errors=%d", 
 #           total, processed, successes, errors)
    return ProcessingSummary(total=total, processed=processed, successes=successes, errors=errors)

def main() -> NoReturn:
    setup_logging(log_level="INFO")
    lg = logging.getLogger(__name__)
    app = build_app()
    repo = app["repo"]
    processor = app["processor"]

    try:
        # Use optimized version with configurable batch size
        summary = process_all_flights_optimized(repo, processor, logger=lg, batch_size=100000)
        
        if summary.total == 0:
            code = 0
        elif summary.errors > 0:
            code = 2
        else:
            code = 0
        
#        lg.info("Exiting with code %d", code)
        sys.exit(code)
    except KeyboardInterrupt:
        lg.warning("Interrupted by user")
        sys.exit(130)
    except DatabaseError as e:
        lg.exception("Database error: %s", e)
        sys.exit(1)
    except Exception as e:
        lg.exception("Fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()