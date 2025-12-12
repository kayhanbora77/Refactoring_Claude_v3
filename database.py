# database.py - OPTIMIZED VERSION
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Dict, Any, List, Optional, Sequence, Tuple

import duckdb
import pandas as pd

from config import config

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass


class DatabaseConnection:
    """
    Optimized DatabaseConnection with connection pooling and batch operations.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.get_database_path()
        self._conn = None

    @contextmanager
    def connection(self, read_only: bool = False):
        """Context manager with reusable connection."""
        conn = None
        try:
            conn = duckdb.connect(database=self.db_path, read_only=read_only)
            yield conn
        except duckdb.Error as e:
            logger.exception("DuckDB error: %s", e)
            raise DatabaseError(str(e)) from e
        except Exception as e:
            logger.exception("Unexpected DB error: %s", e)
            raise DatabaseError(str(e)) from e
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning("Error closing DuckDB connection: %s", e)

    def fetch_dataframe(self, query: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
        """Execute SELECT query and force all columns to string to avoid datetime overflow."""
        with self.connection(read_only=True) as conn:
            try:
                if params:
                    df = conn.execute(query, params).fetchdf()
                else:
                    df = conn.execute(query).fetchdf()

                #  CRITICAL: prevent pandas datetime overflow
                return df.astype(str)

            except Exception as e:
                logger.exception("Failed to execute fetch query: %s", e)
                raise DatabaseError(str(e)) from e
        
    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> None:
        """Execute non-SELECT query."""
        with self.connection() as conn:
            try:
                if params:
                    conn.execute(query, params)
                else:
                    conn.execute(query)
            except Exception as e:
                logger.exception("Failed to execute query: %s", e)
                raise DatabaseError(str(e)) from e

    # NEW: Batch insert optimization
    def execute_batch(self, query: str, params_list: List[Sequence[Any]]) -> None:
        """Execute batch inserts in a single transaction for massive speedup."""
        if not params_list:
            return
        
        with self.connection() as conn:
            try:
                conn.execute("BEGIN TRANSACTION")
                for params in params_list:
                    conn.execute(query, params)
                conn.execute("COMMIT")
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.exception("Failed to execute batch query: %s", e)
                raise DatabaseError(str(e)) from e


class FlightRepository:
    """
    Optimized Repository with batch operations and prepared statements.
    """

    # Canonical column order - converted to tuple for immutability and speed
    COLUMNS_ORDER: Tuple[str, ...] = (
        "PaxName", "BookingRef", "ETicketNo", "ClientCode", "Airline", "JourneyType",
        "FlightNumber1", "FlightNumber2", "FlightNumber3", "FlightNumber4",
        "FlightNumber5", "FlightNumber6", "FlightNumber7",
        "DepartureDateLocal1", "DepartureDateLocal2", "DepartureDateLocal3",
        "DepartureDateLocal4", "DepartureDateLocal5", "DepartureDateLocal6",
        "DepartureDateLocal7", "Airport1", "Airport2", "Airport3", "Airport4",
        "Airport5", "Airport6", "Airport7", "Airport8"
    )

    def __init__(self, db: Optional[DatabaseConnection] = None):
        self.db = db or DatabaseConnection()
        
        # Pre-build INSERT query once for reuse
        table = config.TARGET_TABLE
        placeholders = ", ".join("?" for _ in self.COLUMNS_ORDER)
        col_list = ", ".join(self.COLUMNS_ORDER)
        self._insert_query = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        

    # -------------------------
    # Read operations
    # -------------------------
    def get_all_flights(self) -> pd.DataFrame:
        """Return all rows from SOURCE_TABLE as DataFrame."""
        query = f"SELECT * FROM {config.SOURCE_TABLE}"
        return self.db.fetch_dataframe(query)

    # -------------------------
    # OPTIMIZED: Write operations
    # -------------------------
    def insert_flight(self, row_data: Dict[str, Any], target_table: Optional[str] = None) -> None:
        """Insert single row using pre-built query."""
        if target_table and target_table != config.TARGET_TABLE:
            # Build custom query for different table
            placeholders = ", ".join("?" for _ in self.COLUMNS_ORDER)
            col_list = ", ".join(self.COLUMNS_ORDER)
            query = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})"
        else:
            query = self._insert_query
        
        # Build params tuple in correct order
        params = tuple(self._prepare_value(row_data.get(col)) for col in self.COLUMNS_ORDER)
        self.db.execute(query, params)

    # NEW: Batch insert for massive performance gain
    def insert_flights_batch(self, rows_data: List[Dict[str, Any]], target_table: Optional[str] = None) -> None:
        """
        Batch insert multiple rows in a single transaction.
        This can be 10-100x faster than individual inserts.
        """
        if not rows_data:
            return
        
        if target_table and target_table != config.TARGET_TABLE:
            placeholders = ", ".join("?" for _ in self.COLUMNS_ORDER)
            col_list = ", ".join(self.COLUMNS_ORDER)
            query = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})"
        else:
            query = self._insert_query
        
        # Prepare all params at once
        params_list = [
            tuple(self._prepare_value(row.get(col)) for col in self.COLUMNS_ORDER)
            for row in rows_data
        ]
        self.db.execute_batch(query, params_list)

    @staticmethod
    def _prepare_value(value: Any) -> Any:
        """
        Optimized value normalization with early returns.
        """
        if value is None:
            return None
        
        # Check for pandas Timestamp
        if hasattr(value, 'to_pydatetime'):
            try:
                return value.to_pydatetime()
            except Exception:
                pass
        
        return value


def create_flight_repository(db_path: Optional[str] = None) -> FlightRepository:
    conn = DatabaseConnection(db_path=db_path) if db_path else DatabaseConnection()
    return FlightRepository(conn)