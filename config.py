"""
Configuration settings for the DuckDB application.
"""
from pathlib import Path


class Config:
    """Application configuration settings."""

    # Database configuration
    DATABASE_DIR = Path.home() / "my_database"
    DATABASE_NAME = "my_db.duckdb"
    DATABASE_PATH = DATABASE_DIR / DATABASE_NAME

    # Table names
    SOURCE_TABLE = "my_db.main.TBO3_2022"    
    TARGET_TABLE = "my_db.main.TBO3_2022_TARGET"    

    # Column prefixes
    FLIGHT_NUMBER_PREFIX = "FlightNumber"
    DEPARTURE_DATE_PREFIX = "DepartureDateLocal"

    # Business logic constants
    HOURS_THRESHOLD = 24  # Hours threshold for grouping flights

    # Maximum number of flight entries per row
    MAX_FLIGHT_ENTRIES = 7

    @classmethod
    def ensure_database_dir_exists(cls) -> None:
        """Ensure the database directory exists."""
        cls.DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_database_path(cls) -> str:
        """Get the full database path as a string."""
        cls.ensure_database_dir_exists()
        return str(cls.DATABASE_PATH)

# Global configuration instance
config = Config()

