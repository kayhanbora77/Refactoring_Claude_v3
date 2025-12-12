# models.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

@dataclass
class FlightRow:
    pax_name: str
    booking_ref: str
    client_code: Optional[str] = None
    airline: Optional[str] = None
    journey_type: Optional[str] = None
    e_ticket_no: Optional[str] = None
    airports: List[Optional[str]] = field(default_factory=list)

    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]) -> "FlightRow":
        airports = []
        for i in range(1, 9):
            airports.append(row.get(f"Airport{i}"))
        return cls(
            pax_name=row.get("PaxName", ""),
            booking_ref=row.get("BookingRef", ""),
            client_code=row.get("ClientCode"),
            airline=row.get("Airline"),
            journey_type=row.get("JourneyType"),
            e_ticket_no=row.get("ETicketNo"),
            airports=airports
        )


@dataclass
class ProcessingResult:
    """
    Result object returned by FlightProcessor.process_flight_row.

    - groups: list of groups, where each group is a list of datetime objects (departure dates)
    - duplicates_dates: all duplicated dates found across groups
    - duplicates_flights: all duplicated flight numbers found across groups
    """
    original_row: FlightRow
    groups: List[List[datetime]] = field(default_factory=list)
    success: bool = True
    message: Optional[str] = None
    duplicates_dates: List[datetime] = field(default_factory=list)
    duplicates_flights: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not isinstance(self.groups, list):
            self.groups = []
        if self.duplicates_dates is None:
            self.duplicates_dates = []
        if self.duplicates_flights is None:
            self.duplicates_flights = []

