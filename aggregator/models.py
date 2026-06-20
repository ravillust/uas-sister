"""
models.py
Skema/validasi event menggunakan Pydantic.

Event JSON minimal (sesuai spesifikasi soal):
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": { ... }
}
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: datetime
    source: str = Field(..., min_length=1, max_length=255)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("topic", "event_id", "source")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("tidak boleh kosong/whitespace")
        return v

    def to_db_dict(self) -> Dict[str, Any]:
        """Bentuk dict siap-insert untuk lapisan db (payload diserialisasi ke JSON)."""
        return {
            "topic": self.topic,
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "source": self.source,
            "payload_json": json.dumps(self.payload),
        }


class PublishRequest(BaseModel):
    """
    Menerima single event atau batch.
    Bisa berupa satu objek Event, atau list of Event.
    Endpoint /publish menormalkan keduanya.
    """

    events: List[Event]

    @classmethod
    def from_raw(cls, raw: Union[Dict[str, Any], List[Any]]) -> "PublishRequest":
        if isinstance(raw, list):
            return cls(events=[Event(**e) for e in raw])
        # objek tunggal
        return cls(events=[Event(**raw)])
