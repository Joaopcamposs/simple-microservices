"""Formato de log do projeto: JSON, uma linha por evento, igual ao dos serviços Go.

Campos fixos: time, level, msg, service. Quem loga com `extra={"job_id": ...}`
ganha o campo job_id, que permite seguir um job entre os serviços.
"""

import json
import logging
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    """Formata cada registro como uma linha JSON."""

    def __init__(self, service: str) -> None:
        """Guarda o nome do serviço, repetido em todas as linhas."""
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        """Monta o JSON do registro; inclui job_id e erro quando existirem."""
        line: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
            "service": self._service,
        }
        job_id = getattr(record, "job_id", None)
        if job_id is not None:
            line["job_id"] = str(job_id)
        if record.exc_info:
            line["error"] = self.formatException(record.exc_info)
        return json.dumps(line)
