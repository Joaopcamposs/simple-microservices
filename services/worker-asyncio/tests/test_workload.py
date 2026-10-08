"""Testes da escolha do workload pelo payload: o que o worker aceita e o que rejeita."""

import pytest
from app.workload import workload_of


def test_missing_workload_defaults_to_io_wait() -> None:
    """Payload sem o campo continua valendo (jobs antigos e do gateway)."""
    assert workload_of({"w": 1}) == "io-wait"


@pytest.mark.parametrize("value", ["gpu", "", 5, ["cpu"], None])
def test_invalid_workload_is_rejected(value: object) -> None:
    """Nome fora da lista ou tipo errado levanta ValueError (vira reject, sem retry)."""
    with pytest.raises(ValueError):
        workload_of({"workload": value})
