"""Teste e2e da stack inteira (`make e2e`): exige `make up` e usa só a biblioteca padrão.

Cada cenário fala com os gateways por HTTP, com o RabbitMQ pela API de management
e com o Postgres/containers via `docker compose`. Zera as tabelas no início e no fim.
"""

import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GATEWAYS = {"gateway-py": "http://localhost:58001", "gateway-go": "http://localhost:58002"}
MANAGEMENT = "http://localhost:55673/api"
# Tipo de job -> worker que deve processá-lo (espelha a tabela do router).
ROUTES = {
    "report.generate": "celery",
    "email.send": "taskiq",
    "http.fetch": "asyncio",
    "image.resize": "go",
}
TIMEOUT_S = 40.0


@dataclass(frozen=True, slots=True)
class JobView:
    """Resposta de `GET /jobs/{id}` reduzida ao que os cenários conferem."""

    status: str
    workers: list[str]


class Stack:
    """Acesso à stack em execução: gateways, RabbitMQ (management), Postgres e docker compose."""

    def post_job(self, gateway: str, job_type: str) -> str:
        """Cria um job pelo gateway e devolve o `job_id` (exige 202)."""
        request = urllib.request.Request(
            f"{GATEWAYS[gateway]}/jobs",
            json.dumps({"type": job_type, "payload": {"w": 1}}).encode(),
            {"content-type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != 202:
                raise AssertionError(f"POST /jobs -> {response.status}")
            return str(json.load(response)["job_id"])

    def get_job(self, job_id: str) -> JobView:
        """Lê o job pelo gateway-py (o estado vem do mesmo banco nos dois)."""
        with urllib.request.urlopen(
            f"{GATEWAYS['gateway-py']}/jobs/{job_id}", timeout=10
        ) as response:
            body = json.load(response)
        return JobView(status=body["status"], workers=[r["worker"] for r in body["results"]])

    def wait_status(self, job_id: str, want: str) -> JobView:
        """Espera o job chegar ao status; falha com o último estado visto se estourar o prazo."""
        deadline = time.monotonic() + TIMEOUT_S
        view = self.get_job(job_id)
        while view.status != want and time.monotonic() < deadline:
            time.sleep(0.5)
            view = self.get_job(job_id)
        if view.status != want:
            raise AssertionError(f"job {job_id}: status {view.status!r}, esperado {want!r}")
        return view

    def psql(self, sql: str) -> str:
        """Executa SQL no Postgres do compose e devolve a saída sem formatação."""
        return self.compose(
            "exec", "-T", "postgres", "psql", "-U", "app", "-d", "app", "-qAtc", sql
        )

    def compose(self, *args: str) -> str:
        """Roda `docker compose` na raiz do repositório e devolve o stdout."""
        done = subprocess.run(
            ["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True, check=True
        )
        return done.stdout.strip()

    def management(self, method: str, path: str, body: object | None = None) -> object:
        """Chama a API de management do RabbitMQ (guest/guest) e devolve o JSON, se houver."""
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"{MANAGEMENT}{path}", data, method=method)
        request.add_header("content-type", "application/json")
        token = base64.b64encode(b"guest:guest").decode()
        request.add_header("authorization", f"Basic {token}")
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
        return json.loads(raw) if raw else None

    def publish(self, routing_key: str, payload: str) -> None:
        """Publica um corpo cru na exchange `jobs`, como o router faria (forja mensagens)."""
        body = {
            "properties": {},
            "routing_key": routing_key,
            "payload": payload,
            "payload_encoding": "string",
        }
        self.management("POST", "/exchanges/%2F/jobs/publish", body)

    def reset(self) -> None:
        """Zera jobs, outbox e resultados, e esvazia a DLQ."""
        self.psql("TRUNCATE job_results, outbox, jobs CASCADE")
        self.management("DELETE", "/queues/%2F/jobs.dlq/contents")


class Scenarios:
    """Cenários e2e; cada funcao levanta AssertionError se o comportamento esperado não ocorrer."""

    def __init__(self, stack: Stack) -> None:
        """Recebe a stack sobre a qual os cenários rodam."""
        self.stack = stack

    def all_workers_done(self) -> None:
        """Os 4 tipos, nos 2 gateways, chegam a DONE no worker certo."""
        jobs = [(g, t, self.stack.post_job(g, t)) for g in GATEWAYS for t in ROUTES]
        for gateway, job_type, job_id in jobs:
            view = self.stack.wait_status(job_id, "DONE")
            assert view.workers == [ROUTES[job_type]], (
                f"{gateway} {job_type}: workers {view.workers}"
            )

    def redelivery_is_idempotent(self) -> None:
        """Reentregar o mesmo envelope não duplica o resultado nem tira o job de DONE."""
        job_id = self.stack.post_job("gateway-go", "image.resize")
        self.stack.wait_status(job_id, "DONE")
        envelope = self.stack.psql(f"SELECT envelope FROM outbox WHERE job_id = '{job_id}'")
        self.stack.publish("go", envelope)
        time.sleep(3)
        count = self.stack.psql(f"SELECT count(*) FROM job_results WHERE job_id = '{job_id}'")
        assert count == "1", f"{count} resultados para o mesmo job"
        assert self.stack.get_job(job_id).status == "DONE"

    def unknown_type_fails(self) -> None:
        """Type sem rota é rejeitado pelo router (422) e o job fica FAILED."""
        job_id = self.stack.post_job("gateway-py", "nope")
        self.stack.wait_status(job_id, "FAILED")

    def router_down_recovers(self) -> None:
        """Com o router parado o POST segue 202 e o job fica PENDING; ao voltar, chega a DONE."""
        self.stack.compose("stop", "router")
        try:
            job_ids = [self.stack.post_job(g, "email.send") for g in GATEWAYS]
            time.sleep(3)
            assert all(self.stack.get_job(j).status == "PENDING" for j in job_ids)
        finally:
            self.stack.compose("start", "router")
        for job_id in job_ids:
            self.stack.wait_status(job_id, "DONE")

    def unroutable_waits_for_binding(self) -> None:
        """Fila sem binding (mandatory): job fica PENDING e conclui quando o binding volta."""
        binding = "/bindings/%2F/e/jobs/q/jobs.go"
        self.stack.management("DELETE", f"{binding}/go")
        try:
            job_id = self.stack.post_job("gateway-py", "image.resize")
            time.sleep(4)
            assert self.stack.get_job(job_id).status == "PENDING"
        finally:
            self.stack.management("POST", binding, {"routing_key": "go"})
        self.stack.wait_status(job_id, "DONE")

    def dead_message_fails_job(self) -> None:
        """Mensagem rejeitada vai à DLQ e o dlq-reaper marca o job FAILED; lixo é descartado."""
        job_id = "01a11992-aaaa-7b71-b568-f9d35cfc85e6"
        self.stack.psql(
            f"INSERT INTO jobs (id, type, payload, status, origin) "
            f"VALUES ('{job_id}', 'nope', '{{}}', 'DISPATCHED', 'e2e')"
        )
        self.stack.publish("go", json.dumps({"job_id": job_id, "type": "nope"}))
        self.stack.publish("go", "nao-e-json")
        self.stack.wait_status(job_id, "FAILED")
        time.sleep(2)
        queues = self.stack.management("GET", "/queues/%2F/jobs.dlq")
        assert isinstance(queues, dict) and queues["messages"] == 0, "DLQ não esvaziou"

    def transient_failure_recovers_by_retry(self) -> None:
        """Save falha (tabela ausente); com a tabela de volta, o retry conclui o job."""
        for worker, job_type in (("go", "image.resize"), ("asyncio", "http.fetch")):
            job_id = f"01a11992-cccc-7b71-b568-{'0' if worker == 'go' else '1'}5fa74aae1d4"
            self.stack.psql(
                f"INSERT INTO jobs (id, type, payload, status, origin) "
                f"VALUES ('{job_id}', '{job_type}', '{{}}', 'DISPATCHED', 'e2e')"
            )
            envelope = {
                "job_id": job_id,
                "type": job_type,
                "payload": {},
                "created_at": "2026-10-08T00:00:00Z",
                "origin": "e2e",
            }
            self.stack.psql("ALTER TABLE job_results RENAME TO job_results_off")
            try:
                self.stack.publish(worker, json.dumps(envelope))
                time.sleep(2)
                status = self.stack.psql(f"SELECT status FROM jobs WHERE id = '{job_id}'")
                assert status.strip() == "DISPATCHED", status
            finally:
                self.stack.psql("ALTER TABLE job_results_off RENAME TO job_results")
            self.stack.wait_status(job_id, "DONE")


def run(scenarios: list[Callable[[], None]], stack: Stack) -> int:
    """Roda cada cenário com estado limpo e devolve o número de falhas."""
    failures = 0
    for scenario in scenarios:
        stack.reset()
        started = time.monotonic()
        try:
            scenario()
        except (AssertionError, urllib.error.URLError, subprocess.CalledProcessError) as exc:
            failures += 1
            print(f"FAIL {scenario.__name__}: {exc}")
        else:
            print(f"ok   {scenario.__name__} ({time.monotonic() - started:.1f}s)")
    stack.reset()
    return failures


def main() -> int:
    """Executa todos os cenários; código de saída 1 se algum falhar."""
    stack = Stack()
    s = Scenarios(stack)
    failures = run(
        [
            s.all_workers_done,
            s.redelivery_is_idempotent,
            s.unknown_type_fails,
            s.router_down_recovers,
            s.unroutable_waits_for_binding,
            s.dead_message_fails_job,
            s.transient_failure_recovers_by_retry,
        ],
        stack,
    )
    print(f"{failures} falha(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
