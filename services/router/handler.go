package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

// maxBodyBytes limita o envelope aceito (proteção simples contra abuso).
const maxBodyBytes = 1 << 20

// Publisher entrega uma mensagem ao broker para o worker indicado.
// É uma interface para o handler não depender do RabbitMQ: nos testes entra um fake.
type Publisher interface {
	Publish(ctx context.Context, worker Worker, body []byte) error
}

// DispatchHandler implementa POST /dispatch: valida, roteia e publica.
//
// Os códigos de resposta são o contrato com o relay, que decide o destino da
// linha da outbox pelo status:
//
//	202  publicado e confirmado pelo broker      -> relay marca "sent"
//	400  corpo ilegível ou sem type              -> erro permanente, relay marca "failed"
//	422  type sem worker na tabela               -> erro permanente, relay marca "failed"
//	502  broker indisponível                     -> erro temporário, relay tenta de novo
type DispatchHandler struct {
	routes    Routes
	publisher Publisher
}

// NewDispatchHandler cria o handler com a tabela e o publisher injetados.
func NewDispatchHandler(routes Routes, publisher Publisher) *DispatchHandler {
	return &DispatchHandler{routes: routes, publisher: publisher}
}

// routeKey são os únicos campos do envelope que o router lê. O corpo original
// é republicado intacto, então campos novos do contrato passam sem mudar o router.
type routeKey struct {
	JobID string `json:"job_id"`
	Type  string `json:"type"`
}

// ServeHTTP lê o envelope, escolhe o worker pelo type e publica o corpo original.
func (h *DispatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "unreadable body")
		return
	}
	var key routeKey
	if err := json.Unmarshal(body, &key); err != nil || key.Type == "" {
		writeError(w, http.StatusBadRequest, "invalid envelope")
		return
	}
	worker, ok := h.routes.Lookup(key.Type)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "unknown job type: "+key.Type)
		return
	}
	if err := h.publisher.Publish(r.Context(), worker, body); err != nil {
		log.Printf("publish job %s to %s: %v", key.JobID, worker, err)
		writeError(w, http.StatusBadGateway, "broker unavailable")
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// writeError responde no formato {"detail": "..."} usado por toda a demo.
func writeError(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"detail": detail}); err != nil {
		log.Printf("write error response: %v", err)
	}
}
