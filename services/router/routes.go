// Package main é o router: um webhook que decide para qual worker cada job vai.
//
// Fluxo: o relay envia o envelope em POST /dispatch; o router lê o campo "type",
// consulta a tabela abaixo e publica a mensagem no RabbitMQ com o worker como
// routing key. Este arquivo guarda a tabela, o único lugar do sistema que conhece
// os workers. Gateways e relay só conhecem o "type".
package main

// Worker é o consumidor de destino. Vira a routing key no exchange "jobs",
// que entrega a mensagem na fila "jobs.<worker>" (ver infra/rabbitmq/definitions.json).
type Worker string

// Routes mapeia o type de um job para o worker que o processa.
type Routes map[string]Worker

// DefaultRoutes devolve a tabela fixa da demo. Para um worker ou tipo novo:
// adicione a linha aqui e a fila/binding em definitions.json.
func DefaultRoutes() Routes {
	return Routes{
		"report.generate": "celery",
		"email.send":      "taskiq",
		"http.fetch":      "asyncio",
		"image.resize":    "go",
	}
}

// Lookup devolve o worker de um type; false se o type é desconhecido.
func (r Routes) Lookup(jobType string) (Worker, bool) {
	w, ok := r[jobType]
	return w, ok
}
