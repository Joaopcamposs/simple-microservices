package main

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"fmt"
	"time"
)

// Cargas de trabalho que o payload pode pedir ("workload"). Existem para a
// comparação entre workers (make bench): o mesmo job, três perfis de execução.
const (
	workloadIOWait  = "io-wait"
	workloadIOBlock = "io-block"
	workloadCPU     = "cpu"
)

// ioDelay é a duração simulada de I/O; cpuIterations calibra o PBKDF2 para cerca
// de 100 ms. Os valores são iguais nos quatro workers para a comparação valer.
const (
	ioDelay       = 200 * time.Millisecond
	cpuIterations = 600_000
)

// RunWorkload executa a carga pedida. Em Go, esperar (io-wait e io-block) só
// estaciona a goroutine, então as duas cargas de I/O custam o mesmo; o cpu usa
// os núcleos de verdade. Respeita o cancelamento do contexto nas esperas.
func RunWorkload(ctx context.Context, workload string) error {
	switch workload {
	case workloadIOWait, workloadIOBlock:
		select {
		case <-time.After(ioDelay):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case workloadCPU:
		_, err := pbkdf2.Key(sha256.New, "bench", []byte("salt"), cpuIterations, 32)
		return err
	default:
		return fmt.Errorf("%w: workload %q", ErrUnsupportedType, workload)
	}
}
