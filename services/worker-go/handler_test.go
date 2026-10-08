package main

import (
	"context"
	"errors"
	"testing"
)

// image.resize é o único type deste worker e deve devolver um Result do worker "go".
func TestProcessImageResize(t *testing.T) {
	res, err := Process(context.Background(), Envelope{JobID: "j1", Type: "image.resize"})
	if err != nil || res.Worker != "go" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

// Type de outro worker não pode ser processado aqui; o consumer usa este erro
// para rejeitar a mensagem sem requeue.
func TestProcessUnsupportedType(t *testing.T) {
	_, err := Process(context.Background(), Envelope{JobID: "j1", Type: "email.send"})
	if !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("err = %v, want ErrUnsupportedType", err)
	}
}

// O workload vem do payload; sem o campo vale io-wait, e valor desconhecido é
// rejeitado como não suportado (repetir não adianta).
func TestProcessWorkloadSelection(t *testing.T) {
	cases := map[string]struct {
		payload string
		ok      bool
	}{
		"default":  {`{}`, true},
		"io-wait":  {`{"workload":"io-wait"}`, true},
		"io-block": {`{"workload":"io-block"}`, true},
		"cpu":      {`{"workload":"cpu"}`, true},
		"unknown":  {`{"workload":"gpu"}`, false},
	}
	for name, c := range cases {
		env := Envelope{JobID: "j1", Type: "image.resize", Payload: []byte(c.payload)}
		_, err := Process(context.Background(), env)
		if c.ok && err != nil {
			t.Errorf("%s: err = %v", name, err)
		}
		if !c.ok && !errors.Is(err, ErrUnsupportedType) {
			t.Errorf("%s: err = %v, want ErrUnsupportedType", name, err)
		}
	}
}
