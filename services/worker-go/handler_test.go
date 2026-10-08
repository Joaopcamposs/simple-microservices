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
