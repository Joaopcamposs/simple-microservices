package main

import (
	"context"
	"errors"
	"image"
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

// A redução é a média de cada bloco: metade esquerda 0 e direita 100 viram um
// thumbnail com os mesmos lados (0 | 100); um bloco lido errado misturaria os valores.
func TestResizeAveragesBlocks(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 4, 4))
	for y := range 4 {
		for x := 2; x < 4; x++ {
			src.Pix[src.PixOffset(x, y)] = 100
		}
	}
	dst := Resize(src, 2)
	for y := range 2 {
		if left, right := dst.Pix[dst.PixOffset(0, y)], dst.Pix[dst.PixOffset(1, y)]; left != 0 || right != 100 {
			t.Fatalf("row %d = %d|%d, want 0|100", y, left, right)
		}
	}
}
