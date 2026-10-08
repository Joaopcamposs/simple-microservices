package main

import "image"

// Tamanhos do job image.resize: uma imagem de origem sintética (no lugar de uma
// baixada do storage) reduzida a um thumbnail.
const (
	sourceSize = 2048
	targetSize = 256
)

// NewGradient cria uma imagem quadrada size×size com um gradiente determinístico,
// no lugar da imagem real que o job receberia.
func NewGradient(size int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, size, size))
	for y := range size {
		for x := range size {
			i := img.PixOffset(x, y)
			img.Pix[i] = uint8(x * 255 / size)
			img.Pix[i+1] = uint8(y * 255 / size)
			img.Pix[i+2] = uint8((x + y) * 255 / (2 * size))
			img.Pix[i+3] = 255
		}
	}
	return img
}

// Resize reduz a imagem quadrada src para size×size pela média de cada bloco
// (filtro de caixa). Exige que o lado de src seja múltiplo de size. CPU pura e
// sem estado compartilhado: goroutines distintas usam núcleos distintos.
func Resize(src *image.RGBA, size int) *image.RGBA {
	factor := src.Bounds().Dx() / size
	dst := image.NewRGBA(image.Rect(0, 0, size, size))
	for y := range size {
		for x := range size {
			var sum [4]int
			for dy := range factor {
				row := src.PixOffset(x*factor, y*factor+dy)
				for dx := range factor {
					for c := range 4 {
						sum[c] += int(src.Pix[row+dx*4+c])
					}
				}
			}
			out := dst.PixOffset(x, y)
			for c := range 4 {
				dst.Pix[out+c] = uint8(sum[c] / (factor * factor))
			}
		}
	}
	return dst
}
