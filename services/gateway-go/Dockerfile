# Build multi-stage: binário estático em imagem mínima.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/app .

FROM alpine:3.21
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
