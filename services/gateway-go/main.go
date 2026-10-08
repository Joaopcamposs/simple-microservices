package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "gateway-go/docs" // OpenAPI gerado por `make swagger`
)

// @title       Gateway Go
// @version     1.0
// @description Recebe jobs, grava job + outbox e responde 202.
// @BasePath    /

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// RegisterDocs expõe o Swagger UI em /docs/index.html; /docs e /docs/ redirecionam para ele.
func RegisterDocs(router *gin.Engine) {
	redirect := func(c *gin.Context) { c.Redirect(http.StatusFound, "/docs/index.html") }
	ui := ginSwagger.WrapHandler(swaggerFiles.Handler)
	router.GET("/docs", redirect)
	router.GET("/docs/*any", func(c *gin.Context) {
		if c.Param("any") == "/" {
			redirect(c)
			return
		}
		ui(c)
	})
}

// run monta as dependências e sobe o servidor.
func run() error {
	gin.SetMode(gin.ReleaseMode) // sem o banner de debug do Gin nos logs
	pool, err := pgxpool.New(context.Background(), env("DATABASE_URL", "postgres://app:app@localhost:55432/app"))
	if err != nil {
		return err
	}
	defer pool.Close()
	router := NewRouter(NewJobHandler(NewJobRepository(pool), "gateway-go"))
	RegisterDocs(router)
	addr := env("ADDR", ":8000")
	slog.Info("listening", "addr", addr, "docs", "/docs/index.html")
	return router.Run(addr)
}

// main configura o log estruturado (JSON, uma linha por evento, campo "service")
// e delega a run. Todo log do processo usa o slog padrão configurado aqui.
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "gateway-go"))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
