package main

import (
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

// JobHandler expõe os endpoints de jobs.
type JobHandler struct {
	repo   *JobRepository
	origin string
}

// NewJobHandler cria o handler; origin identifica o gateway no envelope.
func NewJobHandler(repo *JobRepository, origin string) *JobHandler {
	return &JobHandler{repo: repo, origin: origin}
}

// NewRouter registra as rotas do gateway (sem Swagger; ver main.go).
func NewRouter(h *JobHandler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/jobs", h.CreateJob)
	r.GET("/jobs/:id", h.GetJob)
	return r
}

// CreateJob godoc
// @Summary     Cria um job
// @Description Grava o job e a outbox numa transação e responde 202. O processamento é assíncrono.
// @Tags        jobs
// @Accept      json
// @Produce     json
// @Param       body body     CreateJobRequest true "Job"
// @Success     202  {object} CreateJobResponse
// @Failure     422  {object} ErrorResponse
// @Router      /jobs [post]
func (h *JobHandler) CreateJob(c *gin.Context) {
	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, ErrorResponse{Detail: describeBindError(err)})
		return
	}
	id, err := h.repo.Create(c.Request.Context(), req, h.origin)
	if err != nil {
		log.Printf("create job: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Detail: "internal error"})
		return
	}
	c.JSON(http.StatusAccepted, CreateJobResponse{JobID: id})
}

// describeBindError traduz o erro do binding para um texto curto e estável,
// igual ao do gateway-py: `type` ausente/vazio ou corpo que não é JSON válido.
func describeBindError(err error) string {
	var validation validator.ValidationErrors
	if errors.As(err, &validation) {
		return "type is required"
	}
	return "invalid request body"
}

// GetJob godoc
// @Summary     Consulta um job
// @Description Devolve o status e os resultados gravados pelos workers.
// @Tags        jobs
// @Produce     json
// @Param       id  path     string true "ID do job (uuid)"
// @Success     200 {object} JobView
// @Failure     404 {object} ErrorResponse
// @Failure     422 {object} ErrorResponse
// @Router      /jobs/{id} [get]
func (h *JobHandler) GetJob(c *gin.Context) {
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil { // paridade com o 422 do FastAPI
		c.JSON(http.StatusUnprocessableEntity, ErrorResponse{Detail: "invalid job id"})
		return
	}
	view, err := h.repo.Get(c.Request.Context(), id)
	if errors.Is(err, ErrNotFound) {
		c.JSON(http.StatusNotFound, ErrorResponse{Detail: "job not found"})
		return
	}
	if err != nil {
		log.Printf("get job: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Detail: "internal error"})
		return
	}
	c.JSON(http.StatusOK, view)
}
