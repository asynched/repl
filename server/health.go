package server

import (
	"fmt"
	"runtime"
	"time"

	"github.com/gofiber/fiber/v2"
)

type HealthController struct {
	startup time.Time
}

func (controller *HealthController) GetHealth(c *fiber.Ctx) error {
	mem := runtime.MemStats{}

	runtime.ReadMemStats(&mem)

	return c.JSON(fiber.Map{
		"status":   "up",
		"routines": runtime.NumGoroutine(),
		"version":  runtime.Version(),
		"heap":     fmt.Sprintf("%.2fMB", float64(mem.Alloc)/1024/1024),
		"total":    fmt.Sprintf("%.2fMB", float64(mem.TotalAlloc)/1024/1024),
		"uptime":   fmt.Sprintf("%fs", time.Since(controller.startup).Seconds()),
	})
}

func (controller *HealthController) Setup(router fiber.Router) {
	router.Get("/", controller.GetHealth)
}

func NewHealthController() *HealthController {
	return &HealthController{
		startup: time.Now(),
	}
}
