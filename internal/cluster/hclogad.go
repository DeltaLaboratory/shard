package cluster

import (
	"io"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/rs/zerolog"
)

type hclogAdapter struct {
	logger zerolog.Logger
	name   string
	level  hclog.Level
}

// Convert hclog level to zerolog level
func hclogToZerologLevel(level hclog.Level) zerolog.Level {
	switch level {
	case hclog.Trace:
		return zerolog.TraceLevel
	case hclog.Debug:
		return zerolog.DebugLevel
	case hclog.Info:
		return zerolog.InfoLevel
	case hclog.Warn:
		return zerolog.WarnLevel
	case hclog.Error:
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}

func (h *hclogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	event := h.logger.WithLevel(hclogToZerologLevel(level))
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key, ok := args[i].(string)
			if ok {
				event = event.Interface(key, args[i+1])
			}
		}
	}
	event.Msg(msg)
}

func (h *hclogAdapter) Trace(msg string, args ...interface{}) {
	if h.IsTrace() {
		h.Log(hclog.Trace, msg, args...)
	}
}

func (h *hclogAdapter) Debug(msg string, args ...interface{}) {
	if h.IsDebug() {
		h.Log(hclog.Debug, msg, args...)
	}
}

func (h *hclogAdapter) Info(msg string, args ...interface{}) {
	if h.IsInfo() {
		h.Log(hclog.Info, msg, args...)
	}
}

func (h *hclogAdapter) Warn(msg string, args ...interface{}) {
	if h.IsWarn() {
		h.Log(hclog.Warn, msg, args...)
	}
}

func (h *hclogAdapter) Error(msg string, args ...interface{}) {
	if h.IsError() {
		h.Log(hclog.Error, msg, args...)
	}
}

func (h *hclogAdapter) IsTrace() bool {
	return h.logger.GetLevel() <= zerolog.TraceLevel
}

func (h *hclogAdapter) IsDebug() bool {
	return h.logger.GetLevel() <= zerolog.DebugLevel
}

func (h *hclogAdapter) IsInfo() bool {
	return h.logger.GetLevel() <= zerolog.InfoLevel
}

func (h *hclogAdapter) IsWarn() bool {
	return h.logger.GetLevel() <= zerolog.WarnLevel
}

func (h *hclogAdapter) IsError() bool {
	return h.logger.GetLevel() <= zerolog.ErrorLevel
}

func (h *hclogAdapter) ImpliedArgs() []interface{} {
	return []interface{}{} // Zerolog doesn't support this directly
}

func (h *hclogAdapter) With(args ...interface{}) hclog.Logger {
	newLogger := h.logger
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key, ok := args[i].(string)
			if ok {
				newLogger = newLogger.With().Interface(key, args[i+1]).Logger()
			}
		}
	}
	return &hclogAdapter{
		logger: newLogger,
		name:   h.name,
		level:  h.level,
	}
}

func (h *hclogAdapter) Name() string {
	return h.name
}

func (h *hclogAdapter) Named(name string) hclog.Logger {
	var newName string
	if h.name != "" {
		newName = h.name + "." + name
	} else {
		newName = name
	}
	return &hclogAdapter{
		logger: h.logger.With().Str("logger", newName).Logger(),
		name:   newName,
		level:  h.level,
	}
}

func (h *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{
		logger: h.logger.With().Str("logger", name).Logger(),
		name:   name,
		level:  h.level,
	}
}

func (h *hclogAdapter) SetLevel(level hclog.Level) {
	h.level = level
	h.logger = h.logger.Level(hclogToZerologLevel(level))
}

func (h *hclogAdapter) GetLevel() hclog.Level {
	return h.level
}

func (h *hclogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return nil
}

func (h *hclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	if opts == nil {
		opts = &hclog.StandardLoggerOptions{}
	}

	return h.logger.With().Logger().Level(hclogToZerologLevel(opts.ForceLevel))
}

// Constructor function
func NewHclogAdapter(logger zerolog.Logger) hclog.Logger {
	return &hclogAdapter{
		logger: logger,
		level:  hclog.Info, // Default level
	}
}
