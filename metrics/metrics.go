package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	vm "github.com/VictoriaMetrics/metrics"

	"github.com/Clarilab/eventhorizon/namespace"
)

type recorder interface {
	RecordHandlerExecution(ctx context.Context, m metrics)
}

var (
	globalRecorderMu sync.RWMutex
	globalRecorder   recorder
)

func EnableMetrics(workloadName string) error {
	if workloadName == "" {
		return errors.New("workload name cannot be empty")
	}

	globalRecorderMu.Lock()
	defer globalRecorderMu.Unlock()

	globalRecorder = &victoriaMetricsRecorder{Workload: sanitize(workloadName)}
	SetCommandMiddleware(NewCommandHandlerMiddleware(globalRecorder))
	SetEventMiddleware(NewEventHandlerMiddleware(globalRecorder))

	return nil
}

type metrics struct {
	Name        string
	HandlerType string
	Action      string
	Labels      map[string]string
	Error       error
}

type victoriaMetricsRecorder struct {
	Workload string
}

func (r *victoriaMetricsRecorder) RecordHandlerExecution(ctx context.Context, m metrics) {
	name := "kycnow_eventsourcing_total"

	labelMap := map[string]string{}

	for k, v := range m.Labels {
		labelMap[sanitize(k)] = v
	}

	if m.HandlerType != "" {
		labelMap["handler_type"] = m.HandlerType
	}
	if m.Action != "" {
		labelMap["action"] = m.Action
	}

	if r.Workload != "" {
		labelMap["workload"] = r.Workload
	}

	if tenant := namespace.FromContext(ctx); tenant != "" && tenant != namespace.DefaultNamespace {
		labelMap["tenant"] = tenant
	}

	var labels []string

	for k, v := range labelMap {
		if k == "" || v == "" {
			continue
		}

		labels = append(labels, fmt.Sprintf(`%s=%q`, k, v))
	}

	sort.Strings(labels)

	metricName := fmt.Sprintf("%s{%s}", name, strings.Join(labels, ","))

	vm.GetOrCreateCounter(metricName).Inc()
}

func InstallHandler(mux *http.ServeMux, path string) {
	mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
		vm.WritePrometheus(w, true)
	})
}

func extractLabels(i any) map[string]string {
	labels := make(map[string]string)
	if i == nil {
		return labels
	}

	v := reflect.Indirect(reflect.ValueOf(i))
	if v.Kind() != reflect.Struct {
		return labels
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if len(field.Name) == 0 || field.Name[0] < 'A' || field.Name[0] > 'Z' {
			continue
		}

		tag := field.Tag.Get("eh")

		if tag == "nolabel" {
			continue
		}

		fieldValue := v.Field(i)
		fieldKind := fieldValue.Kind()

		if fieldKind == reflect.Struct || fieldKind == reflect.Map || fieldKind == reflect.Slice {
			continue
		}

		if fieldKind == reflect.Int64 {
			if _, ok := fieldValue.Interface().(time.Time); ok {
				continue
			}
		}

		name := field.Name

		parts := strings.SplitSeq(tag, ",")
		for part := range parts {
			if after, ok := strings.CutPrefix(part, "label:"); ok {
				name = after
				break
			}
		}

		value := fmt.Sprint(fieldValue.Interface())
		if value == "" {
			continue
		}

		labels[name] = value
	}

	return labels
}

// sanitize sanitizes label names for Prometheus compatibility.
// Prometheus metric names and label names must match the regex [a-zA-Z_][a-zA-Z0-9_]*.
func sanitize(s string) string {
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == ':' {
			return r
		}

		return '_'
	}, s)

	if len(s) > 0 && s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}

	return s
}
