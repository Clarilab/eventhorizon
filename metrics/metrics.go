package metrics

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	vm "github.com/VictoriaMetrics/metrics"

	"github.com/Clarilab/eventhorizon/namespace"
)

const metricName = "kycnow_eventsourcing_total"

var (
	buffer   = make(chan metric, 10000)
	workload string
)

type metric struct {
	ctx          context.Context
	handlerType  string
	action       string
	data         any
	workloadName string
}

// EnableMetrics enables async metrics recording with the given workload name.
func EnableMetrics(workloadName string) error {
	if workloadName == "" {
		return errors.New("workload name cannot be empty")
	}

	workload = workloadName

	go func() {
		for m := range buffer {
			record(m)
		}
	}()

	SetCommandMiddleware(NewCommandHandlerMiddleware())
	SetEventMiddleware(NewEventHandlerMiddleware())

	return nil
}

// queue sends a metric to the background processor.
func queue(ctx context.Context, handlerType, action string, data any) {
	select {
	case buffer <- metric{
		ctx:          ctx,
		handlerType:  handlerType,
		action:       action,
		data:         data,
		workloadName: workload,
	}:
	default:
		// Buffer full, drop metric
	}
}

// record extracts labels and increments the counter.
func record(m metric) {
	labelMap := extractLabels(m.data)

	if m.handlerType != "" {
		labelMap["handler_type"] = m.handlerType
	}
	if m.action != "" {
		labelMap["action"] = m.action
	}
	if m.workloadName != "" {
		labelMap["workload"] = m.workloadName
	}

	if tenant := namespace.FromContext(m.ctx); tenant != "" && tenant != namespace.DefaultNamespace {
		labelMap["tenant"] = tenant
	}

	var labels []string
	for k, v := range labelMap {
		if k != "" && v != "" {
			labels = append(labels, fmt.Sprintf("%s=%q", sanitizeLabelName(k), v))
		}
	}

	sort.Strings(labels)

	fullName := fmt.Sprintf("%s{%s}", metricName, strings.Join(labels, ","))
	vm.GetOrCreateCounter(fullName).Inc()
}

// extractLabels reads struct fields with eh tags to create metric labels.
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

// sanitizeLabelName ensures label names match Prometheus format: [a-zA-Z_][a-zA-Z0-9_]*
func sanitizeLabelName(s string) string {
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, s)

	if len(s) > 0 && s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}

	return s
}
