package metrics

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	vm "github.com/VictoriaMetrics/metrics"

	"github.com/Clarilab/eventhorizon/namespace"
)

const metricName = "kycnow_eventsourcing_total"

var (
	buffer chan metric
	wg     *sync.WaitGroup
)

type metric struct {
	tenant      string
	handlerType string
	action      string
	data        any
}

// EnableMetrics enables async metrics recording.
func EnableMetrics() {
	buffer = make(chan metric, 10000)

	go func() {
		for m := range buffer {
			record(m)
		}

		wg.Done()
	}()

	SetCommandMiddleware(NewCommandHandlerMiddleware())
	SetEventMiddleware(NewEventHandlerMiddleware())
}

// CloseMetrics closes all resources used by the metrics system.
func CloseMetrics() {
	if buffer != nil {
		wg.Go(func() { close(buffer) })
	}
}

// queue sends a metric to the background processor.
func queue(ctx context.Context, handlerType, action string, data any) {
	tenant := namespace.FromContext(ctx)
	if tenant == namespace.DefaultNamespace {
		tenant = ""
	}

	select {
	case buffer <- metric{
		tenant:      tenant,
		handlerType: handlerType,
		action:      action,
		data:        data,
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

	if m.tenant != "" {
		labelMap["tenant"] = m.tenant
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

		labels[toSnakeCase(name)] = value
	}

	return labels
}

// toSnakeCase converts a string to snake_case.
// Handles PascalCase, camelCase, and ALLCAPS.
func toSnakeCase(s string) string {
	if s == "" {
		return s
	}

	var result []rune
	runes := []rune(s)

	for i := 0; i < len(runes); i++ {
		r := runes[i]

		// If uppercase letter
		if r >= 'A' && r <= 'Z' {
			// Add underscore before uppercase if:
			// 1. Not the first character
			// 2. Previous character is lowercase, OR
			// 3. Next character is lowercase (handles "UserID" -> "user_id")
			if i > 0 {
				prev := runes[i-1]
				nextIsLower := i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z'
				if (prev >= 'a' && prev <= 'z') || nextIsLower {
					result = append(result, '_')
				}
			}
			result = append(result, r-'A'+'a') // Convert to lowercase
		} else {
			result = append(result, r)
		}
	}

	return string(result)
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
