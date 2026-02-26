package metrics

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	vm "github.com/VictoriaMetrics/metrics"

	"github.com/Clarilab/eventhorizon/namespace"
)

const metricName = "kycnow_eventsourcing_total"

var (
	buffer     chan metric
	wg         *sync.WaitGroup
	enableOnce sync.Once
)

type metric struct {
	action      string
	handlerType string
	tenant      string
	successful  bool
	data        any
}

// EnableMetrics enables async metrics recording for commands and events.
//
// This function is safe to call multiple times - subsequent calls are no-ops.
//
// Metric Labels:
// Struct fields are automatically converted to metric labels using the following rules:
//   - Field names are converted to snake_case (e.g., "OrderID" -> "order_id")
//   - Only exported fields (starting with uppercase) are included
//   - Maps, slices, and time.Time fields are excluded
//
// Struct Tags:
//   - metrics:"label:custom_name" - Use a custom label name instead of field name
//   - metrics:"exclude" - Skip this field entirely
//   - metrics:"include" - Flatten this nested struct's fields into labels
//
// Nested Structs:
// By default, only the top-level struct fields become labels. Nested structs are
// included only when they have one of these tags:
//   - json:",inline" tag
//   - bson:",inline" tag
//   - metrics:"include" tag
//
// Example:
//
//	type Command struct {
//	    Base     BaseStruct `metrics:"include"`
//	    ID       string `metrics:"label:aggregate_id"`
//	    UserID   string `metrics:"exclude"`
//	}
//
// This extracts all exported fields from BaseStruct and aggregate_id (from ID).
func EnableMetrics() {
	enableOnce.Do(func() {
		buffer = make(chan metric, 10000)
		wg = &sync.WaitGroup{}

		wg.Go(func() {
			for m := range buffer {
				record(m)
			}

		})

		setCommandMiddleware(NewCommandHandlerMiddleware())
		setEventMiddleware(NewEventHandlerMiddleware())
	})
}

// CloseMetrics closes all resources used by the metrics system.
func CloseMetrics() {
	if buffer != nil {
		wg.Go(func() { close(buffer) })
	}
}

// queue sends a metric to the background processor.
func queue(ctx context.Context, handlerType, action string, successful bool, data any) {
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
		successful:  successful,
	}:
	default:
		// Buffer full, drop metric
	}
}

// record extracts labels and increments the counter.
func record(m metric) {
	labelMap := extractLabels(m.data)

	if m.action != "" {
		labelMap["action"] = m.action
	}

	if m.handlerType != "" {
		labelMap["handler_type"] = m.handlerType
	}

	if m.tenant != "" {
		labelMap["tenant"] = m.tenant
	}

	labelMap["successful"] = strconv.FormatBool(m.successful)

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

// extractLabels reads struct fields with metrics tags to create metric labels.
// Nested structs are flattened one level deep only when they have json/bson inline tags
// or metrics:include tag. Later values overwrite earlier ones.
func extractLabels(data any) map[string]string {
	labels := make(map[string]string)
	if data == nil {
		return labels
	}

	value := reflect.Indirect(reflect.ValueOf(data))
	if value.Kind() != reflect.Struct {
		return labels
	}

	// Get label name from field
	getName := func(structField reflect.StructField) string {
		if label, ok := strings.CutPrefix(structField.Tag.Get("metrics"), "label:"); ok {
			return toSnakeCase(label)
		}
		return toSnakeCase(structField.Name)
	}

	// Process fields of a struct value (skip nested structs and time.Time)
	process := func(structValue reflect.Value) {
		typ := structValue.Type()
		for i := 0; i < typ.NumField(); i++ {
			field, val := typ.Field(i), structValue.Field(i)
			if !val.CanInterface() || field.Tag.Get("metrics") == "exclude" ||
				val.Kind() == reflect.Map || val.Kind() == reflect.Slice ||
				val.Kind() == reflect.Struct {
				continue
			}
			if s := fmt.Sprint(val.Interface()); s != "" {
				labels[getName(field)] = s
			}
		}
	}

	// Check if struct should be flattened (has inline tag or metrics:include)
	shouldFlatten := func(structField reflect.StructField) bool {
		return strings.Contains(structField.Tag.Get("json"), "inline") ||
			strings.Contains(structField.Tag.Get("bson"), "inline") ||
			structField.Tag.Get("metrics") == "include"
	}

	// outer first
	process(value)

	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field, val := typ.Field(i), value.Field(i)
		if val.Kind() == reflect.Struct && val.Type() != reflect.TypeFor[time.Time]() &&
			shouldFlatten(field) {
			// nested last (wins on collision)
			process(reflect.Indirect(val))
		}
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

	for i := range runes {
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
