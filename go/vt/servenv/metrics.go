package servenv

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/metrics"
)

var metricDescriptions []metrics.Description

func RegisterRuntimeMetrics() {
	HTTPHandleFunc("/debug/runtime/metrics", func(writer http.ResponseWriter, request *http.Request) {
		samples := make([]metrics.Sample, len(metricDescriptions))
		for i := range samples {
			samples[i].Name = metricDescriptions[i].Name
		}

		metrics.Read(samples)

		dict := make(map[string]float64)
		for _, sample := range samples {
			name, value := sample.Name, sample.Value

			switch value.Kind() {
			case metrics.KindUint64:
				dict[name] = float64(value.Uint64())
			case metrics.KindFloat64:
				fmt.Printf("%s: %f\n", name, value.Float64())
				dict[name] = value.Float64()
			}
		}

		enc := json.NewEncoder(writer)
		_ = enc.Encode(dict)
	})
}

func init() {
	for _, desc := range metrics.All() {
		switch desc.Kind {
		case metrics.KindUint64, metrics.KindFloat64:
			metricDescriptions = append(metricDescriptions, desc)
		}
	}
	RegisterRuntimeMetrics()
}
