package global

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	MetricsStateOK = 1
	MetricsStateNO = 0
)

var (
	leaderState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_leader_state",
			Help: "The cluster leader state: 0=false, 1=true",
		},
	)

	destinationState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_destination_state",
			Help: "The destination running state: 0=stopped, 1=ok",
		},
	)

	transferDelay = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_delay",
			Help: "The transfer slave lag",
		},
	)

	insertNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_inserted_num",
			Help: "The number of data inserted to destination",
		}, []string{"table"},
	)

	updateNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_updated_num",
			Help: "The number of data updated to destination",
		}, []string{"table"},
	)

	deleteNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_deleted_num",
			Help: "The number of data deleted from destination",
		}, []string{"table"},
	)
)

func SetLeaderState(state int) {
	if _config.EnableExporter {
		leaderState.Set(float64(state))
	}
}

func SetDestinationState(state int) {
	if _config.EnableExporter {
		destinationState.Set(float64(state))
	}
}

func SetTransferDelay(delay uint32) {
	if _config.EnableExporter {
		transferDelay.Set(float64(delay))
	}
}

func IncInsertNum(lab string) {
	if _config.EnableExporter {
		insertNum.WithLabelValues(lab).Inc()
	}
}

func IncUpdateNum(lab string) {
	if _config.EnableExporter {
		updateNum.WithLabelValues(lab).Inc()
	}
}

func IncDeleteNum(lab string) {
	if _config.EnableExporter {
		deleteNum.WithLabelValues(lab).Inc()
	}
}

func StartMonitor() {
	if _config.EnableExporter {
		go func() {
			http.Handle("/", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", _config.ExporterPort), nil)
		}()
	}
}
