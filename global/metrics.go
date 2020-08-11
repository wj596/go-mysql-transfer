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
	applicationState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "application_state",
			Help: "The application running state: 0=stopped, 1=ok",
		},
	)

	transferState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "transfer_state",
			Help: "The transfer running state: 0=stopped, 1=ok",
		},
	)

	leaderState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "leader_state",
			Help: "The cluster leader state: 0=false, 1=true",
		},
	)

	destinationState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "destination_state",
			Help: "The destination running state: 0=stopped, 1=ok",
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

func SetApplicationState(state int) {
	if _config.EnableExporter {
		applicationState.Set(float64(state))
	}
}

func SetTransferState(state int) {
	if _config.EnableExporter {
		transferState.Set(float64(state))
	}
}

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
