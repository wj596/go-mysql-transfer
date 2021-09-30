package test

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
	"testing"
)

func TestGauge(t *testing.T) {
	g := metrics.NewGauge()
	//metrics.Register("bar", g)
	g.Update(1)
	g.Update(2)
	g.Update(3)

	//gg := metrics.Get("bar").(metrics.Gauge)
	fmt.Println(g.Value())

	//go metrics.Log(metrics.DefaultRegistry,
	//	1 * time.Second,
	//	log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
	//
	//var j int64
	//j = 1
	//for true {
	//	time.Sleep(time.Second * 1)
	//	g.Update(j)
	//	j++
	//}

}
