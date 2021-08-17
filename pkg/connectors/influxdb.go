package connectors

import (
	"context"
	"fmt"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/spiceai/spice/pkg/observations"
)

type InfluxDbConnector struct {
	client influxdb2.Client
	org    string
	bucket string
	measurement string
	field  string
}

func NewInfluxDbConnector(params map[string]string) Connector {
	client := influxdb2.NewClient(params["url"], params["token"])
	org := params["org"]
	bucket := params["bucket"]
	measurement := params["measurement"]
	field := params["field"]

	return &InfluxDbConnector{
		client: client,
		org:    org,
		bucket: bucket,
		measurement: measurement,
		field:  field,
	}
}

func (c *InfluxDbConnector) Type() string {
	return InfluxDbConnectorId
}

func (c *InfluxDbConnector) Initialize() error {
	return nil
}

func (c *InfluxDbConnector) FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]observations.Observation, error) {
	periodStart := epoch.Format(time.RFC3339)
	periodEnd := epoch.Add(period).Format(time.RFC3339)

	intervalStr := fmt.Sprintf("%ds", int64(interval.Seconds()))

	query := fmt.Sprintf(`
		from(bucket:"%s") |>
		range(start: %s, stop: %s) |>
		filter(fn: (r) => r["_measurement"] == "%s") |>
		filter(fn: (r) => r["_field"] == "%s") |>
		aggregateWindow(every: %s, fn: mean, createEmpty: false)
    `, c.bucket, periodStart, periodEnd, c.measurement, c.field, intervalStr)

	result, err := c.client.QueryAPI(c.org).Query(context.Background(), query)
	if err != nil {
		log.Printf("Influx query failed: %v", err)
		return nil, err
	}
	defer result.Close()

	var newObservations []observations.Observation

	for result.Next() {
		var ts int64
		var data = make(map[string]float64)

		ts = result.Record().Time().Unix()
		data[c.field] = result.Record().Value().(float64)

		observation := observations.Observation{
			Time: ts,
			Data: data,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}
