package connectors

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/spiceai/spice/pkg/observations"
)

type InfluxDbConnector struct {
	client influxdb2.Client
	org    string
	bucket string
	field  string
}

func NewInfluxDbConnector(params map[string]string) Connector {
	client := influxdb2.NewClient(params["url"], params["token"])
	org := params["org"]
	bucket := strings.ToUpper(params["bucket"])
	field := params["field"]

	return &InfluxDbConnector{
		client: client,
		org:    org,
		bucket: bucket,
		field:  field,
	}
}

func (c *InfluxDbConnector) Initialize() error {
	return nil
}

func (c *InfluxDbConnector) FetchData(period time.Duration, interval time.Duration) ([]observations.Observation, error) {
	periodStr := fmt.Sprintf("%ds", int64(period.Seconds()))
	intervalStr := fmt.Sprintf("%ds", int64(interval.Seconds()))

	query := fmt.Sprintf(`
		from(bucket:"%s") |>
		range(start: -%s) |>
		filter(fn: (r) => r["_measurement"] == "tick") |>
		filter(fn: (r) => r["_field"] == "%s") |>
		aggregateWindow(every: %s, fn: mean, createEmpty: false)
    `, c.bucket, periodStr, c.field, intervalStr)

	result, err := c.client.QueryAPI(c.org).Query(context.Background(), query)
	if err != nil {
		log.Printf("Influx query failed: %v", err)
		return nil, err
	}
	defer result.Close()

	var newObservations []observations.Observation

	for result.Next() {
		var ts uint64
		var data = make(map[string]interface{})

		ts = uint64(result.Record().Time().Unix())
		data[c.field] = result.Record().Value().(float64)

		observation := observations.Observation{
			Timestamp: ts,
			Data:      data,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}
