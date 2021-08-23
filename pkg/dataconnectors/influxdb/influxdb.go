package influxdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/domain"
)

const (
	InfluxDbConnectorName string = "influxdb"
)

type InfluxDbConnector struct {
	client      influxdb2.Client
	org         string
	bucket      string
	field       string
	measurement string
}

func NewInfluxDbConnector() *InfluxDbConnector {
	return &InfluxDbConnector{}
}

func (c *InfluxDbConnector) Init(params map[string]string) error {
	if _, ok := params["url"]; !ok {
		return errors.New("influxdb connector requires the 'url' parameter to be set")
	}

	if _, ok := params["token"]; !ok {
		return errors.New("influxdb connector requires the 'token' parameter to be set")
	}

	c.client = influxdb2.NewClient(params["url"], params["token"])

	if org, ok := params["org"]; ok {
		c.org = org
	}

	if bucket, ok := params["bucket"]; ok {
		c.bucket = bucket
	}

	if field, ok := params["field"]; ok {
		c.field = field
	} else {
		// Default to _value
		c.field = "_value"
	}

	if measurement, ok := params["measurement"]; ok {
		c.measurement = measurement
	} else {
		// Default to _measurement
		c.measurement = "_measurement"
	}

	return nil
}

func (c *InfluxDbConnector) FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]byte, error) {
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

	header := true
	annotations := []domain.DialectAnnotations{"group", "datatype", "default"}
	dateTimeFormat := domain.DialectDateTimeFormatRFC3339
	dialect := &domain.Dialect{
		Header: &header,
		Annotations: &annotations,
		DateTimeFormat: &dateTimeFormat,
	}

	result, err := c.client.QueryAPI(c.org).QueryRaw(context.Background(), query, dialect)
	if err != nil {
		log.Printf("InfluxDb query failed: %v", err)
		return nil, err
	}

	return []byte(result), nil
}
