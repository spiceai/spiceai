package influxdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/domain"
)

const (
	InfluxDbConnectorName string = "influxdb"
)

type InfluxDbConnector struct {
	client             influxdb2.Client
	org                string
	bucket             string
	field              string
	measurement        string
	lastFetchPeriodEnd time.Time
	data               []byte
	dataMutex          sync.RWMutex
}

func NewInfluxDbConnector() *InfluxDbConnector {
	return &InfluxDbConnector{
		dataMutex: sync.RWMutex{},
	}
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
	c.dataMutex.Lock()
	defer c.dataMutex.Unlock()

	periodStart := epoch
	periodEnd := epoch.Add(period)

	if !c.lastFetchPeriodEnd.IsZero() && c.lastFetchPeriodEnd.After(epoch) {
		// If we've already fetched, only fetch the difference with an interval overlap
		periodStart = c.lastFetchPeriodEnd.Add(-interval)
	}

	if periodStart == periodEnd || periodStart.After(periodEnd) {
		// No new data to fetch
		return c.data, nil
	}

	intervalStr := fmt.Sprintf("%ds", int64(interval.Seconds()))

	query := fmt.Sprintf(`
		from(bucket:"%s") |>
		range(start: %s, stop: %s) |>
		filter(fn: (r) => r["_measurement"] == "%s") |>
		filter(fn: (r) => r["_field"] == "%s") |>
		aggregateWindow(every: %s, fn: mean, createEmpty: false)
    `, c.bucket, periodStart.Format(time.RFC3339), periodEnd.Format(time.RFC3339), c.measurement, c.field, intervalStr)

	header := true
	annotations := []domain.DialectAnnotations{"group", "datatype", "default"}
	dateTimeFormat := domain.DialectDateTimeFormatRFC3339
	dialect := &domain.Dialect{
		Header:         &header,
		Annotations:    &annotations,
		DateTimeFormat: &dateTimeFormat,
	}

	result, err := c.client.QueryAPI(c.org).QueryRaw(context.Background(), query, dialect)
	if err != nil {
		log.Printf("InfluxDb query failed: %v", err)
		return nil, err
	}

	data := []byte(result)

	c.data = data
	c.lastFetchPeriodEnd = periodEnd

	return data, nil
}
