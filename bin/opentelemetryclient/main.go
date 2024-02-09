package main

import (
	"context"
	"log"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := collector.NewMetricsServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &collector.ExportMetricsServiceRequest{
		ResourceMetrics: []*metrics.ResourceMetrics{
			{
				ScopeMetrics: []*metrics.ScopeMetrics{{
					Metrics: []*metrics.Metric{
						{
							Name: "test_metric",
							Data: &metrics.Metric_Gauge{
								Gauge: &metrics.Gauge{
									DataPoints: []*metrics.NumberDataPoint{
										{
											TimeUnixNano: uint64(time.Now().UnixNano()),
											Value:        &metrics.NumberDataPoint_AsInt{AsInt: 123},

											Attributes: []*common.KeyValue{
												{
													Key:   "int",
													Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 12}},
												},
												{
													Key:   "string",
													Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test string"}},
												},
												{
													Key:   "bool",
													Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}},
												},
												{
													Key:   "double",
													Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 12.2}},
												},
												{
													Key:   "bytes",
													Value: &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: []byte{1, 2, 3, 4, 5}}},
												},
											},
										},
										{
											TimeUnixNano: uint64(time.Now().UnixNano()),
											Flags:        uint32(metrics.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK),
											//Value:        &metrics.NumberDataPoint_AsInt{AsInt: 123},

											Attributes: []*common.KeyValue{
												{
													Key:   "int",
													Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 12}},
												},
												{
													Key:   "string",
													Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test string"}},
												},
												{
													Key:   "bool",
													Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}},
												},
												{
													Key:   "double",
													Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 12.2}},
												},
												{
													Key:   "bytes",
													Value: &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: []byte{1, 2, 3, 4, 5}}},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				},
			},
		},
	}

	// Send the metrics
	resp, err := client.Export(ctx, req)
	if err != nil {
		log.Fatalf("Could not send metrics: %v", err)
	}

	log.Printf("Metrics sent successfully: %v", resp)
}
