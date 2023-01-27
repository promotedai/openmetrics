package main // "github.com/promotedai/metrics/api/main"

import (
	"context"
	b64 "encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/promotedai/go-common/apigw"
	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Useful to override for testing purposes.
var Now = time.Now
var devMode = unsafeParseBool(os.Getenv("DEV_MODE"), false)
var shouldTextLogRequest = getShouldTextLogRequest()

func getShouldTextLogRequest() bool {
	// Falback to devMode flag.
	if os.Getenv("TEXT_LOG_REQUEST") != "" {
		return unsafeParseBool(os.Getenv("TEXT_LOG_REQUEST"), false)
	} else {
		return devMode
	}
}

type EventResponse struct {
	error string
}

type batchLogLogger struct {
	writer BatchLogWriter
	// Temporarily, can be 0 when not specified.  If specified, this is used as
	// the platformId.
	platformId uint64
}

var batchLogger batchLogLogger

func createKafkaBatchLogWriter() (BatchLogWriter, error) {
	kafkaBrokersString := os.Getenv("EVENT_KAFKA_BROKERS")
	if kafkaBrokersString == "" {
		return nil, errors.New("Needs to specify EVENT_KAFKA_BROKERS")
	}

	kafkaBrokers := strings.Split(kafkaBrokersString, ",")
	tlsPem := ""
	useTlsPem, err := strconv.ParseBool(os.Getenv("USE_AWS_KAFKA_TLS_PEM"))
	if err == nil && useTlsPem {
		tlsPem = awsKafkaTlsPem
	}

	// Kafka topic naming convention.
	// https://riccomini.name/how-paint-bike-shed-kafka-topic-naming-conventions
	// Kafka topic: "<message type>.<dataset name>.<data name>"
	kafkaDataset := os.Getenv("KAFKA_DATASET")
	if kafkaDataset == "" {
		kafkaDataset = "event"
	}

	// This is the max time that KafkaWriter will wait until it gets a full batch
	// before sending to Kafka.
	// 10 microseconds was chosen slightly arbitrarily.
	// After we get higher load, we will eventually want to adjust this for
	// efficiency.  Same for tuning batch size.
	// Timers use 1-buffered channels so firing early should be fine.
	batchTimeoutMicros := int64(10)
	batchTimeoutMicrosString := os.Getenv("KAFKA_BATCH_TIMEOUT_MICROS")
	if batchTimeoutMicrosString != "" {
		batchTimeoutMicros, err = strconv.ParseInt(batchTimeoutMicrosString, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	batchTimeout := time.Duration(batchTimeoutMicros) * time.Microsecond
	return NewKafkaBatchLogWriter(kafkaDataset, kafkaBrokers, tlsPem, batchTimeout)
}

// createBatchLogWriter returns the BatchLogWriter to use for this instance of Metrics API.
// This can be used to swap out implementations.
func createBatchLogWriter() (BatchLogWriter, error) {
	writerImpl := os.Getenv("WRITER_IMPL")
	// Default to kafka.
	if writerImpl == "" || writerImpl == "kafka" {
		return createKafkaBatchLogWriter()
	} else if writerImpl == "print" {
		return &PrintBatchLogWriter{}, nil
	} else if writerImpl == "noop" {
		return &NoopBatchLogWriter{}, nil
	} else {
		return nil, fmt.Errorf("Unrecognized WRITER_IMPL=%s", writerImpl)
	}
}

func main() {
	var err error
	batchLogger.writer, err = createBatchLogWriter()
	if err != nil {
		log.Errorf("Error creating KafkaBatchLogWriter: %s\n", err.Error())
		os.Exit(1)
	}
	platformId, err := getPlatformId()
	if err != nil {
		log.Errorf(err.Error())
		os.Exit(1)
	}

	log.SetFormatter(&log.JSONFormatter{})

	batchLogger.platformId = platformId
	log.Infof("platformId: %d", batchLogger.platformId)

	// AWS Lambda does not actually return from lambda.Start.  Leaving in Close code
	// just in case this changes.
	defer func() {
		fmt.Println("Exiting main.")
		if err := batchLogger.writer.Close(); err != nil {
			log.Errorf("Error closing eventWriter: %s\n", err.Error())
		}
	}()

	lambda.Start(Handler)
}

// The default value is zero.
func getPlatformId() (uint64, error) {
	platformId := os.Getenv("PLATFORM_ID")
	if platformId != "" {
		return strconv.ParseUint(platformId, 10, 64)
	}
	platformEnv := os.Getenv("PLATFORM_ENV")
	return innerGetPlatformId(platformEnv)
}

// TODO - remove this function.  Move to use PLATFORM_ID and have TF set a default PLATFORM_ID.
func innerGetPlatformId(platformEnv string) (uint64, error) {
	// We do not want to use continuous IDs in case hackers try to hack us.
	// We want it to be hard to guess most marketplace IDs.
	platformIdMap := map[string]uint64{
		// Keep map in order by ID.
		// For environments from the same
		"aaa-prod":     1,
		"aaa-dev":      2,
		"aaa-prod2":    3,
		"aaa-dev2":     4,
		"prm-dev":      10,
		"pangea-prod":  20,
		"pangea-dev":   21,
		"thefeed-prod": 40,
		"thefeed-dev":  41,
		"thefeed-dev2": 42,
		"bbb-prod":     80,
		"bbb-dev":      81,
		"ccc-prod":     120,
		"ccc-dev":      121,
		"ddd-dev":      160,
		"ddd-prod":     161,
		"eee-prod":     200,
		"eee-dev":      201,
		// TODO - for next ones, use a random long (less than a certain size).
		// Please keep some space between IDs (~40 ids is fine).  Keep a few between platform IDs.
	}
	platformId, ok := platformIdMap[platformEnv]
	if !ok {
		return 0, errors.New(fmt.Sprintf("Invalid PLATFORM_ENV='%s'", platformEnv))
	}
	return platformId, nil
}

func firstByteOpenCurlyBrace(body string) bool {
	if len(body) == 0 {
		return false
	}
	return body[0] == '{'
}

func Handler(ctx context.Context, request events.APIGatewayProxyRequest) (response *apigw.APIResponse, err error) {
	logRequest := &event.LogRequest{}

	// Just rely on binary protos to be base64 encoded.
	// If the first character is not a open curly brace, try to base64 decode it.
	// Don't futz w/ content-type headers.
	// TODO: We'll also not futz w/ binary responses for now.
	isBinaryProto := request.IsBase64Encoded || !firstByteOpenCurlyBrace(request.Body)
	if isBinaryProto {
		log.WithFields(log.Fields{"b64_body": request.Body}).Debug("isBase64Encoded")
		var bytes []byte
		if bytes, err = b64.StdEncoding.DecodeString(request.Body); err == nil {
			err = proto.Unmarshal(bytes, logRequest)
		}
	} else {
		err = protojson.Unmarshal([]byte(request.Body), logRequest)
	}
	if err != nil {
		log.Errorf("Error: %s, request: %+v\n", err.Error(), request)
		return apigw.Err(err)
	}

	shouldTextLogThisRequest := shouldTextLogRequest && !containsLoadTestTrafficType(logRequest)
	if shouldTextLogThisRequest && !isBinaryProto {
		// Log the request JSON in dev.
		log.Infof("handle APIGatewayProxyRequest; request.Body: %s\n", request.Body)
	}

	err = batchLogger.log(ctx, logRequest)
	if err != nil {
		log.Errorf("Error: %s, logRequest=%s\n", err.Error(), tryToJsonString(logRequest))
		return apigw.Err(err)
	}

	var eventResponse EventResponse
	responseHeaders := apigw.Headers{
		"Content-Type": "application/json",
	}
	response, err = apigw.ResponseWithHeaders(eventResponse, http.StatusOK, responseHeaders)
	return
}

func (p *batchLogLogger) log(ctx context.Context, logRequest *event.LogRequest) error {
	// Skip logging for LOAD_TEST.
	shouldTextLogThisRequest := shouldTextLogRequest && !containsLoadTestTrafficType(logRequest)
	if shouldTextLogThisRequest {
		log.Infof("log: %s\n", tryToJsonString(logRequest))
	} else {
		recordIds := newRecordsId()
		recordIds.collectLogRequestFields(logRequest)
		log.WithFields(recordIds.getLogFields()).Info("Start processing LogRequest")
	}
	if err := prepare(p.platformId, getNowMillis(), logRequest); err != nil {
		return err
	}
	if shouldTextLogThisRequest {
		log.Infof("validated log: %s\n", tryToJsonString(logRequest))
	}
	return p.writeLogRequest(ctx, logRequest)
}

func containsLoadTestTrafficType(logRequest *event.LogRequest) bool {
	if logRequest.GetClientInfo().GetTrafficType() == common.ClientInfo_LOAD_TEST {
		return true
	}
	for _, deliveryLog := range logRequest.GetDeliveryLog() {
		if deliveryLog.GetRequest().GetClientInfo().GetTrafficType() == common.ClientInfo_LOAD_TEST {
			return true
		}
	}
	return false
}

func getNowMillis() uint64 {
	return uint64(Now().UnixNano() / 1000000)
}

// LogRequest could write for multiple logUserIds in a single batch.
func (p *batchLogLogger) writeLogRequest(ctx context.Context, logRequest *event.LogRequest) error {
	err := p.writer.WriteLogRequest(ctx, logRequest)
	if err != nil {
		// TODO - when we're confident in this, switch to return an error
		log.Errorf("Failed to WriteLogRequest: %s\n", err.Error())
		return err
	}
	return nil
}
