package main // "github.com/promotedai/metrics/api/main"

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/promotedai/metrics/api/main/pool"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"

	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const millisInSecond = 1000
const nsInSecond = 1000000000

// The max compressed size of a record.  It's a little less than 1mb.
// We currently use the Kafka default message.max.bytes.
// https://kafka.apache.org/0110/documentation.html#brokerconfigs
const MAX_COMPRESSED_MESSAGE_BYTES = 1000012

// Since kafka-go does not expose a way to cap based on the compressed bytes,
// we'll put a safety limit in our code for uncompressed bytes.
// When looking at a sampled LogRequest, we were able to get a 3.8x compression ratio.
// For now, just increase the mulipltiplier to 2x.
const UNCOMPRESSED_MULTIPLIER = 2
const MAX_UNCOMPRESSED_MESSAGE_BYTES = UNCOMPRESSED_MULTIPLIER * MAX_COMPRESSED_MESSAGE_BYTES

// This is a number of KafkaWriters (per type) to keep around.  These don't get pre-allocated.
// It's called weirdPoolSize because it doesn't match normal pool parameters.
const poolReleaseMinSize = 5

// A fallback in case we want to change this.  The default is Gzip.
var compressionCodec = parseCompressionCodec(getEnvOrDefault("COMPRESSION", "GZIP"))

func parseCompressionCodec(compressionType string) kafka.CompressionCodec {
	switch compressionType {
	case "GZIP":
		return kafka.Gzip.Codec()
	case "NIL":
		return nil
	case "ZSTD":
		return kafka.Zstd.Codec()
	default:
		panic(fmt.Sprintf("Unsupported compressionCodec: '%s'", compressionType))
	}
}

func toTime(millis int64) time.Time {
	return time.UnixMilli(millis).UTC()
}

//
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaBatchLogWriter struct {
	userWriterPool              pool.Pool
	cohortMembershipWriterPool  pool.Pool
	sessionProfileWriterPool    pool.Pool
	sessionWriterPool           pool.Pool
	viewWriterPool              pool.Pool
	autoViewWriterPool          pool.Pool
	requestWriterPool           pool.Pool
	fullRequestWriterPool       pool.Pool
	responseInsertionWriterPool pool.Pool
	deliveryLogWriterPool       pool.Pool
	impressionWriterPool        pool.Pool
	actionWriterPool            pool.Pool
	diagnosticsWriterPool       pool.Pool
	logRequestWriterPool        pool.Pool
}

func newTlsConfig(tlsPem string) (*tls.Config, error) {
	if tlsPem != "" {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(tlsPem)) {
			return nil, errors.New("Kafka TLS Credentials error: failed to append certificates from PEM")
		}
		return &tls.Config{
			InsecureSkipVerify: false,
			RootCAs:            cp,
		}, nil
	} else {
		return nil, nil
	}
}

// TODO - can I reuse Kafka Dialer across topics?  I tried but this failed.
func newKafkaDialer(tlsConfig *tls.Config) (*kafka.Dialer, error) {
	return &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true,
		TLS:       tlsConfig,
	}, nil
}

// The kafka-go writer sucks for use in Lambda.  We have to micro optimize
func newKafkaWriter(topic string, brokers []string, tlsConfig *tls.Config, batchTimeout time.Duration) (*kafka.Writer, error) {
	dialer, err := newKafkaDialer(tlsConfig)
	if err != nil {
		return nil, err
	}
	// When we use AWS Lambda, we cannot execute any code after the Lambda finishes.
	// For now, send the event to Kafka synchronously.  This will add latency to the Event API call.
	// We can optimize this later (make the whole Event API call to be asynchronous, switch off Lambda or fire a
	// separate Lambda event).
	//
	// TODO - optimize these settings.
	// https://github.com/segmentio/kafka-go
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Dialer:       dialer,
		WriteTimeout: 3 * time.Second,
		// TODO - what balancer should we use?
		Balancer:         kafka.Murmur2Balancer{},
		BatchSize:        100,
		CompressionCodec: kafka.Gzip.Codec(),
		// The current library flushes messages at a max delay of BatchTimeout.
		BatchTimeout: batchTimeout,
		Async:        false,
		// TODO - do we want to change RequiredAcks?
	}), nil
}

// kafkaDataset - the middle part of our Kafka topic.  Used to improve multi-tenancy.
// tlsPem - an empty string disables TLS.
func NewKafkaBatchLogWriter(kafkaDataset string, brokers []string, tlsPem string, batchTimeout time.Duration) (*KafkaBatchLogWriter, error) {
	w := &KafkaBatchLogWriter{}
	userTopic := fmt.Sprintf("tracking.%s.user", kafkaDataset)
	cohortMembershipTopic := fmt.Sprintf("tracking.%s.cohort-membership", kafkaDataset)
	sessionProfileTopic := fmt.Sprintf("tracking.%s.session-profile", kafkaDataset)
	sessionTopic := fmt.Sprintf("tracking.%s.session", kafkaDataset)
	viewTopic := fmt.Sprintf("tracking.%s.view", kafkaDataset)
	autoViewTopic := fmt.Sprintf("tracking.%s.auto-view", kafkaDataset)
	fullRequestTopic := fmt.Sprintf("tracking.%s.full-request", kafkaDataset)
	requestTopic := fmt.Sprintf("tracking.%s.request", kafkaDataset)
	responseInsertionTopic := fmt.Sprintf("tracking.%s.insertion", kafkaDataset)
	deliveryLogTopic := fmt.Sprintf("tracking.%s.delivery-log", kafkaDataset)
	impressionTopic := fmt.Sprintf("tracking.%s.impression", kafkaDataset)
	actionTopic := fmt.Sprintf("tracking.%s.action", kafkaDataset)
	diagnosticsTopic := fmt.Sprintf("tracking.%s.diagnostics", kafkaDataset)
	logRequestTopic := fmt.Sprintf("tracking.%s.log-request", kafkaDataset)

	tlsConfig, err := newTlsConfig(tlsPem)
	if err != nil {
		return nil, err
	}

	newKafkaWriterPool := func(topic string) (pool.Pool, error) {
		return pool.NewPool(
			func() (interface{}, error) { return newKafkaWriter(topic, brokers, tlsConfig, batchTimeout) },
			func(r interface{}) error {
				writer := r.(KafkaWriter)
				return writer.Close()
			},
			poolReleaseMinSize)
	}

	w.userWriterPool, err = newKafkaWriterPool(userTopic)
	if err != nil {
		return nil, err
	}
	w.cohortMembershipWriterPool, err = newKafkaWriterPool(cohortMembershipTopic)
	if err != nil {
		return nil, err
	}
	w.sessionProfileWriterPool, err = newKafkaWriterPool(sessionProfileTopic)
	if err != nil {
		return nil, err
	}
	w.sessionWriterPool, err = newKafkaWriterPool(sessionTopic)
	if err != nil {
		return nil, err
	}
	w.viewWriterPool, err = newKafkaWriterPool(viewTopic)
	if err != nil {
		return nil, err
	}
	w.autoViewWriterPool, err = newKafkaWriterPool(autoViewTopic)
	if err != nil {
		return nil, err
	}
	w.fullRequestWriterPool, err = newKafkaWriterPool(fullRequestTopic)
	if err != nil {
		return nil, err
	}
	w.requestWriterPool, err = newKafkaWriterPool(requestTopic)
	if err != nil {
		return nil, err
	}
	w.responseInsertionWriterPool, err = newKafkaWriterPool(responseInsertionTopic)
	if err != nil {
		return nil, err
	}
	w.deliveryLogWriterPool, err = newKafkaWriterPool(deliveryLogTopic)
	if err != nil {
		return nil, err
	}
	w.impressionWriterPool, err = newKafkaWriterPool(impressionTopic)
	if err != nil {
		return nil, err
	}
	w.actionWriterPool, err = newKafkaWriterPool(actionTopic)
	if err != nil {
		return nil, err
	}
	w.diagnosticsWriterPool, err = newKafkaWriterPool(diagnosticsTopic)
	if err != nil {
		return nil, err
	}
	w.logRequestWriterPool, err = newKafkaWriterPool(logRequestTopic)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func toUserKeyBytes(platformId uint64, userId string) []byte {
	b := make([]byte, 8, 8+1+len(userId))
	binary.LittleEndian.PutUint64(b, platformId)
	b = append(b, []byte(userId)...)
	return b
}

// Used with defer to log latencies of methods.
// logFields can be modified during the defer.
func logMethodLatency(funcName string, logFields log.Fields) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		logFields["duration"] = duration.Milliseconds()
		log.WithFields(logFields).Infof("%s completed", funcName)
	}
}

type getPlatformIdFunc func(message proto.Message) (platformId uint64, ok bool)

type getKeyUserIdFunc func(message proto.Message) (logUserId string, ok bool)

func writeRecords(ctx context.Context, writerPool pool.Pool, methodName string, protoMessages []proto.Message, getPlatformId getPlatformIdFunc, getKeyUserId getKeyUserIdFunc) error {
	return writeRecordsWithMax(ctx, writerPool, methodName, protoMessages, getPlatformId, getKeyUserId, MAX_UNCOMPRESSED_MESSAGE_BYTES)
}

func writeRecordsWithMax(ctx context.Context, writerPool pool.Pool, methodName string, protoMessages []proto.Message, getPlatformId getPlatformIdFunc, getKeyUserId getKeyUserIdFunc, maxMessageBytes int) error {
	if len(protoMessages) == 0 {
		return nil
	}
	// For logging
	logFields := log.Fields{"inputCount": len(protoMessages)}
	defer logMethodLatency(methodName, logFields)()
	totalValueBytesLen := 0
	defer func() { logFields["valueBytesLen"] = totalValueBytesLen }()
	writtenIndex := 0
	defer func() { logFields["underMaxSizeCount"] = writtenIndex }()

	messages := make([]kafka.Message, len(protoMessages))
	for _, protoMessage := range protoMessages {
		valueBytes, err := proto.Marshal(protoMessage)
		if err != nil {
			return err
		}
		valueBytesLen := len(valueBytes)
		if valueBytesLen > maxMessageBytes {
			log.Errorf("%s hit too large record.  Not saved.  protoMessage=%s, len(valueBytes)=%d", methodName, protoMessage, valueBytesLen)
		} else {
			platformId, ok := getPlatformId(protoMessage)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to getPlatformId on %s", protoMessage))
			}
			keyUserId, ok := getKeyUserId(protoMessage)
			if !ok {
				return errors.New(fmt.Sprintf("Failed to getLogUserId on %s", protoMessage))
			}
			key := toUserKeyBytes(platformId, keyUserId)
			messages[writtenIndex].Key = key
			messages[writtenIndex].Value = valueBytes
			totalValueBytesLen += len(valueBytes)
			writtenIndex++
		}
	}
	return writeMessagesToPool(ctx, writerPool, messages[0:writtenIndex])
}

func writeMessagesToPool(ctx context.Context, pool pool.Pool, messages []kafka.Message) error {
	resource, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer pool.Release(resource)
	writer := resource.(KafkaWriter)
	return writer.WriteMessages(ctx, messages...)
}

func getPlatformIdForUser(message proto.Message) (uint64, bool) {
	user, ok := message.(*event.User)
	return user.GetPlatformId(), ok
}

func getUserIdForUser(message proto.Message) (string, bool) {
	user, ok := message.(*event.User)
	return user.GetUserInfo().GetUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteUsers(ctx context.Context, users []*event.User) error {
	messages := make([]proto.Message, len(users))
	for i, user := range users {
		messages[i] = user
	}
	return writeRecords(ctx, w.userWriterPool, "WriteUsers", messages, getPlatformIdForUser, getUserIdForUser)
}

func getPlatformIdForCohortMembership(message proto.Message) (uint64, bool) {
	// I don't know if okay is safe here.
	cohortMembership, ok := message.(*event.CohortMembership)
	return cohortMembership.GetPlatformId(), ok
}

func getLogUserIdForCohortMembership(message proto.Message) (string, bool) {
	cohortMembership, ok := message.(*event.CohortMembership)
	return cohortMembership.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteCohortMemberships(ctx context.Context, cohortMemberships []*event.CohortMembership) error {
	messages := make([]proto.Message, len(cohortMemberships))
	for i, cohortMembership := range cohortMemberships {
		messages[i] = cohortMembership
	}
	return writeRecords(ctx, w.cohortMembershipWriterPool, "WriteCohortMemberships", messages, getPlatformIdForCohortMembership, getLogUserIdForCohortMembership)
}

func getPlatformIdForSessionProfile(message proto.Message) (uint64, bool) {
	sessionProfile, ok := message.(*event.SessionProfile)
	return sessionProfile.GetPlatformId(), ok
}

func getUserIdForSessionProfile(message proto.Message) (string, bool) {
	sessionProfile, ok := message.(*event.SessionProfile)
	return sessionProfile.GetUserInfo().GetUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteSessionProfiles(ctx context.Context, sessionProfiles []*event.SessionProfile) error {
	messages := make([]proto.Message, len(sessionProfiles))
	for i, sessionProfile := range sessionProfiles {
		messages[i] = sessionProfile
	}
	return writeRecords(ctx, w.sessionProfileWriterPool, "WriteSessionProfiles", messages, getPlatformIdForSessionProfile, getUserIdForSessionProfile)
}

func getPlatformIdForSession(message proto.Message) (uint64, bool) {
	session, ok := message.(*event.Session)
	return session.GetPlatformId(), ok
}

func getLogUserIdForSession(message proto.Message) (string, bool) {
	session, ok := message.(*event.Session)
	return session.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteSessions(ctx context.Context, sessions []*event.Session) error {
	messages := make([]proto.Message, len(sessions))
	for i, session := range sessions {
		messages[i] = session
	}
	return writeRecords(ctx, w.sessionWriterPool, "WriteSessions", messages, getPlatformIdForSession, getLogUserIdForSession)
}

func getPlatformIdForView(message proto.Message) (uint64, bool) {
	view, ok := message.(*event.View)
	return view.GetPlatformId(), ok
}

func getLogUserIdForView(message proto.Message) (string, bool) {
	view, ok := message.(*event.View)
	return view.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteViews(ctx context.Context, views []*event.View) error {
	messages := make([]proto.Message, len(views))
	for i, view := range views {
		messages[i] = view
	}
	return writeRecords(ctx, w.viewWriterPool, "WriteViews", messages, getPlatformIdForView, getLogUserIdForView)
}

func getPlatformIdForAutoView(message proto.Message) (uint64, bool) {
	autoView, ok := message.(*event.AutoView)
	return autoView.GetPlatformId(), ok
}

func getLogUserIdForAutoView(message proto.Message) (string, bool) {
	autoView, ok := message.(*event.AutoView)
	return autoView.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteAutoViews(ctx context.Context, autoViews []*event.AutoView) error {
	messages := make([]proto.Message, len(autoViews))
	for i, autoView := range autoViews {
		messages[i] = autoView
	}
	return writeRecords(ctx, w.autoViewWriterPool, "WriteAutoViews", messages, getPlatformIdForAutoView, getLogUserIdForAutoView)
}

func getPlatformIdForDeliveryLog(message proto.Message) (uint64, bool) {
	deliveryLog, ok := message.(*delivery.DeliveryLog)
	return deliveryLog.GetPlatformId(), ok
}

func getLogUserIdForDeliveryLog(message proto.Message) (string, bool) {
	deliveryLog, ok := message.(*delivery.DeliveryLog)
	return deliveryLog.GetRequest().GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteDeliveryLogs(ctx context.Context, deliveryLogs []*delivery.DeliveryLog) error {
	messages := make([]proto.Message, len(deliveryLogs))
	for i, deliveryLog := range deliveryLogs {
		messages[i] = deliveryLog
	}
	return writeRecords(ctx, w.deliveryLogWriterPool, "WriteDeliveryLogs", messages, getPlatformIdForDeliveryLog, getLogUserIdForDeliveryLog)
}

func getPlatformIdForImpression(message proto.Message) (uint64, bool) {
	impression, ok := message.(*event.Impression)
	return impression.GetPlatformId(), ok
}

func getLogUserIdForImpression(message proto.Message) (string, bool) {
	impression, ok := message.(*event.Impression)
	return impression.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteImpressions(ctx context.Context, impressions []*event.Impression) error {
	messages := make([]proto.Message, len(impressions))
	for i, impression := range impressions {
		messages[i] = impression
	}
	return writeRecords(ctx, w.impressionWriterPool, "WriteImpression", messages, getPlatformIdForImpression, getLogUserIdForImpression)
}

func getPlatformIdForAction(message proto.Message) (uint64, bool) {
	action, ok := message.(*event.Action)
	return action.GetPlatformId(), ok
}

func getLogUserIdForAction(message proto.Message) (string, bool) {
	action, ok := message.(*event.Action)
	return action.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteActions(ctx context.Context, actions []*event.Action) error {
	messages := make([]proto.Message, len(actions))
	for i, action := range actions {
		messages[i] = action
	}
	return writeRecords(ctx, w.actionWriterPool, "WriteActions", messages, getPlatformIdForAction, getLogUserIdForAction)
}

func getPlatformIdForDiagnostics(message proto.Message) (uint64, bool) {
	diagnostics, ok := message.(*event.Diagnostics)
	return diagnostics.GetPlatformId(), ok
}

func getLogUserIdForDiagnostics(message proto.Message) (string, bool) {
	diagnostics, ok := message.(*event.Diagnostics)
	return diagnostics.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteDiagnostics(ctx context.Context, diagnostics []*event.Diagnostics) error {
	messages := make([]proto.Message, len(diagnostics))
	for i, diagnostic := range diagnostics {
		messages[i] = diagnostic
	}
	return writeRecords(ctx, w.diagnosticsWriterPool, "WriteDiagnostics", messages, getPlatformIdForDiagnostics, getLogUserIdForDiagnostics)
}

func (w *KafkaBatchLogWriter) Close() error {
	var err error = nil
	if userError := w.userWriterPool.Close(); userError != nil {
		err = userError
	}
	if cohortMembershipError := w.cohortMembershipWriterPool.Close(); cohortMembershipError != nil {
		err = cohortMembershipError
	}
	if sessionProfileError := w.sessionProfileWriterPool.Close(); sessionProfileError != nil {
		err = sessionProfileError
	}
	if sessionError := w.sessionWriterPool.Close(); sessionError != nil {
		err = sessionError
	}
	if viewError := w.viewWriterPool.Close(); viewError != nil {
		err = viewError
	}
	if autoViewError := w.autoViewWriterPool.Close(); autoViewError != nil {
		err = autoViewError
	}
	if requestError := w.requestWriterPool.Close(); requestError != nil {
		err = requestError
	}
	if responseInsertionError := w.responseInsertionWriterPool.Close(); responseInsertionError != nil {
		err = responseInsertionError
	}
	if deliveryLogError := w.deliveryLogWriterPool.Close(); deliveryLogError != nil {
		err = deliveryLogError
	}
	if impressionError := w.impressionWriterPool.Close(); impressionError != nil {
		err = impressionError
	}
	if actionError := w.actionWriterPool.Close(); actionError != nil {
		err = actionError
	}
	if diagnosticsError := w.diagnosticsWriterPool.Close(); diagnosticsError != nil {
		err = diagnosticsError
	}
	return err
}

func getPlatformIdForLogRequest(message proto.Message) (uint64, bool) {
	logRequest, ok := message.(*event.LogRequest)
	return logRequest.GetPlatformId(), ok
}

func getLogUserIdForLogRequest(message proto.Message) (string, bool) {
	logRequest, ok := message.(*event.LogRequest)
	return logRequest.GetUserInfo().GetLogUserId(), ok
}

func (w *KafkaBatchLogWriter) WriteLogRequest(ctx context.Context, logRequest *event.LogRequest) error {
	// We want the LogRequest kafka topic to be keyed by log_user_id so we split the LogRequest.
	// This splitting could become a problem down the road if we want to check LogRequests with clients.  TBD on how to solve this.
	logRequests, err := prepareLogRequestTopicBatches(logRequest)
	if err != nil {
		// If our pre-processing has any errors, do not write any of the LogRequest records.
		return err
	}
	var allErrors error
	for _, logRequest := range logRequests {
		messages := make([]proto.Message, 1)
		messages[0] = logRequest
		err := writeRecords(ctx, w.logRequestWriterPool, "WriteLogRequest", messages, getPlatformIdForLogRequest, getLogUserIdForLogRequest)
		if err != nil {
			allErrors = multierror.Append(allErrors, err)
		}
	}
	return allErrors
}
