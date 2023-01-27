package main // "github.com/promotedai/metrics/api/main"

import (
	"context"
	"testing"
	"time"

	"github.com/promotedai/metrics/api/main/pool"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
)

type MockKafkaWriter struct {
	mock.Mock
}

func (mock *MockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	args := mock.Called(ctx, msgs)
	return args.Error(0)
}

func (mock *MockKafkaWriter) Close() error {
	args := mock.Called()
	return args.Error(0)
}

func createTestUser(userId string) *event.User {
	return &event.User{
		PlatformId: 1,
		UserInfo:   createUserInfo("321", ""),
	}
}

func createTestSessionProfile(sessionId string) *event.SessionProfile {
	return &event.SessionProfile{
		PlatformId: 1,
		UserInfo:   createUserInfo("321", ""),
		SessionId:  sessionId,
	}
}

func createTestSession(sessionId string) *event.Session {
	return &event.Session{
		PlatformId: 1,
		UserInfo:   createUserInfo("", "12345"),
		SessionId:  sessionId,
	}
}

func createTestView(viewId string) *event.View {
	return &event.View{
		PlatformId: 1,
		UserInfo:   createUserInfo("", "12345"),
		ViewId:     viewId,
	}
}

func createTestDeliveryLog(request *delivery.Request) *delivery.DeliveryLog {
	return &delivery.DeliveryLog{
		PlatformId: 1,
		Request:    request,
	}
}

// Special function to test Request clearing.
func createTestRequest(requestId string, requestInsertions []*delivery.Insertion) *delivery.Request {
	request := &delivery.Request{
		PlatformId: 1,
		UserInfo:   createUserInfo("", "12345"),
		RequestId:  requestId,
	}
	if requestInsertions != nil {
		request.Insertion = requestInsertions
	}
	return request
}

func createTestInsertion(insertionId string) *delivery.Insertion {
	return &delivery.Insertion{
		PlatformId:  1,
		UserInfo:    createUserInfo("", "12345"),
		InsertionId: insertionId,
	}
}

func createTestImpression(impressionId string) *event.Impression {
	return &event.Impression{
		PlatformId:   1,
		UserInfo:     createUserInfo("", "12345"),
		ImpressionId: impressionId,
	}
}

func createTestAction(actionId string) *event.Action {
	return &event.Action{
		PlatformId: 1,
		UserInfo:   createUserInfo("", "12345"),
		ActionId:   actionId,
	}
}

func createTestLogRequest(impression *event.Impression) *event.LogRequest {
	return &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("", "12345"),
		Timing:     createTiming(123456789, 123456789),
		Impression: []*event.Impression{impression},
	}
}

func TestParseCompressionCodec(t *testing.T) {
	assert.NotNil(t, parseCompressionCodec("GZIP"))
	assert.Nil(t, parseCompressionCodec("NIL"))
	assert.NotNil(t, parseCompressionCodec("ZSTD"))
}

func TestToTime(t *testing.T) {
	assert.Equal(t,
		time.Date(2021, time.November, 27, 12, 0, 0, 0, time.UTC),
		toTime(1638014400000))
}

func TestWriteUser_Empty(t *testing.T) {
	var userWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		userWriterPool: &pool.MockPool{Resource: &userWriter},
	}
	assert.Nil(t, writer.WriteUsers(context.Background(), []*event.User{}))
}

func TestWriteUser(t *testing.T) {
	var userWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		userWriterPool: &pool.MockPool{Resource: &userWriter},
	}

	valueBytes, _ := proto.Marshal(createTestUser("id1"))
	userWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "321"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteUsers(context.Background(), []*event.User{
		createTestUser("id1"),
	}))
	userWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteSessionProfile_Empty(t *testing.T) {
	var sessionProfileWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		sessionProfileWriterPool: &pool.MockPool{Resource: &sessionProfileWriter},
	}
	assert.Nil(t, writer.WriteSessionProfiles(context.Background(), []*event.SessionProfile{}))
}

func TestWriteSessionProfile(t *testing.T) {
	var sessionProfileWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		sessionProfileWriterPool: &pool.MockPool{Resource: &sessionProfileWriter},
	}

	valueBytes, _ := proto.Marshal(createTestSessionProfile("id1"))
	sessionProfileWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "321"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteSessionProfiles(context.Background(), []*event.SessionProfile{
		createTestSessionProfile("id1"),
	}))
	sessionProfileWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteSession_Empty(t *testing.T) {
	var sessionWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		sessionWriterPool: &pool.MockPool{Resource: &sessionWriter},
	}
	assert.Nil(t, writer.WriteSessions(context.Background(), []*event.Session{}))
}

func TestWriteSession(t *testing.T) {
	var sessionWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		sessionWriterPool: &pool.MockPool{Resource: &sessionWriter},
	}

	valueBytes, _ := proto.Marshal(createTestSession("id1"))
	sessionWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteSessions(context.Background(), []*event.Session{
		createTestSession("id1"),
	}))
	sessionWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteView_Empty(t *testing.T) {
	var viewWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		viewWriterPool: &pool.MockPool{Resource: &viewWriter},
	}
	assert.Nil(t, writer.WriteViews(context.Background(), []*event.View{}))
}

func TestWriteView(t *testing.T) {
	var viewWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		viewWriterPool: &pool.MockPool{Resource: &viewWriter},
	}

	valueBytes, _ := proto.Marshal(createTestView("id1"))
	viewWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteViews(context.Background(), []*event.View{
		createTestView("id1"),
	}))
	viewWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteDeliveryLog_Empty(t *testing.T) {
	var deliveryLogWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		deliveryLogWriterPool: &pool.MockPool{Resource: &deliveryLogWriter},
	}
	assert.Nil(t, writer.WriteDeliveryLogs(context.Background(), []*delivery.DeliveryLog{}))
}

func TestWriteDeliveryLog(t *testing.T) {
	var deliveryLogWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		deliveryLogWriterPool: &pool.MockPool{Resource: &deliveryLogWriter},
	}
	valueBytes, _ := proto.Marshal(createTestDeliveryLog(createTestRequest("id1", []*delivery.Insertion{
		createTestInsertion("id2"),
	})))
	deliveryLogWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteDeliveryLogs(context.Background(), []*delivery.DeliveryLog{
		createTestDeliveryLog(createTestRequest("id1", []*delivery.Insertion{
			createTestInsertion("id2"),
		})),
	}))
	deliveryLogWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

// test the filtering code.
func TestWriteRecordsWithMax_Truncated(t *testing.T) {
	var deliveryLogWriter MockKafkaWriter
	deliveryLogWriterPool := pool.MockPool{Resource: &deliveryLogWriter}

	valueBytes1, _ := proto.Marshal(createTestDeliveryLog(createTestRequest("id1", []*delivery.Insertion{
		createTestInsertion("id2"),
	})))
	valueBytes5, _ := proto.Marshal(createTestDeliveryLog(createTestRequest("id5", []*delivery.Insertion{
		createTestInsertion("id6"),
	})))
	deliveryLogWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes1,
		},
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes5,
		},
	}).Return(nil)

	messages := []proto.Message{
		createTestDeliveryLog(createTestRequest("id1", []*delivery.Insertion{
			createTestInsertion("id2"),
		})),
		createTestDeliveryLog(createTestRequest("longlonglonglonglonglonglonglongid3", []*delivery.Insertion{
			createTestInsertion("id4"),
		})),
		createTestDeliveryLog(createTestRequest("id5", []*delivery.Insertion{
			createTestInsertion("id6"),
		})),
		createTestDeliveryLog(createTestRequest("longlonglonglonglonglonglonglongid7", []*delivery.Insertion{
			createTestInsertion("id8"),
		})),
	}
	assert.Nil(t, writeRecordsWithMax(context.Background(), &deliveryLogWriterPool, "WriteDeliveryLogs", messages, getPlatformIdForDeliveryLog, getLogUserIdForDeliveryLog, 50))
	deliveryLogWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteImpression_Empty(t *testing.T) {
	var impressionWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		impressionWriterPool: &pool.MockPool{Resource: &impressionWriter},
	}
	assert.Nil(t, writer.WriteImpressions(context.Background(), []*event.Impression{}))
}

func TestWriteImpression(t *testing.T) {
	var impressionWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		impressionWriterPool: &pool.MockPool{Resource: &impressionWriter},
	}

	valueBytes, _ := proto.Marshal(createTestImpression("id1"))
	impressionWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteImpressions(context.Background(), []*event.Impression{
		createTestImpression("id1"),
	}))
	impressionWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteAction_Empty(t *testing.T) {
	var actionWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		actionWriterPool: &pool.MockPool{Resource: &actionWriter},
	}
	assert.Nil(t, writer.WriteActions(context.Background(), []*event.Action{}))
}

func TestWriteAction(t *testing.T) {
	var actionWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		actionWriterPool: &pool.MockPool{Resource: &actionWriter},
	}

	valueBytes, _ := proto.Marshal(createTestAction("id1"))
	actionWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteActions(context.Background(), []*event.Action{
		createTestAction("id1"),
	}))
	actionWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteLogRequest(t *testing.T) {
	var logRequestWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		logRequestWriterPool: &pool.MockPool{Resource: &logRequestWriter},
	}
	valueBytes, _ := proto.Marshal(createTestLogRequest(createTestImpression("id1")))
	logRequestWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	assert.Nil(t, writer.WriteLogRequest(context.Background(), createTestLogRequest(createTestImpression("id1"))))
	logRequestWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}

func TestWriteLogRequest_NilUserInfo(t *testing.T) {
	var logRequestWriter MockKafkaWriter
	writer := KafkaBatchLogWriter{
		logRequestWriterPool: &pool.MockPool{Resource: &logRequestWriter},
	}
	expectedImpression := createTestImpression("id1")
	expected := createTestLogRequest(expectedImpression)
	expected.PlatformId = expectedImpression.PlatformId
	expected.UserInfo = expectedImpression.UserInfo
	valueBytes, _ := proto.Marshal(expected)
	logRequestWriter.On("WriteMessages", mock.Anything, []kafka.Message{
		kafka.Message{
			Key:   toUserKeyBytes(1, "12345"),
			Value: valueBytes,
		},
	}).Return(nil)
	input := createTestLogRequest(createTestImpression("id1"))
	input.UserInfo = nil
	assert.Nil(t, writer.WriteLogRequest(context.Background(), input))
	logRequestWriter.AssertCalled(t, "WriteMessages", mock.Anything, mock.Anything)
}
