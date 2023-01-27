package main // "github.com/promotedai/metrics/api/main"

import (
	"context"
	b64 "encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TODO - this test is very hard to update.  Golang's mock.Mock is pretty painful when combined with protobuf.
// We should move setFieldsAndValidate to a different file and then test it directly.
// The tests will be cleaner and we can use assertProtoEquals.

const uNowMillis = uint64(1583687671000)

type MockBatchLogWriter struct {
	mock.Mock
}

func (mock *MockBatchLogWriter) WriteLogRequest(ctx context.Context, logRequest *event.LogRequest) error {
	args := mock.Called(ctx, logRequest)
	return args.Error(0)
}

func (mock *MockBatchLogWriter) Close() error {
	args := mock.Called()
	return args.Error(0)
}

func clone(timing *common.Timing) *common.Timing {
	if timing == nil {
		return timing
	}
	return proto.Clone(timing).(*common.Timing)
}

func newTestLogRequest() event.LogRequest {
	return newTestLogRequestWithTiming(createTiming(uNowMillis, 0))
}

func newTestLogRequestWithTiming(timing *common.Timing) event.LogRequest {
	return event.LogRequest{
		UserInfo: createUserInfo("2", "12345"),
		Timing:   clone(timing),
		User: []*event.User{
			&event.User{},
		},
		CohortMembership: []*event.CohortMembership{
			&event.CohortMembership{
				MembershipId: "id1",
			},
		},
		SessionProfile: []*event.SessionProfile{
			&event.SessionProfile{},
		},
		Session: []*event.Session{
			&event.Session{
				SessionId: "id2",
			},
		},
		View: []*event.View{
			&event.View{
				ViewId: "id3",
			},
		},
		AutoView: []*event.AutoView{
			&event.AutoView{
				AutoViewId: "id9",
			},
		},
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				Request: &delivery.Request{
					RequestId: "id4",
				},
			},
		},
		Impression: []*event.Impression{
			&event.Impression{
				ImpressionId: "id6",
			},
		},
		Action: []*event.Action{
			&event.Action{
				ActionId: "id7",
			},
		},
		Diagnostics: []*event.Diagnostics{
			&event.Diagnostics{
				DiagnosticsMessage: &event.Diagnostics_MobileDiagnostics{
					&event.MobileDiagnostics{
						DeviceIdentifier: "id8",
					},
				},
			},
		},
	}
}

func newTestOneActionLogRequest() event.LogRequest {
	return event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("2", "12345"),
		Timing:     createTiming(uNowMillis, 0),
		Action: []*event.Action{
			&event.Action{
				PlatformId: 1,
				UserInfo:   createUserInfo("", "12345"),
				Timing:     createTiming(uNowMillis, 0),
				ActionId:   "id1",
			},
		},
	}
}

func createFullyMockedBatchLogWriter(expected *event.LogRequest) MockBatchLogWriter {
	var writer MockBatchLogWriter
	writer.On("WriteLogRequest", mock.Anything, expected).Return(nil)
	return writer
}

func createBatchLogLogger(writer *MockBatchLogWriter) batchLogLogger {
	Now = func() time.Time { return toTime(int64(uNowMillis)) }
	var batchLogLogger batchLogLogger
	batchLogLogger.writer = writer
	batchLogLogger.platformId = 1
	return batchLogLogger
}

func assertFullyMockedBatchLogWriter(t *testing.T, writer *MockBatchLogWriter, expected event.LogRequest) {
	writer.AssertCalled(t, "WriteLogRequest", mock.Anything, mock.Anything)
}

func TestProcessSuccess(t *testing.T) {
	// Use this version since platformIds get added.
	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	writer := createFullyMockedBatchLogWriter(&expectedRequest)
	batchLogLogger := createBatchLogLogger(&writer)

	inputRequest := newTestLogRequest()
	err := batchLogLogger.log(context.Background(), &inputRequest)
	assert.Nil(t, err)
	assertFullyMockedBatchLogWriter(t, &writer, expectedRequest)
}

// Keeping this test here because no logUserId could impact other parts of the handler.
func TestProcessSuccessWithNoUserInfoOnLogRequest(t *testing.T) {
	// Use this version since platformIds get added.
	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	expectedRequest.UserInfo = nil
	writer := createFullyMockedBatchLogWriter(&expectedRequest)
	batchLogLogger := createBatchLogLogger(&writer)

	inputRequest := newTestLogRequest()
	inputRequest.UserInfo = nil
	err := batchLogLogger.log(context.Background(), &inputRequest)
	assert.Nil(t, err)
	assertFullyMockedBatchLogWriter(t, &writer, expectedRequest)
}

func TestProcessWriterError(t *testing.T) {
	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1

	var writer MockBatchLogWriter
	expectedErr := errors.New("Test error")
	writer.On("WriteLogRequest", mock.Anything, mock.Anything).Return(expectedErr)
	batchLogLogger := createBatchLogLogger(&writer)

	inputRequest := newTestLogRequest()
	inputRequest.PlatformId = 1
	err := batchLogLogger.log(context.Background(), &inputRequest)
	assert.Equal(t, expectedErr, err)
	writer.AssertCalled(t, "WriteLogRequest", mock.Anything, mock.Anything)
}

func TestProcessErrorBadPlatformId(t *testing.T) {
	// Use this version since platformIds get added.
	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	writer := createFullyMockedBatchLogWriter(&expectedRequest)
	batchLogLogger := createBatchLogLogger(&writer)
	batchLogLogger.platformId = 2

	inputRequest := newTestLogRequest()
	inputRequest.PlatformId = 1
	err := batchLogLogger.log(context.Background(), &inputRequest)
	assert.Equal(t, errors.New("Invalid logRequest.PlatformId=1 defaultPlatformId=2"), err)
	writer.AssertNotCalled(t, "WriteLogRequest", mock.Anything, mock.Anything)
}

func TestInnerGetPlatformId(t *testing.T) {
	// Just make sure not nils are returned.
	platformId, err := innerGetPlatformId("pangea-prod")
	assert.Equal(t, uint64(20), platformId)
	assert.Nil(t, err)
}

func TestContainsLoadTestTrafficType_NoLoadTest(t *testing.T) {
	inputRequest := newTestLogRequest()
	assert.False(t, containsLoadTestTrafficType(&inputRequest))
}

func TestContainsLoadTestTrafficType_DifferentTrafficTypeOnDeliveryLog(t *testing.T) {
	inputRequest := newTestLogRequest()
	inputRequest.DeliveryLog[0].Request.ClientInfo = &common.ClientInfo{
		TrafficType: common.ClientInfo_PRODUCTION,
	}
	assert.False(t, containsLoadTestTrafficType(&inputRequest))
}

func TestContainsLoadTestTrafficType_LoadTestOnLogRequest(t *testing.T) {
	inputRequest := newTestLogRequest()
	inputRequest.ClientInfo = &common.ClientInfo{
		TrafficType: common.ClientInfo_LOAD_TEST,
	}
	assert.True(t, containsLoadTestTrafficType(&inputRequest))
}

func TestContainsLoadTestTrafficType_LoadTestOnDeliveryLog(t *testing.T) {
	inputRequest := newTestLogRequest()
	inputRequest.DeliveryLog[0].Request.ClientInfo = &common.ClientInfo{
		TrafficType: common.ClientInfo_LOAD_TEST,
	}
	assert.True(t, containsLoadTestTrafficType(&inputRequest))
}

func convertToBase64(t *testing.T, batchRequest event.LogRequest) string {
	out, err := proto.Marshal(&batchRequest)
	assert.Nil(t, err)
	return b64.StdEncoding.EncodeToString(out)
}

const binaryBody = "EgoKATISBTEyMzQ1GgcI2LmQ2YsuOgBCBTIDaWQxSgBSBTIDaWQyWgUyA2lkM3IFMgNpZDZ6BTIDaWQ3kgEHEgUyA2lkNLoBByoFCgNpZDjKAQUyA2lkOQ=="

func TestHandler_binaryWithIsBase64encoded(t *testing.T) {
	// Uncomment these lines to get the APIGatewayProxyRequest.Body
	// encodedRequest := convertToBase64(t, newTestLogRequest())
	// assert.Equal(t, "", encodedRequest)

	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	writer := createFullyMockedBatchLogWriter(&expectedRequest)
	batchLogger = createBatchLogLogger(&writer)

	response, err := Handler(context.Background(), events.APIGatewayProxyRequest{
		IsBase64Encoded: true,
		Body:            binaryBody,
	})
	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, 200, response.StatusCode)
	assertFullyMockedBatchLogWriter(t, &writer, expectedRequest)
}

func TestHandler_binary(t *testing.T) {
	// Uncomment these lines to get the APIGatewayProxyRequest.Body
	// encodedRequest := convertToBase64(t, newTestLogRequest())
	// assert.Equal(t, "", encodedRequest)

	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	writer := createFullyMockedBatchLogWriter(&expectedRequest)
	batchLogger = createBatchLogLogger(&writer)

	response, err := Handler(context.Background(), events.APIGatewayProxyRequest{
		Body: binaryBody,
	})
	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, 200, response.StatusCode)
	assertFullyMockedBatchLogWriter(t, &writer, expectedRequest)
}

func TestHandler_json(t *testing.T) {
	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	writer := createFullyMockedBatchLogWriter(&expectedRequest)
	batchLogger = createBatchLogLogger(&writer)

	actualRequest := newTestLogRequest()
	logRequestJson, err1 := protojson.Marshal(&actualRequest)
	assert.Nil(t, err1)
	response, err2 := Handler(context.Background(), events.APIGatewayProxyRequest{
		Body: string(logRequestJson),
	})
	assert.Nil(t, err2)
	assert.NotNil(t, response)
	assert.Equal(t, 200, response.StatusCode)
	assertFullyMockedBatchLogWriter(t, &writer, expectedRequest)
}
