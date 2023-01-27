package main // "github.com/promotedai/metrics/api/main"

import (
	"errors"
	"testing"

	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	"github.com/stretchr/testify/assert"
)

const uNowMillis2 = uint64(1583689891000)

func createUserInfo(userId, logUserId string) *common.UserInfo {
	return &common.UserInfo{
		UserId:    userId,
		LogUserId: logUserId,
	}
}

func createTiming(clientLogTimestamp, eventApiTimestamp uint64) *common.Timing {
	return &common.Timing{
		ClientLogTimestamp: clientLogTimestamp,
		EventApiTimestamp:  eventApiTimestamp,
	}
}

func createClientInfo(trafficType common.ClientInfo_TrafficType) *common.ClientInfo {
	return &common.ClientInfo{
		TrafficType: trafficType,
	}
}

func newMockPreparer(batchPlatformId uint64) *recordPreparer {
	return newRecordPreparer(batchPlatformId, uNowMillis2, mockNewUUID)
}

func mockNewUUID() string {
	return "newuuid"
}

var mockPreparer = newMockPreparer(1)

// prepare - top-level method

func TestPrepare(t *testing.T) {
	inputRequest := newTestLogRequest()
	err := prepareWithUUID(1, uNowMillis, &inputRequest, mockNewUUID)
	assert.Nil(t, err)

	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	assert.Equal(t, inputRequest, expectedRequest)
}

func TestPrepare_WithPlatformIdSet(t *testing.T) {
	inputRequest := newTestLogRequest()
	inputRequest.PlatformId = 1
	err := prepareWithUUID(1, uNowMillis, &inputRequest, mockNewUUID)
	assert.Nil(t, err)

	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	assert.Equal(t, inputRequest, expectedRequest)
}

func TestPrepare_WithoutTiming(t *testing.T) {
	inputRequest := newTestLogRequestWithTiming(nil)
	err := prepareWithUUID(1, uNowMillis, &inputRequest, mockNewUUID)
	assert.Nil(t, err)

	expectedRequest := newTestLogRequestWithTiming(nil)
	expectedRequest.PlatformId = 1
	expectedRequest.Timing = &common.Timing{
		EventApiTimestamp: uNowMillis,
	}
	assert.Equal(t, inputRequest, expectedRequest)
}

func TestPrepare_WithNoUserInfoOnLogRequest(t *testing.T) {
	inputRequest := newTestLogRequest()
	inputRequest.UserInfo = nil
	err := prepareWithUUID(1, uNowMillis, &inputRequest, mockNewUUID)
	assert.Nil(t, err)

	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 1
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	expectedRequest.UserInfo = nil
	assert.Equal(t, inputRequest, expectedRequest)
}

func TestPrepare_DefaultPlatform(t *testing.T) {
	inputRequest := newTestLogRequest()
	inputRequest.PlatformId = 0
	err := prepareWithUUID(111, uNowMillis, &inputRequest, mockNewUUID)
	assert.Nil(t, err)

	expectedRequest := newTestLogRequest()
	expectedRequest.PlatformId = 111
	expectedRequest.Timing.EventApiTimestamp = uNowMillis
	assert.Equal(t, inputRequest, expectedRequest)
}

// User

func TestSetUserFields(t *testing.T) {
	actual := &event.User{}
	assert.Nil(t, mockPreparer.setUserFields(actual))
	AssertProtoEquals(t,
		&event.User{},
		actual)
}

func TestSetUserFields_ToLower(t *testing.T) {
	actual := &event.User{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
	}
	assert.Nil(t, mockPreparer.setUserFields(actual))
	AssertProtoEquals(t,
		&event.User{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
		},
		actual)
}

func TestSetUserFields_PlatformIdError(t *testing.T) {
	actual := &event.User{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setUserFields(actual))
}

// CohortMembership

func TestSetCohortMembershipFields(t *testing.T) {
	actual := &event.CohortMembership{}
	assert.Nil(t, mockPreparer.setCohortMembershipFields(actual))
	AssertProtoEquals(t,
		&event.CohortMembership{
			MembershipId: "newuuid",
		},
		actual)
}

func TestSetCohortMembershipFields_ToLower(t *testing.T) {
	actual := &event.CohortMembership{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		MembershipId: "4F4FE",
	}
	assert.Nil(t, mockPreparer.setCohortMembershipFields(actual))
	AssertProtoEquals(t,
		&event.CohortMembership{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			MembershipId: "4f4fe",
		},
		actual)
}

func TestSetCohortMembershipFields_PlatformIdError(t *testing.T) {
	actual := &event.CohortMembership{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setCohortMembershipFields(actual))
}

// SessionProfile

func TestSetSessionProfileFields(t *testing.T) {
	actual := &event.SessionProfile{}
	assert.Nil(t, mockPreparer.setSessionProfileFields(actual))
	AssertProtoEquals(t,
		&event.SessionProfile{},
		actual)
}

func TestSetSessionProfileFields_ToLower(t *testing.T) {
	actual := &event.SessionProfile{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		SessionId: "8DSF",
	}
	assert.Nil(t, mockPreparer.setSessionProfileFields(actual))
	AssertProtoEquals(t,
		&event.SessionProfile{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			SessionId: "8dsf",
		},
		actual)
}

func TestSetSessionProfileFields_PlatformIdError(t *testing.T) {
	actual := &event.SessionProfile{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setSessionProfileFields(actual))
}

// Session

func TestSetSessionFields(t *testing.T) {
	actual := &event.Session{}
	assert.Nil(t, mockPreparer.setSessionFields(actual))
	AssertProtoEquals(t,
		&event.Session{
			SessionId: "newuuid",
		},
		actual)
}

func TestSetSessionFields_ToLower(t *testing.T) {
	actual := &event.Session{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		SessionId: "4FDS",
	}
	assert.Nil(t, mockPreparer.setSessionFields(actual))
	AssertProtoEquals(t,
		&event.Session{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			SessionId: "4fds",
		},
		actual)
}

func TestSetSessionFields_PlatformIdError(t *testing.T) {
	actual := &event.Session{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setSessionFields(actual))
}

// View

func TestSetViewFields(t *testing.T) {
	actual := &event.View{}
	assert.Nil(t, mockPreparer.setViewFields(actual))
	AssertProtoEquals(t,
		&event.View{
			ViewId:   "newuuid",
			ViewType: event.View_UNKNOWN_VIEW_TYPE,
		},
		actual)
}

func TestSetViewFields_ToLower(t *testing.T) {
	actual := &event.View{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		ViewId:    "4F4FE",
		SessionId: "DFS42-FR",
	}
	assert.Nil(t, mockPreparer.setViewFields(actual))
	AssertProtoEquals(t,
		&event.View{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			ViewId:    "4f4fe",
			SessionId: "dfs42-fr",
			ViewType:  event.View_UNKNOWN_VIEW_TYPE,
		},
		actual)
}

func TestSetViewFields_PlatformIdError(t *testing.T) {
	actual := &event.View{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setViewFields(actual))
}

func assertSetViewType(t *testing.T, actual *event.View, expected *event.View) {
	assert.Nil(t, mockPreparer.setViewFields(actual))
	expected.ViewId = "newuuid"

	AssertProtoEquals(t, expected, actual)
}

func TestSetViewType_WebPage(t *testing.T) {
	assertSetViewType(t,
		&event.View{
			UiType: &event.View_WebPageView{},
		},
		&event.View{
			ViewType: event.View_WEB_PAGE,
			UiType:   &event.View_WebPageView{},
		},
	)
}

func TestSetViewType_WebPage_OverrideBad(t *testing.T) {
	assertSetViewType(t,
		&event.View{
			ViewType: event.View_APP_SCREEN,
			UiType:   &event.View_WebPageView{},
		},
		&event.View{
			ViewType: event.View_WEB_PAGE,
			UiType:   &event.View_WebPageView{},
		},
	)
}

func TestSetViewType_AppScreen(t *testing.T) {
	assertSetViewType(t,
		&event.View{
			UiType: &event.View_AppScreenView{},
		},
		&event.View{
			ViewType: event.View_APP_SCREEN,
			UiType:   &event.View_AppScreenView{},
		},
	)
}

func TestSetViewType_AppScreen_OverrideBad(t *testing.T) {
	assertSetViewType(t,
		&event.View{
			ViewType: event.View_WEB_PAGE,
			UiType:   &event.View_AppScreenView{},
		},
		&event.View{
			ViewType: event.View_APP_SCREEN,
			UiType:   &event.View_AppScreenView{},
		},
	)
}

// AutoView

func TestSetAutoViewFields(t *testing.T) {
	actual := &event.AutoView{}
	assert.Nil(t, mockPreparer.setAutoViewFields(actual))
	AssertProtoEquals(t,
		&event.AutoView{
			AutoViewId: "newuuid",
		},
		actual)
}

func TestSetAutoViewFields_ToLower(t *testing.T) {
	actual := &event.AutoView{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		AutoViewId: "4F4FE",
		SessionId:  "DFS42-FR",
		ViewId:     "FOOBAR",
	}
	assert.Nil(t, mockPreparer.setAutoViewFields(actual))
	AssertProtoEquals(t,
		&event.AutoView{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			AutoViewId: "4f4fe",
			SessionId:  "dfs42-fr",
			ViewId:     "foobar",
		},
		actual)
}

func TestSetAutoViewFields_PlatformIdError(t *testing.T) {
	actual := &event.AutoView{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setAutoViewFields(actual))
}

// DeliveryLog

func TestSetDeliveryLogFields(t *testing.T) {
	actual := &delivery.DeliveryLog{
		Request: &delivery.Request{},
		Response: &delivery.Response{
			Insertion: []*delivery.Insertion{
				&delivery.Insertion{},
			},
		},
		Execution: &delivery.DeliveryExecution{
			ExecutionInsertion: []*delivery.Insertion{
				&delivery.Insertion{},
			},
		},
	}
	assert.Nil(t, mockPreparer.setDeliveryLogFields(actual))
	AssertProtoEquals(t,
		&delivery.DeliveryLog{
			Request: &delivery.Request{
				RequestId: "newuuid",
			},
			Response: &delivery.Response{
				Insertion: []*delivery.Insertion{
					&delivery.Insertion{
						InsertionId: "newuuid",
					},
				},
			},
			Execution: &delivery.DeliveryExecution{
				ExecutionInsertion: []*delivery.Insertion{
					&delivery.Insertion{},
				},
			},
		},
		actual)
}

func TestSetDeliveryLogFields_ToLower(t *testing.T) {
	actual := &delivery.DeliveryLog{
		Request: &delivery.Request{
			UserInfo: &common.UserInfo{
				LogUserId: "LOGUSERID1",
			},
			RequestId: "F4WF32",
			ViewId:    "F7WFEW",
			SessionId: "8W-3F-5A",
		},
		Response: &delivery.Response{
			Insertion: []*delivery.Insertion{
				&delivery.Insertion{
					InsertionId: "UUID1",
				},
			},
		},
		Execution: &delivery.DeliveryExecution{
			ExecutionInsertion: []*delivery.Insertion{
				&delivery.Insertion{
					InsertionId: "UUID1",
				},
			},
		},
	}
	assert.Nil(t, mockPreparer.setDeliveryLogFields(actual))
	AssertProtoEquals(t,
		&delivery.DeliveryLog{
			Request: &delivery.Request{
				UserInfo: &common.UserInfo{
					LogUserId: "loguserid1",
				},
				RequestId: "f4wf32",
				ViewId:    "f7wfew",
				SessionId: "8w-3f-5a",
			},
			Response: &delivery.Response{
				Insertion: []*delivery.Insertion{
					&delivery.Insertion{
						InsertionId: "uuid1",
					},
				},
			},
			Execution: &delivery.DeliveryExecution{
				ExecutionInsertion: []*delivery.Insertion{
					&delivery.Insertion{
						InsertionId: "uuid1",
					},
				},
			},
		},
		actual)
}

func TestSetDeliveryLogFields_PlatformIdError(t *testing.T) {
	actual := &delivery.DeliveryLog{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setDeliveryLogFields(actual))
}

func TestSetDeliveryLogFields_missingRequest(t *testing.T) {
	actual := &delivery.DeliveryLog{
		PlatformId: 1,
	}

	assert.Equal(t, errors.New("DeliveryLog needs a Request; deliveryLog=platform_id:1"),
		mockPreparer.setDeliveryLogFields(actual))
}

func TestSetDeliveryLogFields_noResponseOrExecution(t *testing.T) {
	actual := &delivery.DeliveryLog{
		Request: &delivery.Request{},
	}
	assert.Nil(t, mockPreparer.setDeliveryLogFields(actual))
	AssertProtoEquals(t,
		&delivery.DeliveryLog{
			Request: &delivery.Request{
				RequestId: "newuuid",
			},
		},
		actual)
}

// Request

func TestSetRequestFields(t *testing.T) {
	actual := &delivery.Request{}
	assert.Nil(t, mockPreparer.setRequestFields(actual))
	AssertProtoEquals(t,
		&delivery.Request{
			RequestId: "newuuid",
		},
		actual)
}

func TestSetRequestFields_ToLower(t *testing.T) {
	actual := &delivery.Request{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		RequestId: "F4WF32",
		ViewId:    "F7WFEW",
		SessionId: "8W-3F-5A",
	}
	assert.Nil(t, mockPreparer.setRequestFields(actual))
	AssertProtoEquals(t,
		&delivery.Request{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			RequestId: "f4wf32",
			ViewId:    "f7wfew",
			SessionId: "8w-3f-5a",
		},
		actual)
}

func TestSetRequestFields_PlatformIdError(t *testing.T) {
	actual := &delivery.Request{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setRequestFields(actual))
}

// response Insertion

func TestSetResponseInsertionFields(t *testing.T) {
	actual := &delivery.Insertion{}
	assert.Nil(t, mockPreparer.setResponseInsertionFields(actual))
	AssertProtoEquals(t,
		&delivery.Insertion{
			InsertionId: "newuuid",
		},
		actual)
}

func TestSetResponseInsertionFields_DoNotPush(t *testing.T) {
	actual := &delivery.Insertion{
		InsertionId: "otherid",
	}
	assert.Nil(t, mockPreparer.setResponseInsertionFields(actual))
	AssertProtoEquals(t,
		&delivery.Insertion{
			InsertionId: "otherid",
		},
		actual)
}

func TestSetResponseInsertionFields_ToLower(t *testing.T) {
	actual := &delivery.Insertion{
		InsertionId: "8FE",
		RequestId:   "GE42",
		SessionId:   "AB-CD-EF",
		ViewId:      "GER",
	}
	assert.Nil(t, mockPreparer.setResponseInsertionFields(actual))
	AssertProtoEquals(t,
		&delivery.Insertion{
			InsertionId: "8fe",
			RequestId:   "ge42",
			SessionId:   "ab-cd-ef",
			ViewId:      "ger",
		},
		actual)
}

func TestSetResponseInsertionFields_PlatformIdError(t *testing.T) {
	actual := &delivery.Insertion{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setResponseInsertionFields(actual))
}

// Impression

func TestSetImpressionFields(t *testing.T) {
	actual := &event.Impression{}
	assert.Nil(t, mockPreparer.setImpressionFields(actual))
	AssertProtoEquals(t,
		&event.Impression{
			ImpressionId: "newuuid",
		},
		actual)
}

func TestSetImpressionFields_ToLower(t *testing.T) {
	actual := &event.Impression{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		ImpressionId: "12345-DEF",
		InsertionId:  "12345-GHI",
		RequestId:    "12345-JKL",
		SessionId:    "12345-MNO",
		ViewId:       "12345-PQR",
	}
	assert.Nil(t, mockPreparer.setImpressionFields(actual))
	AssertProtoEquals(t,
		&event.Impression{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			ImpressionId: "12345-def",
			InsertionId:  "12345-ghi",
			RequestId:    "12345-jkl",
			SessionId:    "12345-mno",
			ViewId:       "12345-pqr",
		},
		actual)
}

func TestSetImpressionFields_PlatformIdError(t *testing.T) {
	actual := &event.Impression{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setImpressionFields(actual))
}

// Action

func TestSetActionFields(t *testing.T) {
	actual := &event.Action{}
	assert.Nil(t, mockPreparer.setActionFields(actual))
	AssertProtoEquals(t,
		&event.Action{
			ActionId: "newuuid",
		},
		actual)
}

func TestSetActionFields_ToLower(t *testing.T) {
	actual := &event.Action{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
		ActionId:     "12345-DEF",
		ImpressionId: "12345-gHI",
		InsertionId:  "12345-JKl",
		RequestId:    "12345-mNO",
		SessionId:    "12345-PqR",
		ViewId:       "12345-StU",
	}
	assert.Nil(t, mockPreparer.setActionFields(actual))
	AssertProtoEquals(t,
		&event.Action{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
			ActionId:     "12345-def",
			ImpressionId: "12345-ghi",
			InsertionId:  "12345-jkl",
			RequestId:    "12345-mno",
			SessionId:    "12345-pqr",
			ViewId:       "12345-stu",
		},
		actual)
}

func TestSetActionFields_PlatformIdError(t *testing.T) {
	actual := &event.Action{
		PlatformId: 9,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=9 batchPlatformId=1"),
		mockPreparer.setActionFields(actual))
}

// Diagnostics

func TestSetDiagnosticsFields(t *testing.T) {
	actual := &event.Diagnostics{}
	assert.Nil(t, mockPreparer.setDiagnosticsFields(actual))
	AssertProtoEquals(t,
		&event.Diagnostics{},
		actual)
}

func TestSetDiagnosticsFields_ToLower(t *testing.T) {
	actual := &event.Diagnostics{
		UserInfo: &common.UserInfo{
			LogUserId: "LOGUSERID1",
		},
	}
	assert.Nil(t, mockPreparer.setDiagnosticsFields(actual))
	AssertProtoEquals(t,
		&event.Diagnostics{
			UserInfo: &common.UserInfo{
				LogUserId: "loguserid1",
			},
		},
		actual)
}

func TestSetDiagnosticsFields_platformIdChanges(t *testing.T) {
	actual := &event.Diagnostics{
		PlatformId: 2,
	}
	assert.Equal(t, errors.New("Invalid recordPlatformId=2 batchPlatformId=1"),
		mockPreparer.setDiagnosticsFields(actual))
}
