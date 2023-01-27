package main // "github.com/promotedai/metrics/api/main"

import (
	"errors"
	"testing"

	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestPrepareLogRequestTopicBatches_Empty(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("2", "12345"),
		Timing:     createTiming(1234567890, 0),
	}
	actual, err := prepareLogRequestTopicBatches(input)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(actual))
	AssertProtoEquals(t, input, actual[0])
}

func TestPrepareLogRequestTopicBatches_AllSame(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("2", "12345"),
		Timing:     createTiming(1234567890, 0),
		User: []*event.User{
			&event.User{
				UserInfo: createUserInfo("2", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		CohortMembership: []*event.CohortMembership{
			&event.CohortMembership{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		SessionProfile: []*event.SessionProfile{
			&event.SessionProfile{
				UserInfo: createUserInfo("2", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		Session: []*event.Session{
			&event.Session{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		View: []*event.View{
			&event.View{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				Request: &delivery.Request{
					UserInfo: createUserInfo("", "12345"),
					Timing:   createTiming(1234567890, 0),
				},
			},
		},
		Request: []*delivery.Request{
			&delivery.Request{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		Insertion: []*delivery.Insertion{
			&delivery.Insertion{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		Impression: []*event.Impression{
			&event.Impression{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		Action: []*event.Action{
			&event.Action{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
		Diagnostics: []*event.Diagnostics{
			&event.Diagnostics{
				UserInfo: createUserInfo("", "12345"),
				Timing:   createTiming(1234567890, 0),
			},
		},
	}
	actual, err := prepareLogRequestTopicBatches(input)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(actual))
	AssertProtoEquals(t, input, actual[0])
}

func TestPrepareLogRequestTopicBatches_ChildFieldsEmpty(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("2", "12345"),
		Timing:     createTiming(1234567890, 0),
		User: []*event.User{
			&event.User{},
		},
		CohortMembership: []*event.CohortMembership{
			&event.CohortMembership{},
		},
		SessionProfile: []*event.SessionProfile{
			&event.SessionProfile{},
		},
		Session: []*event.Session{
			&event.Session{},
		},
		View: []*event.View{
			&event.View{},
		},
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				Request: &delivery.Request{},
			},
		},
		Request: []*delivery.Request{
			&delivery.Request{},
		},
		Insertion: []*delivery.Insertion{
			&delivery.Insertion{},
		},
		Impression: []*event.Impression{
			&event.Impression{},
		},
		Action: []*event.Action{
			&event.Action{},
		},
		Diagnostics: []*event.Diagnostics{
			&event.Diagnostics{},
		},
	}
	actual, err := prepareLogRequestTopicBatches(input)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(actual))
	AssertProtoEquals(t, input, actual[0])
}

func TestPrepareLogRequestTopicBatches_error_noPlatformId(t *testing.T) {
	input := &event.LogRequest{
		UserInfo: createUserInfo("2", "12345"),
		Timing:   createTiming(1234567890, 0),
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				Request: &delivery.Request{
					UserInfo: createUserInfo("", "12345"),
					Timing:   createTiming(1234567890, 0),
				},
			},
		},
	}
	actual, err := prepareLogRequestTopicBatches(input)
	assert.Equal(t, "logRequest.PlatformId is not set but should be.", err.Error())
	assert.Equal(t, 0, len(actual))
}

func TestPrepareLogRequestTopicBatches_error_noTiming(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("2", "12345"),
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				Request: &delivery.Request{
					UserInfo: createUserInfo("", "12345"),
				},
			},
		},
	}
	actual, err := prepareLogRequestTopicBatches(input)
	assert.Equal(t, "logRequest.Timing is not set but should be.", err.Error())
	assert.Equal(t, 0, len(actual))
}

func TestPrepareLogRequestTopicBatches_SplitAll_baseValuesSet(t *testing.T) {
	testPrepareLogRequestTopicBatches_SplitAll(t, createUserInfo("2", "1"), createTiming(1, 0))
}

func TestPrepareLogRequestTopicBatches_SplitAll_noUserInfoSet(t *testing.T) {
	testPrepareLogRequestTopicBatches_SplitAll(t, nil, createTiming(1, 0))
}

// The last two args should not matter.
func testPrepareLogRequestTopicBatches_SplitAll(t *testing.T, baseUserInfo *common.UserInfo, baseTiming *common.Timing) {
	input := event.LogRequest{
		PlatformId: 1,
		UserInfo:   baseUserInfo,
		Timing:     baseTiming,
		User: []*event.User{
			&event.User{
				UserInfo: createUserInfo("2", "2"),
				Timing:   createTiming(2, 0),
			},
		},
		CohortMembership: []*event.CohortMembership{
			&event.CohortMembership{
				UserInfo: createUserInfo("", "3"),
				Timing:   createTiming(3, 0),
			},
		},
		SessionProfile: []*event.SessionProfile{
			&event.SessionProfile{
				UserInfo: createUserInfo("2", "4"),
				Timing:   createTiming(4, 0),
			},
		},
		Session: []*event.Session{
			&event.Session{
				UserInfo: createUserInfo("", "5"),
				Timing:   createTiming(5, 0),
			},
		},
		View: []*event.View{
			&event.View{
				UserInfo: createUserInfo("", "6"),
				Timing:   createTiming(6, 0),
			},
		},
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				Request: &delivery.Request{
					UserInfo: createUserInfo("", "7"),
					Timing:   createTiming(7, 0),
				},
			},
		},
		Request: []*delivery.Request{
			&delivery.Request{
				UserInfo: createUserInfo("", "8"),
				Timing:   createTiming(8, 0),
			},
		},
		Insertion: []*delivery.Insertion{
			&delivery.Insertion{
				UserInfo: createUserInfo("", "9"),
				Timing:   createTiming(9, 0),
			},
		},
		Impression: []*event.Impression{
			&event.Impression{
				UserInfo: createUserInfo("", "10"),
				Timing:   createTiming(10, 0),
			},
		},
		Action: []*event.Action{
			&event.Action{
				UserInfo: createUserInfo("", "11"),
				Timing:   createTiming(11, 0),
			},
		},
		Diagnostics: []*event.Diagnostics{
			&event.Diagnostics{
				UserInfo: createUserInfo("", "12"),
				Timing:   createTiming(12, 0),
			},
		},
	}
	actualLogRequests, err := prepareLogRequestTopicBatches(&input)
	assert.Nil(t, err)
	// Does not include the LogRequest userId
	assert.Equal(t, 11, len(actualLogRequests))
	seenSet := make(map[string]bool)
	for _, actual := range actualLogRequests {
		switch logUserId := actual.GetUserInfo().GetLogUserId(); logUserId {
		case "2":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("2", "2"),
					Timing:     baseTiming,
					User: []*event.User{
						&event.User{
							UserInfo: createUserInfo("2", "2"),
							Timing:   createTiming(2, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "2")
		case "3":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "3"),
					Timing:     baseTiming,
					CohortMembership: []*event.CohortMembership{
						&event.CohortMembership{
							UserInfo: createUserInfo("", "3"),
							Timing:   createTiming(3, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "3")
		case "4":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("2", "4"),
					Timing:     baseTiming,
					SessionProfile: []*event.SessionProfile{
						&event.SessionProfile{
							UserInfo: createUserInfo("2", "4"),
							Timing:   createTiming(4, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "4")
		case "5":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "5"),
					Timing:     baseTiming,
					Session: []*event.Session{
						&event.Session{
							UserInfo: createUserInfo("", "5"),
							Timing:   createTiming(5, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "5")
		case "6":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "6"),
					Timing:     baseTiming,
					View: []*event.View{
						&event.View{
							UserInfo: createUserInfo("", "6"),
							Timing:   createTiming(6, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "6")
		case "7":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "7"),
					Timing:     baseTiming,
					DeliveryLog: []*delivery.DeliveryLog{
						&delivery.DeliveryLog{
							Request: &delivery.Request{
								UserInfo: createUserInfo("", "7"),
								Timing:   createTiming(7, 0),
							},
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "7")
		case "8":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "8"),
					Timing:     baseTiming,
					Request: []*delivery.Request{
						&delivery.Request{
							UserInfo: createUserInfo("", "8"),
							Timing:   createTiming(8, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "8")
		case "9":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "9"),
					Timing:     baseTiming,
					Insertion: []*delivery.Insertion{
						&delivery.Insertion{
							UserInfo: createUserInfo("", "9"),
							Timing:   createTiming(9, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "9")
		case "10":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "10"),
					Timing:     baseTiming,
					Impression: []*event.Impression{
						&event.Impression{
							UserInfo: createUserInfo("", "10"),
							Timing:   createTiming(10, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "10")
		case "11":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "11"),
					Timing:     baseTiming,
					Action: []*event.Action{
						&event.Action{
							UserInfo: createUserInfo("", "11"),
							Timing:   createTiming(11, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "11")
		case "12":
			AssertProtoEquals(t,
				&event.LogRequest{
					PlatformId: 1,
					UserInfo:   createUserInfo("", "12"),
					Timing:     baseTiming,
					Diagnostics: []*event.Diagnostics{
						&event.Diagnostics{
							UserInfo: createUserInfo("", "12"),
							Timing:   createTiming(12, 0),
						},
					},
				},
				actual,
			)
			markAsSeen(t, seenSet, "12")
		default:
			assert.Fail(t, "Default should not be called")
		}
	}
}

func markAsSeen(t *testing.T, seenSet map[string]bool, key string) {
	assert.False(t, seenSet[key])
	seenSet[key] = true
}

func TestPushUpCommonFields_alreadySet(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("2", "12345"),
		Timing:     createTiming(1234567890, 0),
		View: []*event.View{
			&event.View{
				PlatformId: 1,
				UserInfo:   createUserInfo("", "12345"),
				Timing:     createTiming(1234567890, 0),
			},
		},
	}
	expected := proto.Clone(input)
	assert.Nil(t, pushUpCommonFields(input))
	AssertProtoEquals(t, expected, input)
}

func TestPushUpCommonFields_platformIdNotSet(t *testing.T) {
	input := &event.LogRequest{
		View: []*event.View{
			&event.View{
				PlatformId: 1,
				UserInfo:   createUserInfo("", "12345"),
				Timing:     createTiming(1234567890, 0),
			},
		},
	}
	expected := &event.LogRequest{
		UserInfo: createUserInfo("", "12345"),
		View: []*event.View{
			&event.View{
				PlatformId: 1,
				UserInfo:   createUserInfo("", "12345"),
				Timing:     createTiming(1234567890, 0),
			},
		},
	}
	assert.Equal(t, errors.New("logRequest.PlatformId is not set but should be."), pushUpCommonFields(input))
	AssertProtoEquals(t, expected, input)
}

func TestPushUpCommonFields_timingNotSet(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		View: []*event.View{
			&event.View{
				PlatformId: 1,
				UserInfo:   createUserInfo("", "12345"),
				Timing:     createTiming(1234567890, 0),
			},
		},
	}
	expected := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("", "12345"),
		View: []*event.View{
			&event.View{
				PlatformId: 1,
				UserInfo:   createUserInfo("", "12345"),
				Timing:     createTiming(1234567890, 0),
			},
		},
	}
	assert.Equal(t, errors.New("logRequest.Timing is not set but should be."), pushUpCommonFields(input))
	AssertProtoEquals(t, expected, input)
}
