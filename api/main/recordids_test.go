package main // "github.com/promotedai/metrics/api/main"

import (
	"testing"

	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGetLogFields_empty(t *testing.T) {
	input := &event.LogRequest{}
	recordIds := newRecordsId()
	recordIds.sort = true
	recordIds.collectLogRequestFields(input)
	assert.Equal(t, log.Fields{}, recordIds.getLogFields())
}

// Make sure we don't explode.
func TestGetLogFields_emptyRecords(t *testing.T) {
	input := &event.LogRequest{
		PlatformId:       1,
		UserInfo:         createUserInfo("1", "100"),
		User:             []*event.User{&event.User{}},
		CohortMembership: []*event.CohortMembership{&event.CohortMembership{}},
		SessionProfile:   []*event.SessionProfile{&event.SessionProfile{}},
		Session:          []*event.Session{&event.Session{}},
		View:             []*event.View{&event.View{}},
		DeliveryLog:      []*delivery.DeliveryLog{&delivery.DeliveryLog{}},
		Request:          []*delivery.Request{&delivery.Request{}},
		Insertion:        []*delivery.Insertion{&delivery.Insertion{}},
		Impression:       []*event.Impression{&event.Impression{}},
		Action:           []*event.Action{&event.Action{}},
		Diagnostics:      []*event.Diagnostics{&event.Diagnostics{}},
	}

	recordIds := newRecordsId()
	recordIds.sort = true
	recordIds.collectLogRequestFields(input)
	assert.Equal(t,
		log.Fields{
			"logUserIds":  []string{"100"},
			"platformIds": []uint64{1},
			"userIds":     []string{"1"},
		},
		recordIds.getLogFields())
}

func TestGetLogFields(t *testing.T) {
	input := &event.LogRequest{
		PlatformId: 1,
		UserInfo:   createUserInfo("1", "100"),
		User: []*event.User{
			&event.User{
				PlatformId: 2,
				UserInfo:   createUserInfo("2", "101"),
			},
		},
		CohortMembership: []*event.CohortMembership{
			&event.CohortMembership{
				PlatformId:   3,
				UserInfo:     createUserInfo("", "102"),
				MembershipId: "mem1",
			},
		},
		SessionProfile: []*event.SessionProfile{
			&event.SessionProfile{
				PlatformId: 4,
				UserInfo:   createUserInfo("3", "103"),
				SessionId:  "ses1",
			},
		},
		Session: []*event.Session{
			&event.Session{
				PlatformId: 5,
				UserInfo:   createUserInfo("", "104"),
				SessionId:  "ses2",
			},
		},
		View: []*event.View{
			&event.View{
				PlatformId: 6,
				UserInfo:   createUserInfo("", "105"),
				SessionId:  "ses3",
				ViewId:     "view1",
			},
		},
		DeliveryLog: []*delivery.DeliveryLog{
			&delivery.DeliveryLog{
				PlatformId: 6,
				Request: &delivery.Request{
					UserInfo:        createUserInfo("", "106"),
					SessionId:       "ses4",
					ViewId:          "view2",
					RequestId:       "req1",
					ClientRequestId: "clientreq1",
				},
			},
		},
		Request: []*delivery.Request{
			&delivery.Request{
				PlatformId: 7,
				UserInfo:   createUserInfo("", "107"),
				SessionId:  "ses5",
				ViewId:     "view3",
				RequestId:  "req2",
			},
		},
		Insertion: []*delivery.Insertion{
			&delivery.Insertion{
				PlatformId:  8,
				UserInfo:    createUserInfo("", "108"),
				SessionId:   "ses6",
				ViewId:      "view4",
				RequestId:   "req3",
				InsertionId: "ins1",
			},
		},
		Impression: []*event.Impression{
			&event.Impression{
				PlatformId:   9,
				UserInfo:     createUserInfo("", "109"),
				SessionId:    "ses7",
				ViewId:       "view5",
				RequestId:    "req4",
				InsertionId:  "ins2",
				ImpressionId: "imp1",
			},
		},
		Action: []*event.Action{
			&event.Action{
				PlatformId:   10,
				UserInfo:     createUserInfo("", "110"),
				SessionId:    "ses8",
				ViewId:       "view6",
				RequestId:    "req5",
				InsertionId:  "ins3",
				ImpressionId: "imp2",
				ActionId:     "act1",
			},
		},
		Diagnostics: []*event.Diagnostics{
			&event.Diagnostics{
				PlatformId: 11,
				UserInfo:   createUserInfo("", "111"),
			},
		},
	}

	recordIds := newRecordsId()
	recordIds.sort = true
	recordIds.collectLogRequestFields(input)
	assert.Equal(t,
		log.Fields{
			"actionIds":            []string{"act1"},
			"impressionIds":        []string{"imp1", "imp2"},
			"logUserIds":           []string{"100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111"},
			"membershipIds":        []string{"mem1"},
			"platformIds":          []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			"requestIds":           []string{"req1", "req2", "req3", "req4", "req5"},
			"clientRequestIds":     []string{"clientreq1"},
			"responseInsertionIds": []string{"ins1", "ins2", "ins3"},
			"sessionIds":           []string{"ses1", "ses2", "ses3", "ses4", "ses5", "ses6", "ses7", "ses8"},
			"userIds":              []string{"1", "2", "3"},
			"viewIds":              []string{"view1", "view2", "view3", "view4", "view5", "view6"},
		},
		recordIds.getLogFields())
}
