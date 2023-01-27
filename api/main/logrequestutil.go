package main // "github.com/promotedai/metrics/api/main"

import (
	"errors"

	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
)

// prepareLogRequestBatches takes a LogRequest and splits/prepares it for writing to the LogRequest Kafka topic.
// We want LogRequest to be keyed by logUserId.  This code handles two corner cases:
// 1. If a LogRequest refers to multiple logUserIds.
// 2. If the LogRequest.UserInfo.LogUserId is not set directly but child records are set.
func prepareLogRequestTopicBatches(logRequest *event.LogRequest) (resultLogRequest []*event.LogRequest, err error) {
	// Faster short cut for most common case (only one key in the batch).
	if allRecordsHaveSameLogUserId(logRequest) {
		resultLogRequest = []*event.LogRequest{logRequest}
	} else {
		resultLogRequest = innerSplitLogRequestByKeys(logRequest)
	}
	err = pushUpCommonFieldsInList(resultLogRequest)
	if err != nil {
		resultLogRequest = nil
	}
	return
}

// This is used to perform a quick check to see if we should split at all.
func allRecordsHaveSameLogUserId(logRequest *event.LogRequest) bool {
	// Assume we usually are batched to the same
	// Maybe run through and do a simple check.
	seenLogUserIds := make(map[string]bool)
	seenLogUserIds[logRequest.GetUserInfo().GetLogUserId()] = true

	for _, user := range logRequest.GetUser() {
		seenLogUserIds[user.GetUserInfo().GetLogUserId()] = true
	}

	for _, cohortMembership := range logRequest.GetCohortMembership() {
		seenLogUserIds[cohortMembership.GetUserInfo().GetLogUserId()] = true
	}

	for _, sessionProfile := range logRequest.GetSessionProfile() {
		seenLogUserIds[sessionProfile.GetUserInfo().GetLogUserId()] = true
	}

	for _, session := range logRequest.GetSession() {
		seenLogUserIds[session.GetUserInfo().GetLogUserId()] = true
	}

	for _, view := range logRequest.GetView() {
		seenLogUserIds[view.GetUserInfo().GetLogUserId()] = true
	}

	for _, deliveryLog := range logRequest.GetDeliveryLog() {
		seenLogUserIds[deliveryLog.GetRequest().GetUserInfo().GetLogUserId()] = true
	}

	for _, request := range logRequest.GetRequest() {
		seenLogUserIds[request.GetUserInfo().GetLogUserId()] = true
	}

	for _, responseInsertion := range logRequest.GetInsertion() {
		seenLogUserIds[responseInsertion.GetUserInfo().GetLogUserId()] = true
	}

	for _, impression := range logRequest.GetImpression() {
		seenLogUserIds[impression.GetUserInfo().GetLogUserId()] = true
	}

	for _, action := range logRequest.GetAction() {
		seenLogUserIds[action.GetUserInfo().GetLogUserId()] = true
	}

	for _, diagnostics := range logRequest.GetDiagnostics() {
		seenLogUserIds[diagnostics.GetUserInfo().GetLogUserId()] = true
	}

	_, hasEmptyLogUserId := seenLogUserIds[""]
	return len(seenLogUserIds) <= 1 || (len(seenLogUserIds) == 2 && hasEmptyLogUserId)
}

// A "get or create" method to simplify result building.
func getOrCreateLogRequestBy(m map[string]*event.LogRequest, baseLogRequest *event.LogRequest, userInfo *common.UserInfo) *event.LogRequest {
	logRequest, has := m[userInfo.GetLogUserId()]
	if !has {
		logRequest = &event.LogRequest{}
		logRequest.PlatformId = baseLogRequest.PlatformId
		logRequest.UserInfo = userInfo
		logRequest.Timing = baseLogRequest.Timing
		m[userInfo.GetLogUserId()] = logRequest
	}
	return logRequest
}

// Does the splitting work.
func innerSplitLogRequestByKeys(logRequest *event.LogRequest) []*event.LogRequest {

	// Assume we usually are batched to the same
	// Maybe run through and do a simple check.
	logUserIdToLogRequest := make(map[string]*event.LogRequest)

	// The ordering impacts whether UserId appears on the split LogRequest.
	for _, user := range logRequest.GetUser() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, user.GetUserInfo())
		lr.User = append(lr.GetUser(), user)
	}

	for _, cohortMembership := range logRequest.GetCohortMembership() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, cohortMembership.GetUserInfo())
		lr.CohortMembership = append(lr.GetCohortMembership(), cohortMembership)
	}

	for _, sessionProfile := range logRequest.GetSessionProfile() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, sessionProfile.GetUserInfo())
		lr.SessionProfile = append(lr.GetSessionProfile(), sessionProfile)
	}

	for _, session := range logRequest.GetSession() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, session.GetUserInfo())
		lr.Session = append(lr.GetSession(), session)
	}

	for _, view := range logRequest.GetView() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, view.GetUserInfo())
		lr.View = append(lr.GetView(), view)
	}

	for _, deliveryLog := range logRequest.GetDeliveryLog() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, deliveryLog.GetRequest().GetUserInfo())
		lr.DeliveryLog = append(lr.GetDeliveryLog(), deliveryLog)
	}

	for _, request := range logRequest.GetRequest() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, request.GetUserInfo())
		lr.Request = append(lr.GetRequest(), request)
	}

	for _, responseInsertion := range logRequest.GetInsertion() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, responseInsertion.GetUserInfo())
		lr.Insertion = append(lr.GetInsertion(), responseInsertion)
	}

	for _, impression := range logRequest.GetImpression() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, impression.GetUserInfo())
		lr.Impression = append(lr.GetImpression(), impression)
	}

	for _, action := range logRequest.GetAction() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, action.GetUserInfo())
		lr.Action = append(lr.GetAction(), action)
	}

	for _, diagnostics := range logRequest.GetDiagnostics() {
		lr := getOrCreateLogRequestBy(logUserIdToLogRequest, logRequest, diagnostics.GetUserInfo())
		lr.Diagnostics = append(lr.GetDiagnostics(), diagnostics)
	}

	logRequests := make([]*event.LogRequest, 0, len(logUserIdToLogRequest))
	for _, lr := range logUserIdToLogRequest {
		logRequests = append(logRequests, lr)
	}
	return logRequests
}

// pushUpCommonFieldsInList will fill in top-level LogRequest fields (if missing) based on
// the record specified fields.
func pushUpCommonFieldsInList(logRequests []*event.LogRequest) error {
	for _, logRequest := range logRequests {
		err := pushUpCommonFields(logRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

func pushUpCommonFields(logRequest *event.LogRequest) error {
	if logRequest.GetUserInfo() == nil {
		// TODO - clone?
		logRequest.UserInfo = getUserInfo(logRequest)
	}
	if logRequest.GetPlatformId() == 0 {
		return errors.New("logRequest.PlatformId is not set but should be.")
	}
	if logRequest.GetTiming() == nil {
		return errors.New("logRequest.Timing is not set but should be.")
	}
	return nil
}

func getUserInfo(logRequest *event.LogRequest) *common.UserInfo {
	for _, user := range logRequest.GetUser() {
		if user.GetUserInfo() != nil {
			return user.GetUserInfo()
		}
	}

	for _, cohortMembership := range logRequest.GetCohortMembership() {
		if cohortMembership.GetUserInfo() != nil {
			return cohortMembership.GetUserInfo()
		}
	}

	for _, sessionProfile := range logRequest.GetSessionProfile() {
		if sessionProfile.GetUserInfo() != nil {
			return sessionProfile.GetUserInfo()
		}
	}

	for _, session := range logRequest.GetSession() {
		if session.GetUserInfo() != nil {
			return session.GetUserInfo()
		}
	}

	for _, view := range logRequest.GetView() {
		if view.GetUserInfo() != nil {
			return view.GetUserInfo()
		}
	}

	for _, deliveryLog := range logRequest.GetDeliveryLog() {
		if deliveryLog.GetRequest().GetUserInfo() != nil {
			return deliveryLog.GetRequest().GetUserInfo()
		}
	}

	for _, request := range logRequest.GetRequest() {
		if request.GetUserInfo() != nil {
			return request.GetUserInfo()
		}
	}

	for _, responseInsertion := range logRequest.GetInsertion() {
		if responseInsertion.GetUserInfo() != nil {
			return responseInsertion.GetUserInfo()
		}
	}

	for _, impression := range logRequest.GetImpression() {
		if impression.GetUserInfo() != nil {
			return impression.GetUserInfo()
		}
	}

	for _, action := range logRequest.GetAction() {
		if action.GetUserInfo() != nil {
			return action.GetUserInfo()
		}
	}

	for _, diagnostics := range logRequest.GetDiagnostics() {
		if diagnostics.GetUserInfo() != nil {
			return diagnostics.GetUserInfo()
		}
	}

	return nil
}
