package main // "github.com/promotedai/metrics/api/main"

import (
	"sort"

	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
	log "github.com/sirupsen/logrus"
)

type recordIds struct {
	// Sorting for tests.
	sort                 bool
	platformIds          map[uint64]bool
	userIds              map[string]bool
	logUserIds           map[string]bool
	membershipIds        map[string]bool
	sessionIds           map[string]bool
	viewIds              map[string]bool
	requestIds           map[string]bool
	clientRequestIds     map[string]bool
	responseInsertionIds map[string]bool
	impressionIds        map[string]bool
	actionIds            map[string]bool
}

func newRecordsId() recordIds {
	return recordIds{
		sort:                 false,
		platformIds:          make(map[uint64]bool),
		userIds:              make(map[string]bool),
		logUserIds:           make(map[string]bool),
		membershipIds:        make(map[string]bool),
		sessionIds:           make(map[string]bool),
		viewIds:              make(map[string]bool),
		requestIds:           make(map[string]bool),
		clientRequestIds:     make(map[string]bool),
		responseInsertionIds: make(map[string]bool),
		impressionIds:        make(map[string]bool),
		actionIds:            make(map[string]bool),
	}
}

func (r *recordIds) getLogFields() log.Fields {
	fields := log.Fields{}
	if len(r.platformIds) > 0 {
		platformIdSlice := make([]uint64, 0, len(r.platformIds))
		for platformId := range r.platformIds {
			platformIdSlice = append(platformIdSlice, platformId)
		}
		fields["platformIds"] = r.processSlice(platformIdSlice)
	}
	if len(r.userIds) > 0 {
		fields["userIds"] = r.processKeys(r.userIds)
	}
	if len(r.logUserIds) > 0 {
		fields["logUserIds"] = r.processKeys(r.logUserIds)
	}
	if len(r.membershipIds) > 0 {
		fields["membershipIds"] = r.processKeys(r.membershipIds)
	}
	if len(r.sessionIds) > 0 {
		fields["sessionIds"] = r.processKeys(r.sessionIds)
	}
	if len(r.viewIds) > 0 {
		fields["viewIds"] = r.processKeys(r.viewIds)
	}
	if len(r.requestIds) > 0 {
		fields["requestIds"] = r.processKeys(r.requestIds)
	}
	if len(r.clientRequestIds) > 0 {
		fields["clientRequestIds"] = r.processKeys(r.clientRequestIds)
	}
	if len(r.responseInsertionIds) > 0 {
		fields["responseInsertionIds"] = r.processKeys(r.responseInsertionIds)
	}
	if len(r.impressionIds) > 0 {
		fields["impressionIds"] = r.processKeys(r.impressionIds)
	}
	if len(r.actionIds) > 0 {
		fields["actionIds"] = r.processKeys(r.actionIds)
	}
	return fields
}

// Copied/modified from Golang's strings.Join.
func (r *recordIds) processKeys(elems map[string]bool) []string {
	var keys []string
	for k := range elems {
		keys = append(keys, k)
	}
	if r.sort {
		sort.Strings(keys)
	}
	return keys
}

// Copied/modified from Golang's strings.Join.
func (r *recordIds) processSlice(l []uint64) []uint64 {
	if r.sort {
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	}
	return l
}

func (r *recordIds) collectLogRequestFields(logRequest *event.LogRequest) {
	r.collectPlatformId(logRequest.GetPlatformId())
	r.collectUserIds(logRequest.GetUserInfo())

	for _, user := range logRequest.GetUser() {
		r.collectUserFields(user)
	}

	for _, cohortMembership := range logRequest.GetCohortMembership() {
		r.collectCohortMembershipFields(cohortMembership)
	}

	for _, sessionProfile := range logRequest.GetSessionProfile() {
		r.collectSessionProfileFields(sessionProfile)
	}

	for _, session := range logRequest.GetSession() {
		r.collectSessionFields(session)
	}

	for _, view := range logRequest.GetView() {
		r.collectViewFields(view)
	}

	for _, deliveryLog := range logRequest.GetDeliveryLog() {
		r.collectDeliveryLogFields(deliveryLog)
	}

	for _, request := range logRequest.GetRequest() {
		r.collectRequestFields(request)
	}

	for _, responseInsertion := range logRequest.GetInsertion() {
		r.collectResponseInsertionFields(responseInsertion)
	}

	for _, impression := range logRequest.GetImpression() {
		r.collectImpressionFields(impression)
	}

	for _, action := range logRequest.GetAction() {
		r.collectActionFields(action)
	}

	for _, diagnostics := range logRequest.GetDiagnostics() {
		r.collectDiagnosticsFields(diagnostics)
	}
}

func (r *recordIds) collectPlatformId(platformId uint64) {
	if platformId > 0 {
		r.platformIds[platformId] = true
	}
}

func (r *recordIds) collectUserIds(userInfo *common.UserInfo) {
	if userInfo.GetUserId() != "" {
		r.userIds[userInfo.GetUserId()] = true
	}
	if userInfo.GetLogUserId() != "" {
		r.logUserIds[userInfo.GetLogUserId()] = true
	}
}

func (r *recordIds) collectMembershipId(membershipId string) {
	if membershipId != "" {
		r.membershipIds[membershipId] = true
	}
}

func (r *recordIds) collectSessionId(sessionId string) {
	if sessionId != "" {
		r.sessionIds[sessionId] = true
	}
}

func (r *recordIds) collectViewId(viewId string) {
	if viewId != "" {
		r.viewIds[viewId] = true
	}
}

func (r *recordIds) collectRequestId(requestId string) {
	if requestId != "" {
		r.requestIds[requestId] = true
	}
}

func (r *recordIds) collectClientRequestId(clientRequestId string) {
	if clientRequestId != "" {
		r.clientRequestIds[clientRequestId] = true
	}
}

func (r *recordIds) collectResponseInsertionId(responseInsertionId string) {
	if responseInsertionId != "" {
		r.responseInsertionIds[responseInsertionId] = true
	}
}

func (r *recordIds) collectImpressionId(impressionId string) {
	if impressionId != "" {
		r.impressionIds[impressionId] = true
	}
}

func (r *recordIds) collectActionId(actionIds string) {
	if actionIds != "" {
		r.actionIds[actionIds] = true
	}
}

func (r *recordIds) collectUserFields(user *event.User) {
	r.collectPlatformId(user.GetPlatformId())
	r.collectUserIds(user.GetUserInfo())
}

func (r *recordIds) collectCohortMembershipFields(cohortMembershir *event.CohortMembership) {
	r.collectPlatformId(cohortMembershir.GetPlatformId())
	r.collectUserIds(cohortMembershir.GetUserInfo())
	r.collectMembershipId(cohortMembershir.GetMembershipId())
}

func (r *recordIds) collectSessionProfileFields(sessionProfile *event.SessionProfile) {
	r.collectPlatformId(sessionProfile.GetPlatformId())
	r.collectUserIds(sessionProfile.GetUserInfo())
	r.collectSessionId(sessionProfile.GetSessionId())
}

func (r *recordIds) collectSessionFields(session *event.Session) {
	r.collectPlatformId(session.GetPlatformId())
	r.collectUserIds(session.GetUserInfo())
	r.collectSessionId(session.GetSessionId())
}

func (r *recordIds) collectViewFields(view *event.View) {
	r.collectPlatformId(view.GetPlatformId())
	r.collectUserIds(view.GetUserInfo())
	r.collectSessionId(view.GetSessionId())
	r.collectViewId(view.GetViewId())
}

func (r *recordIds) collectDeliveryLogFields(deliveryLog *delivery.DeliveryLog) {
	r.collectPlatformId(deliveryLog.GetPlatformId())
	r.collectRequestFields(deliveryLog.GetRequest())
}

func (r *recordIds) collectRequestFields(request *delivery.Request) {
	r.collectPlatformId(request.GetPlatformId())
	r.collectUserIds(request.GetUserInfo())
	r.collectSessionId(request.GetSessionId())
	r.collectViewId(request.GetViewId())
	r.collectRequestId(request.GetRequestId())
	r.collectClientRequestId(request.GetClientRequestId())
}

func (r *recordIds) collectResponseInsertionFields(responseInsertion *delivery.Insertion) {
	r.collectPlatformId(responseInsertion.GetPlatformId())
	r.collectUserIds(responseInsertion.GetUserInfo())
	r.collectSessionId(responseInsertion.GetSessionId())
	r.collectViewId(responseInsertion.GetViewId())
	r.collectRequestId(responseInsertion.GetRequestId())
	r.collectResponseInsertionId(responseInsertion.GetInsertionId())
}

func (r *recordIds) collectImpressionFields(impression *event.Impression) {
	r.collectPlatformId(impression.GetPlatformId())
	r.collectUserIds(impression.GetUserInfo())
	r.collectSessionId(impression.GetSessionId())
	r.collectViewId(impression.GetViewId())
	r.collectRequestId(impression.GetRequestId())
	r.collectResponseInsertionId(impression.GetInsertionId())
	r.collectImpressionId(impression.GetImpressionId())
}

func (r *recordIds) collectActionFields(action *event.Action) {
	r.collectPlatformId(action.GetPlatformId())
	r.collectUserIds(action.GetUserInfo())
	r.collectSessionId(action.GetSessionId())
	r.collectViewId(action.GetViewId())
	r.collectRequestId(action.GetRequestId())
	r.collectResponseInsertionId(action.GetInsertionId())
	r.collectImpressionId(action.GetImpressionId())
	r.collectActionId(action.GetActionId())
}

func (r *recordIds) collectDiagnosticsFields(diagnostics *event.Diagnostics) {
	r.collectPlatformId(diagnostics.GetPlatformId())
	r.collectUserIds(diagnostics.GetUserInfo())
}
