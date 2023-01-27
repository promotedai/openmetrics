package main // "github.com/promotedai/metrics/api/main"

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/promotedai/schema-internal/generated/go/proto/common"
	"github.com/promotedai/schema-internal/generated/go/proto/delivery"
	"github.com/promotedai/schema-internal/generated/go/proto/event"
)

// Returns a new UUID string.  Must return lowercase string.
type newUUID func() string

// Prepares records for the writer: validates and fills in fields.
// Scoped to a single LogRequest
type recordPreparer struct {
	newUUID         newUUID
	nowMillis       uint64
	batchPlatformId uint64
}

// Do very simple validation on all events first.
func prepare(defaultPlatformId uint64, nowMillis uint64, logRequest *event.LogRequest) error {
	return prepareWithUUID(defaultPlatformId, nowMillis, logRequest, uuid.NewString)
}

// Factory method for testing
func prepareWithUUID(defaultPlatformId uint64, nowMillis uint64, logRequest *event.LogRequest, newUUID newUUID) error {
	// Do platformId checks here so we can set p.batchPlatformId.
	if logRequest.GetPlatformId() == 0 {
		logRequest.PlatformId = defaultPlatformId
	}
	if logRequest.GetPlatformId() == 0 {
		return errors.New("platformId needs to be specified")
	}
	if logRequest.GetPlatformId() != defaultPlatformId {
		return errors.New(fmt.Sprintf("Invalid logRequest.PlatformId=%d defaultPlatformId=%d", logRequest.GetPlatformId(), defaultPlatformId))
	}
	return newRecordPreparer(logRequest.GetPlatformId(), nowMillis, newUUID).prepare(logRequest)
}

func newRecordPreparer(batchPlatformId uint64, nowMillis uint64, newUUID newUUID) *recordPreparer {
	return &recordPreparer{
		newUUID:         newUUID,
		nowMillis:       nowMillis,
		batchPlatformId: batchPlatformId,
	}
}

// Do very simple validation on all events first.
func (p *recordPreparer) prepare(logRequest *event.LogRequest) error {
	if logRequest.GetTiming() == nil {
		logRequest.Timing = &common.Timing{}
	}
	logRequest.GetTiming().EventApiTimestamp = p.nowMillis

	for _, user := range logRequest.GetUser() {
		err := p.setUserFields(user)
		if err != nil {
			return err
		}
	}

	for _, cohortMembership := range logRequest.GetCohortMembership() {
		err := p.setCohortMembershipFields(cohortMembership)
		if err != nil {
			return err
		}
	}

	for _, sessionProfile := range logRequest.GetSessionProfile() {
		err := p.setSessionProfileFields(sessionProfile)
		if err != nil {
			return err
		}
	}

	for _, session := range logRequest.GetSession() {
		err := p.setSessionFields(session)
		if err != nil {
			return err
		}
	}

	for _, view := range logRequest.GetView() {
		err := p.setViewFields(view)
		if err != nil {
			return err
		}
	}

	for _, autoView := range logRequest.GetAutoView() {
		err := p.setAutoViewFields(autoView)
		if err != nil {
			return err
		}
	}

	for _, deliveryLog := range logRequest.GetDeliveryLog() {
		err := p.setDeliveryLogFields(deliveryLog)
		if err != nil {
			return err
		}
	}

	for _, impression := range logRequest.GetImpression() {
		err := p.setImpressionFields(impression)
		if err != nil {
			return err
		}
	}

	for _, action := range logRequest.GetAction() {
		err := p.setActionFields(action)
		if err != nil {
			return err
		}
	}

	for _, diagnostics := range logRequest.GetDiagnostics() {
		err := p.setDiagnosticsFields(diagnostics)
		if err != nil {
			return err
		}
	}

	return nil
}

// Used by fill methods to set default primary key values.
func (p *recordPreparer) toPrimaryKeyUuid(value string) string {
	if value != "" {
		return strings.ToLower(value)
	} else {
		return p.newUUID()
	}
}

func (p *recordPreparer) checkPlatformId(recordPlatformId uint64) error {
	if recordPlatformId != 0 && recordPlatformId != p.batchPlatformId {
		return errors.New(fmt.Sprintf("Invalid recordPlatformId=%d batchPlatformId=%d", recordPlatformId, p.batchPlatformId))
	} else {
		return nil
	}
}

func (p *recordPreparer) setUserInfoFields(userInfo *common.UserInfo) *common.UserInfo {
	if userInfo != nil && userInfo.LogUserId != "" {
		userInfo.LogUserId = strings.ToLower(userInfo.GetLogUserId())
	}
	return userInfo
}

// The code lowercases UUIDs for log record fields.

func (p *recordPreparer) setUserFields(user *event.User) error {
	err := p.checkPlatformId(user.GetPlatformId())
	user.UserInfo = p.setUserInfoFields(user.GetUserInfo())
	return err
}

func (p *recordPreparer) setCohortMembershipFields(cohortMembership *event.CohortMembership) error {
	err := p.checkPlatformId(cohortMembership.GetPlatformId())
	cohortMembership.UserInfo = p.setUserInfoFields(cohortMembership.GetUserInfo())
	cohortMembership.MembershipId = p.toPrimaryKeyUuid(cohortMembership.GetMembershipId())
	return err
}

func (p *recordPreparer) setSessionProfileFields(sessionProfile *event.SessionProfile) error {
	err := p.checkPlatformId(sessionProfile.GetPlatformId())
	sessionProfile.UserInfo = p.setUserInfoFields(sessionProfile.GetUserInfo())
	sessionProfile.SessionId = strings.ToLower(sessionProfile.GetSessionId())
	return err
}

func (p *recordPreparer) setSessionFields(session *event.Session) error {
	err := p.checkPlatformId(session.GetPlatformId())
	session.UserInfo = p.setUserInfoFields(session.GetUserInfo())
	session.SessionId = p.toPrimaryKeyUuid(session.GetSessionId())
	return err
}

func (p *recordPreparer) setViewFields(view *event.View) error {
	err := p.checkPlatformId(view.GetPlatformId())
	view.UserInfo = p.setUserInfoFields(view.GetUserInfo())
	view.ViewId = p.toPrimaryKeyUuid(view.GetViewId())
	view.SessionId = strings.ToLower(view.GetSessionId())
	err2 := p.setViewTypeField(view)
	if err != nil {
		return err
	}
	return err2
}

func (p *recordPreparer) setViewTypeField(view *event.View) error {
	switch x := view.UiType.(type) {
	case *event.View_WebPageView:
		view.ViewType = event.View_WEB_PAGE
	case *event.View_AppScreenView:
		view.ViewType = event.View_APP_SCREEN
	case nil:
		view.ViewType = event.View_UNKNOWN_VIEW_TYPE
	default:
		return fmt.Errorf("Unexpected ViewType view=%T", x)
	}
	return nil
}

func (p *recordPreparer) setAutoViewFields(autoView *event.AutoView) error {
	err := p.checkPlatformId(autoView.GetPlatformId())
	autoView.UserInfo = p.setUserInfoFields(autoView.GetUserInfo())
	autoView.AutoViewId = p.toPrimaryKeyUuid(autoView.GetAutoViewId())
	autoView.SessionId = strings.ToLower(autoView.GetSessionId())
	autoView.ViewId = strings.ToLower(autoView.GetViewId())
	return err
}

func (p *recordPreparer) setDeliveryLogFields(deliveryLog *delivery.DeliveryLog) error {
	err := p.checkPlatformId(deliveryLog.GetPlatformId())
	request := deliveryLog.GetRequest()

	var err2 error
	if request != nil {
		err2 = p.setRequestFields(deliveryLog.GetRequest())
	} else {
		err2 = errors.New(fmt.Sprintf("DeliveryLog needs a Request; deliveryLog=%+v", deliveryLog))
	}

	// Even though Response Insertions are logged on a DeliveryLog record, we want Response
	// Insertions to be treated as separate log records.  We want to make sure they have
	// their own insertionIds.
	responseInsertions := deliveryLog.GetResponse().GetInsertion()
	if responseInsertions != nil {
		for _, responseInsertion := range responseInsertions {
			p.setResponseInsertionFields(responseInsertion)
		}
	}
	executionInsertions := deliveryLog.GetExecution().GetExecutionInsertion()
	if executionInsertions != nil {
		for _, executionInsertion := range executionInsertions {
			executionInsertion.InsertionId = strings.ToLower(executionInsertion.GetInsertionId())
		}
	}

	if err != nil {
		return err
	}
	return err2
}

func (p *recordPreparer) setRequestFields(request *delivery.Request) error {
	err := p.checkPlatformId(request.GetPlatformId())
	request.UserInfo = p.setUserInfoFields(request.GetUserInfo())
	request.RequestId = p.toPrimaryKeyUuid(request.GetRequestId())
	request.ViewId = strings.ToLower(request.GetViewId())
	request.SessionId = strings.ToLower(request.GetSessionId())
	return err
}

func (p *recordPreparer) setResponseInsertionFields(responseInsertion *delivery.Insertion) error {
	err := p.checkPlatformId(responseInsertion.GetPlatformId())
	responseInsertion.InsertionId = p.toPrimaryKeyUuid(responseInsertion.GetInsertionId())
	responseInsertion.RequestId = strings.ToLower(responseInsertion.GetRequestId())
	responseInsertion.ViewId = strings.ToLower(responseInsertion.GetViewId())
	responseInsertion.SessionId = strings.ToLower(responseInsertion.GetSessionId())
	return err
}

func (p *recordPreparer) setImpressionFields(impression *event.Impression) error {
	err := p.checkPlatformId(impression.GetPlatformId())
	impression.UserInfo = p.setUserInfoFields(impression.GetUserInfo())
	impression.ImpressionId = p.toPrimaryKeyUuid(impression.GetImpressionId())
	impression.InsertionId = strings.ToLower(impression.GetInsertionId())
	impression.RequestId = strings.ToLower(impression.GetRequestId())
	impression.ViewId = strings.ToLower(impression.GetViewId())
	impression.SessionId = strings.ToLower(impression.GetSessionId())
	return err
}

func (p *recordPreparer) setActionFields(action *event.Action) error {
	err := p.checkPlatformId(action.GetPlatformId())
	action.UserInfo = p.setUserInfoFields(action.GetUserInfo())
	action.ActionId = p.toPrimaryKeyUuid(action.GetActionId())
	action.ImpressionId = strings.ToLower(action.GetImpressionId())
	action.InsertionId = strings.ToLower(action.GetInsertionId())
	action.RequestId = strings.ToLower(action.GetRequestId())
	action.ViewId = strings.ToLower(action.GetViewId())
	action.SessionId = strings.ToLower(action.GetSessionId())
	return err
}

func (p *recordPreparer) setDiagnosticsFields(diagnostics *event.Diagnostics) error {
	err := p.checkPlatformId(diagnostics.GetPlatformId())
	diagnostics.UserInfo = p.setUserInfoFields(diagnostics.GetUserInfo())
	return err
}
