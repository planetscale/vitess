package kmsbackup

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kms"
)

var (
	// Retry these KMS operations for "read: connection reset" errors.
	kmsRetryableOpsForReadConnResetError = map[string]bool{
		"Decrypt":         true,
		"GenerateDataKey": true,
	}
)

func canRetryKMSOperationForReadConnResetError(o *request.Operation) bool {
	return kmsRetryableOpsForReadConnResetError[o.Name]
}

func isKMSRequest(r *request.Request) bool {
	return r.ClientInfo.ServiceID == kms.ServiceID
}

func isReadConnResetError(err error) bool {
	return strings.Contains(err.Error(), "read: connection reset")
}

// kmsReadConnResetRetryer retries "read: connection reset" errors for specific
// KMS API operations.
//
// The DefaultRetryer used by KMS treats "read: connection reset" errors as
// non-retryable.
//
// See: https://github.com/aws/aws-sdk-go/blob/cd16b71be915af3d08b739c1678f084243133905/aws/request/connection_reset_error.go#L8-L10
//
// This is because the AWS SDK cannot determine from the error if the request
// was successfully processed by AWS, and does not want to blindly retry
// requests that may not be idempotent.
//
// See: https://github.com/aws/aws-sdk-go/pull/2926#issuecomment-553196888
// See: https://github.com/aws/aws-sdk-go/issues/3027#issuecomment-567269161
//
// While this may make sense as a general behavior, with KMS it is safe to
// retry API calls that we know to be idempotent (such as Decrypt) or which are
// safe to retry for other reasons (such as GenerateDataKey).
type kmsReadConnResetRetryer struct {
	client.DefaultRetryer
}

func newKMSReadConnResetRetryer() *kmsReadConnResetRetryer {
	return &kmsReadConnResetRetryer{
		client.DefaultRetryer{
			NumMaxRetries: client.DefaultRetryerMaxNumRetries,
		},
	}
}

func (kr *kmsReadConnResetRetryer) ShouldRetry(r *request.Request) bool {
	if !isKMSRequest(r) {
		return kr.DefaultRetryer.ShouldRetry(r)
	}

	// "ShouldRetry returns false if number of max retries is 0."
	//
	// This behavior is copied from DefaultRetryer.
	//
	// See: https://github.com/aws/aws-sdk-go/blob/cd16b71be915af3d08b739c1678f084243133905/aws/client/default_retryer.go
	if kr.DefaultRetryer.NumMaxRetries == 0 {
		return false
	}

	// "If one of the other handlers already set the retry state
	// we don't want to override it based on the service's state"
	//
	// This behavior is copied from DefaultRetryer.
	//
	// See: https://github.com/aws/aws-sdk-go/blob/cd16b71be915af3d08b739c1678f084243133905/aws/client/default_retryer.go
	if r.Retryable != nil {
		return *r.Retryable
	}

	// The DefaultRetryer treats all "read: connection reset" errors as
	// non-retryable. We want to invert this behavior for some KMS operations.
	if isReadConnResetError(r.Error) && canRetryKMSOperationForReadConnResetError(r.Operation) {
		return true
	}

	// Fall back to the wrapped Retryer.
	return kr.DefaultRetryer.ShouldRetry(r)
}
