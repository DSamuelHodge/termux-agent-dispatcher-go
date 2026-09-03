// Package errs defines the stable machine-readable error codes shared by
// the dispatcher packages. Mirrors dispatch/errors.py.
package errs

// Stable codes. HTTP status is conventional, not part of the code name.
const (
	Unauthorized           = "UNAUTHORIZED"
	NotFound               = "NOT_FOUND"
	UnknownVerb            = "UNKNOWN_VERB"
	InvalidJSON            = "INVALID_JSON"
	InvalidBody            = "INVALID_BODY"
	InvalidArgs            = "INVALID_ARGS"
	InvalidRoute           = "INVALID_ROUTE"
	MissingIdempotencyKey  = "MISSING_IDEMPOTENCY_KEY"
	IdempotencyConflict    = "IDEMPOTENCY_CONFLICT"
	ConfirmDenied          = "CONFIRM_DENIED"
	ConfirmPending         = "CONFIRM_PENDING"
	ConfirmNotFound        = "CONFIRM_NOT_FOUND"
	CircuitOpen            = "CIRCUIT_OPEN"
	ExecutionFailed        = "EXECUTION_FAILED"
	Timeout                = "TIMEOUT"
	TierCUnavailable       = "TIER_C_UNAVAILABLE"
	OriginForbidden        = "ORIGIN_FORBIDDEN"
)

// Payload builds a structured error body: {"error": message, "code": code, ...extra}.
func Payload(code, message string, extra map[string]any) map[string]any {
	body := map[string]any{"error": message, "code": code}
	for k, v := range extra {
		body[k] = v
	}
	return body
}
