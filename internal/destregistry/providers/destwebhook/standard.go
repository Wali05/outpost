package destwebhook

import "time"

// Standard Webhooks (https://www.standardwebhooks.com) expressed as values of
// the webhook options. "standard" mode is resolved in config, which sets
// these; the provider itself has no modes.
const (
	StandardHeaderPrefix         = "webhook-"
	StandardEventIDHeaderKey     = "id" // "<prefix>id" rather than "<prefix>event-id"
	StandardTimestampFormat      = TimestampFormatUnix
	StandardSignatureContentTmpl = "{{.EventID}}.{{.Timestamp.Unix}}.{{.Body}}"
	StandardSignatureHeaderTmpl  = "v1,{{index .Signatures 0}}{{range slice .Signatures 1}} v1,{{.}}{{end}}"
	StandardEncoding             = "base64"
	StandardSecretEncoding       = SecretEncodingBase64
	StandardSecretPrefix         = "whsec_"
	StandardSigningSecretTmpl    = "whsec_{{.RandomBase64}}"
	StandardMetadataName         = "webhook_standard" // metadata/providers dir with the verification instructions
)

// StandardRetrySchedule is the Standard Webhooks example retry schedule: the
// delay before each retry after the immediate first attempt.
var StandardRetrySchedule = []time.Duration{
	5 * time.Second,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	5 * time.Hour,
	10 * time.Hour,
	14 * time.Hour,
	20 * time.Hour,
	24 * time.Hour,
}
