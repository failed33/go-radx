package dimse

import (
	"crypto/tls"
	"time"

	"github.com/codeninja55/go-radx/dicom"
)

// Default timeouts mirror the pynetdicom defaults (dimse.md "Application Entity"). All are
// also overridable per association via context deadlines.
const (
	defaultACSETimeout    = 30 * time.Second // association negotiation/release
	defaultDIMSETimeout   = 30 * time.Second // awaiting a DIMSE response
	defaultNetworkTimeout = 60 * time.Second // idle association
	defaultMaxPDULength   = MaxPDULength(16382)
)

// aeConfig holds the per-AE knobs set by functional options. There is no global mutable
// state (PRD §9.4); every AE carries its own configuration.
type aeConfig struct {
	acseTimeout       time.Duration
	dimseTimeout      time.Duration
	networkTimeout    time.Duration
	connectionTimeout time.Duration // 0 = no explicit TCP-connect timeout
	maxPDULength      MaxPDULength

	implementationClassUID dicom.UID
	implementationVersion  string

	// tlsConfig, when non-nil, secures every transport this AE opens: an outbound SCU connection
	// dials over TLS and an inbound SCP listener terminates TLS. It is nil by default, so an AE
	// with no WithTLS runs over plaintext TCP, unchanged. The config is never mutated in place;
	// each transport clones it before adjusting per-connection fields.
	tlsConfig *tls.Config
}

// AEOption configures an AE at construction.
type AEOption func(*aeConfig)

// WithACSETimeout sets the association negotiation/release timeout (default 30s).
func WithACSETimeout(d time.Duration) AEOption {
	return func(c *aeConfig) { c.acseTimeout = d }
}

// WithDIMSETimeout sets the SCU-side data-transfer idle bound (default 30s): the maximum
// silence between PDUs on an established association, in either direction. The bound
// re-arms on every PDU (Conn.SetIdleTimeout), so it measures peer inactivity — never an
// operation's total duration nor a message's size: a C-MOVE streaming responses for
// minutes, or a large dataset mid-reassembly, keeps re-arming it. An expiry with the
// caller's ctx still live surfaces as a *TimeoutError (via Association.LastError on the
// query/retrieve iterators), after which the association must be discarded. Zero disables
// the bound. Association negotiation is governed by WithACSETimeout, not this.
func WithDIMSETimeout(d time.Duration) AEOption {
	return func(c *aeConfig) { c.dimseTimeout = d }
}

// WithNetworkTimeout sets the SCP-side data-transfer idle bound (default 60s): the
// server-side counterpart of WithDIMSETimeout, armed on accepted associations once
// negotiation completes and re-armed per PDU. Zero disables the bound.
func WithNetworkTimeout(d time.Duration) AEOption {
	return func(c *aeConfig) { c.networkTimeout = d }
}

// WithConnectionTimeout sets the TCP-connect timeout (default none).
func WithConnectionTimeout(d time.Duration) AEOption {
	return func(c *aeConfig) { c.connectionTimeout = d }
}

// WithMaxPDULength sets the maximum PDU length to advertise (default 16382; 0 = unlimited,
// resolved at fragmentation time against the local send cap — Codex DIMSE-005).
func WithMaxPDULength(n MaxPDULength) AEOption {
	return func(c *aeConfig) { c.maxPDULength = n }
}

// WithImplementationClassUID sets the Implementation Class UID advertised in the
// A-ASSOCIATE user information (PS3.7 D.3.3.2).
func WithImplementationClassUID(uid dicom.UID) AEOption {
	return func(c *aeConfig) { c.implementationClassUID = uid }
}

// WithImplementationVersionName sets the Implementation Version Name advertised in the
// A-ASSOCIATE user information (PS3.7 D.3.3.2).
func WithImplementationVersionName(name string) AEOption {
	return func(c *aeConfig) { c.implementationVersion = name }
}

// WithTLS secures the AE's transport with cfg, applied to BOTH outbound SCU connections (which
// dial over TLS) and the inbound SCP listener (which terminates TLS). Peer-certificate
// verification is on by default: the library never sets InsecureSkipVerify, so a caller who needs
// it must set it explicitly on cfg (the deliberate, auditable escape hatch). The library enforces a
// TLS 1.2 floor: any cfg.MinVersion below 1.2 (unset, or a pinned 1.0/1.1) is raised to 1.2,
// preferring 1.3 where the peer supports it (automatic once the floor is 1.2); a caller-pinned
// higher floor is preserved. Mutual TLS is configured through
// cfg: the SCU supplies client certificates in cfg.Certificates, and the SCP requires them with
// cfg.ClientAuth = tls.RequireAndVerifyClientCert plus cfg.ClientCAs. A nil cfg leaves the AE on
// plaintext TCP (the default). Certificates and keys come from environment or files, never
// hard-coded, and are never logged (PRD §9.7, §9.8).
func WithTLS(cfg *tls.Config) AEOption {
	return func(c *aeConfig) {
		// Clone on apply so a caller that mutates or reuses cfg after NewAE cannot change this AE's
		// trust roots, certificates, or verification settings: the per-AE config is immutable and
		// concurrent Associate calls must not race a caller-side mutation.
		if cfg == nil {
			c.tlsConfig = nil
			return
		}
		c.tlsConfig = cfg.Clone()
	}
}

// AE is a local DICOM Application Entity: the factory for outbound associations (SCU) and
// inbound listeners (SCP). It carries no global mutable state; every knob is per-AE and set
// with functional options. Safe for concurrent use (the config is immutable after NewAE).
type AE struct {
	title AETitle
	cfg   aeConfig
}

// NewAE constructs an AE with the given local AE Title and options. Zero options yield the
// pynetdicom-default timeouts and a 16382-byte maximum PDU length. It returns a typed
// *ValidationError if the title is not a conformant AE Title.
func NewAE(title AETitle, opts ...AEOption) (*AE, error) {
	if _, err := ParseAETitle(string(title)); err != nil {
		return nil, err
	}
	cfg := aeConfig{
		acseTimeout:    defaultACSETimeout,
		dimseTimeout:   defaultDIMSETimeout,
		networkTimeout: defaultNetworkTimeout,
		maxPDULength:   defaultMaxPDULength,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &AE{title: title, cfg: cfg}, nil
}

// Title returns the local AE Title.
func (ae *AE) Title() AETitle { return ae.title }

// config returns the resolved configuration (internal; used by Associate and Server).
func (ae *AE) config() aeConfig { return ae.cfg }

// tlsConfigWithFloor returns a clone of the AE's TLS config with the minimum protocol version
// clamped up to TLS 1.2, or nil when the AE has no TLS config. Cloning keeps the caller's config
// immutable and gives each transport its own copy to adjust per-connection (e.g. the SCU sets
// ServerName). Any MinVersion below 1.2 — unset (0) or a pinned 1.0/1.1 — is raised to 1.2 so the
// public "TLS 1.2+" contract holds regardless of what the caller supplied; a caller-pinned HIGHER
// floor (e.g. 1.3) is preserved. The 1.2 floor still prefers 1.3 automatically: Go negotiates the
// highest version both sides support.
func (c aeConfig) tlsConfigWithFloor() *tls.Config {
	if c.tlsConfig == nil {
		return nil
	}
	cfg := c.tlsConfig.Clone()
	if cfg.MinVersion < tls.VersionTLS12 {
		cfg.MinVersion = tls.VersionTLS12
	}
	return cfg
}
