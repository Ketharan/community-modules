// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"encoding/json"
	"fmt"

	openchoreov1alpha1 "github.com/openchoreo/openchoreo/api/v1alpha1"
)

// IsolationTier defines the sandbox isolation level for an agent workload.
type IsolationTier string

const (
	// IsolationStandard uses hardened Linux namespaces (runc). No extra node requirement.
	IsolationStandard IsolationTier = "standard"
	// IsolationEnhanced uses gVisor for syscall interception via a user-space kernel.
	// Requires the "gvisor" RuntimeClass on data-plane nodes.
	IsolationEnhanced IsolationTier = "enhanced"
	// IsolationMaximum uses Kata Containers for full VM isolation via Firecracker/QEMU.
	// Requires the "kata" RuntimeClass on data-plane nodes.
	IsolationMaximum IsolationTier = "maximum"
)

// agentParams holds the validated, typed parameters for an agent Component.
type agentParams struct {
	// IsolationTier controls the sandbox runtime (standard | enhanced | maximum).
	IsolationTier IsolationTier `json:"isolationTier,omitempty"`
	// SandboxPolicyRef is the name of the SandboxPolicy to use for network egress control.
	SandboxPolicyRef string `json:"sandboxPolicyRef,omitempty"`
}

// parseAgentParams unmarshals the Component's raw parameters into an agentParams struct.
// Missing fields default to: IsolationTier → "standard".
func parseAgentParams(comp *openchoreov1alpha1.Component) (*agentParams, error) {
	p := &agentParams{
		IsolationTier: IsolationStandard,
	}
	if comp.Spec.Parameters == nil {
		return p, nil
	}
	if err := json.Unmarshal(comp.Spec.Parameters.Raw, p); err != nil {
		return nil, fmt.Errorf("invalid agent parameters: %w", err)
	}
	if p.IsolationTier == "" {
		p.IsolationTier = IsolationStandard
	}
	return p, nil
}

// runtimeClassName maps an IsolationTier to the Kubernetes runtimeClassName.
// Returns an empty string for IsolationStandard (no explicit runtimeClassName needed).
func runtimeClassName(tier IsolationTier) string {
	switch tier {
	case IsolationEnhanced:
		return "gvisor"
	case IsolationMaximum:
		return "kata"
	default:
		return ""
	}
}
