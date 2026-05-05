// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EgressAction defines the default posture for outbound traffic.
// +kubebuilder:validation:Enum=deny;allow
type EgressAction string

const (
	// EgressActionDeny blocks all outbound traffic not explicitly allowed.
	EgressActionDeny EgressAction = "deny"
	// EgressActionAllow permits all outbound traffic not explicitly blocked.
	EgressActionAllow EgressAction = "allow"
)

// AllowedHost describes a single egress target that the agent is permitted to reach.
type AllowedHost struct {
	// Host is the DNS hostname or CIDR block to allow.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// Ports lists the allowed destination ports. If empty, all ports are allowed for this host.
	// +optional
	Ports []int32 `json:"ports,omitempty"`

	// Protocol is the IP protocol (TCP or UDP). Defaults to TCP.
	// +optional
	// +kubebuilder:validation:Enum=TCP;UDP
	// +kubebuilder:default=TCP
	Protocol string `json:"protocol,omitempty"`
}

// AllowedMCPServer describes an MCP server the agent may reach, with optional scope restrictions.
type AllowedMCPServer struct {
	// URL is the base URL of the MCP server.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`

	// Scopes lists the MCP tool scopes the agent is allowed to use.
	// If empty, all scopes on this server are allowed.
	// +optional
	Scopes []string `json:"scopes,omitempty"`
}

// TokenLimitsSpec is reserved for Phase 2: LLM token budget controls.
// +kubebuilder:pruning:PreserveUnknownFields
type TokenLimitsSpec struct{}

// OperationsSpec is reserved for Phase 2: shell and filesystem constraints.
// +kubebuilder:pruning:PreserveUnknownFields
type OperationsSpec struct{}

// AuditSpec is reserved for Phase 2: structured audit logging.
// +kubebuilder:pruning:PreserveUnknownFields
type AuditSpec struct{}

// ToolAccessSpec is reserved for Phase 2: MCP tool governance.
// +kubebuilder:pruning:PreserveUnknownFields
type ToolAccessSpec struct{}

// SandboxPolicySpec defines the policy applied to an agent workload.
// Phase 1 implements network egress controls. Future phases add tokenLimits,
// operations, audit, and toolAccess.
type SandboxPolicySpec struct {
	// DefaultEgress is the default action for outbound traffic that does not match any rule.
	// Use "deny" (recommended for production) to enforce an explicit allow-list,
	// or "allow" for development environments where wider access is acceptable.
	// +kubebuilder:default=deny
	DefaultEgress EgressAction `json:"defaultEgress"`

	// AllowedHosts lists hosts and ports the agent may reach.
	// kube-dns (UDP 53) is always allowed regardless of this list.
	// +optional
	AllowedHosts []AllowedHost `json:"allowedHosts,omitempty"`

	// AllowedMCPServers lists MCP server endpoints the agent may call.
	// +optional
	AllowedMCPServers []AllowedMCPServer `json:"allowedMCPServers,omitempty"`

	// TokenLimits is reserved for Phase 2 (LLM token budgets). Ignored in Phase 1.
	// +optional
	TokenLimits *TokenLimitsSpec `json:"tokenLimits,omitempty"`

	// Operations is reserved for Phase 2 (shell/filesystem constraints). Ignored in Phase 1.
	// +optional
	Operations *OperationsSpec `json:"operations,omitempty"`

	// Audit is reserved for Phase 2 (structured audit logging). Ignored in Phase 1.
	// +optional
	Audit *AuditSpec `json:"audit,omitempty"`

	// ToolAccess is reserved for Phase 2 (MCP tool governance). Ignored in Phase 1.
	// +optional
	ToolAccess *ToolAccessSpec `json:"toolAccess,omitempty"`
}

// SandboxPolicyStatus defines the observed state of SandboxPolicy.
type SandboxPolicyStatus struct {
	// ObservedGeneration is the last generation reconciled by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the SandboxPolicy state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sp;sps
// +kubebuilder:printcolumn:name="DefaultEgress",type=string,JSONPath=`.spec.defaultEgress`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// SandboxPolicy defines network egress and access controls for an agent workload.
// A SandboxPolicy is referenced by name in a Component's parameters.sandboxPolicyRef field.
type SandboxPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxPolicySpec   `json:"spec,omitempty"`
	Status SandboxPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxPolicyList contains a list of SandboxPolicy.
type SandboxPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxPolicy{}, &SandboxPolicyList{})
}
