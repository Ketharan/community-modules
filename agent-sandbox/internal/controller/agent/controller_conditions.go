// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types set on the Component by the agent-sandbox controller.
const (
	// ConditionAgentSandboxReady indicates whether the agent Sandbox is running.
	ConditionAgentSandboxReady = "AgentSandboxReady"
)

// Condition reasons.
const (
	// ReasonSandboxClaimBound indicates the SandboxClaim was fulfilled and the Sandbox is running.
	ReasonSandboxClaimBound = "SandboxClaimBound"

	// ReasonSandboxClaimPending indicates the SandboxClaim exists but has not yet been
	// bound to a Sandbox by the upstream controller.
	ReasonSandboxClaimPending = "SandboxClaimPending"

	// ReasonUpstreamNotInstalled indicates the kubernetes-sigs/agent-sandbox CRDs are not
	// yet registered on the cluster (upstream pre-install Job may still be running).
	ReasonUpstreamNotInstalled = "UpstreamNotInstalled"

	// ReasonWorkloadNotFound indicates no Workload resource exists for this Component yet.
	ReasonWorkloadNotFound = "WorkloadNotFound"

	// ReasonDeploymentPipelineNotFound indicates no DeploymentPipeline is configured for the project.
	ReasonDeploymentPipelineNotFound = "DeploymentPipelineNotFound"

	// ReasonInvalidConfiguration indicates the Component has missing or invalid configuration.
	ReasonInvalidConfiguration = "InvalidConfiguration"

	// ReasonReconcileError indicates an unexpected error during reconciliation.
	ReasonReconcileError = "ReconcileError"
)

// agentSandboxReadyCondition returns a metav1.Condition for the AgentSandboxReady type.
func agentSandboxReadyCondition(status metav1.ConditionStatus, reason, message string, generation int64) metav1.Condition {
	return metav1.Condition{
		Type:               ConditionAgentSandboxReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
	}
}

// setCondition upserts a condition on the given slice by Type.
func setCondition(conditions *[]metav1.Condition, newCond metav1.Condition) {
	for i, c := range *conditions {
		if c.Type == newCond.Type {
			if c.Status == newCond.Status && c.Reason == newCond.Reason && c.Message == newCond.Message {
				return // no change, preserve LastTransitionTime
			}
			(*conditions)[i] = newCond
			return
		}
	}
	*conditions = append(*conditions, newCond)
}
