// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

// sandbox.go builds SandboxTemplate and SandboxClaim resource manifests and
// wraps them in an OpenChoreo RenderedRelease so the existing cross-cluster
// apply infrastructure (cluster-gateway → data plane) handles deployment.
//
// The module controller never talks to the data plane directly.  Instead it
// creates a RenderedRelease in the control-plane namespace; the core's
// renderedrelease controller picks it up and server-side-applies the resources
// to the correct data-plane namespace via the cluster-gateway.
package agent

import (
	"context"
	"encoding/json"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	openchoreov1alpha1 "github.com/openchoreo/openchoreo/api/v1alpha1"

	sandboxv1alpha1 "github.com/openchoreo/community-modules/agent-sandbox/api/v1alpha1"
)

// Label keys stamped on sandbox resources managed by this controller.
const (
	labelManagedBy   = "agent.openchoreo.dev/managed-by"
	labelComponent   = "agent.openchoreo.dev/component"
	labelEnvironment = "agent.openchoreo.dev/environment"
	labelPolicyRef   = "agent.openchoreo.dev/sandbox-policy"

	managedByValue = "agent-sandbox-controller"
)

// renderedReleaseName returns a stable name for the RenderedRelease.
func renderedReleaseName(compName, env string) string {
	return fmt.Sprintf("agent-%s-%s", compName, env)
}

// ensureRenderedRelease creates or updates a RenderedRelease containing the
// SandboxTemplate and SandboxClaim manifests targeting the data-plane namespace.
func (r *Reconciler) ensureRenderedRelease(
	ctx context.Context,
	comp *openchoreov1alpha1.Component,
	params *agentParams,
	container *openchoreov1alpha1.Container,
	env string,
	policy *sandboxv1alpha1.SandboxPolicy,
) error {
	logger := log.FromContext(ctx)
	rrName := renderedReleaseName(comp.Name, env)

	// Compute the data-plane namespace using the same convention as the core.
	dpNamespace := generateDPNamespace(comp.Namespace, comp.Spec.Owner.ProjectName, env)
	baseName := generateResourceName(comp.Name, env)

	// Build resource manifests as JSON.
	templateJSON, err := buildSandboxTemplateJSON(params, container, baseName, dpNamespace, comp.Name)
	if err != nil {
		return fmt.Errorf("failed to build SandboxTemplate JSON: %w", err)
	}
	claimJSON, err := buildSandboxClaimJSON(params, baseName, dpNamespace, comp.Name, env)
	if err != nil {
		return fmt.Errorf("failed to build SandboxClaim JSON: %w", err)
	}

	resources := []openchoreov1alpha1.Resource{
		{
			ID:     "sandbox-template",
			Object: &runtime.RawExtension{Raw: templateJSON},
		},
		{
			ID:     "sandbox-claim",
			Object: &runtime.RawExtension{Raw: claimJSON},
		},
	}

	// Add SandboxWarmPool if warmPoolSize > 0.
	if params.WarmPoolSize > 0 {
		warmPoolJSON, err := buildSandboxWarmPoolJSON(params, baseName, dpNamespace, comp.Name)
		if err != nil {
			return fmt.Errorf("failed to build SandboxWarmPool JSON: %w", err)
		}
		resources = append(resources, openchoreov1alpha1.Resource{
			ID:     "sandbox-warmpool",
			Object: &runtime.RawExtension{Raw: warmPoolJSON},
		})
	}

	// Add NetworkPolicy if a SandboxPolicy is referenced — the NetworkPolicy must
	// land in the data-plane namespace so it applies to the agent Sandbox pods.
	if policy != nil {
		npJSON, err := buildNetworkPolicyJSON(policy, baseName, dpNamespace, comp.Name)
		if err != nil {
			return fmt.Errorf("failed to build NetworkPolicy JSON: %w", err)
		}
		resources = append(resources, openchoreov1alpha1.Resource{
			ID:     "network-policy",
			Object: &runtime.RawExtension{Raw: npJSON},
		})
	}

	desired := &openchoreov1alpha1.RenderedRelease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rrName,
			Namespace: comp.Namespace,
			Labels: map[string]string{
				labelManagedBy:   managedByValue,
				labelComponent:   comp.Name,
				labelEnvironment: env,
			},
		},
		Spec: openchoreov1alpha1.RenderedReleaseSpec{
			Owner: openchoreov1alpha1.RenderedReleaseOwner{
				ProjectName:   comp.Spec.Owner.ProjectName,
				ComponentName: comp.Name,
			},
			EnvironmentName: env,
			TargetPlane:     "dataplane",
			Resources:       resources,
		},
	}

	existing := &openchoreov1alpha1.RenderedRelease{}
	err = r.Get(ctx, types.NamespacedName{Name: rrName, Namespace: comp.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return fmt.Errorf("failed to create RenderedRelease %q: %w", rrName, err)
		}
		logger.Info("Created RenderedRelease", "name", rrName, "dpNamespace", dpNamespace)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get RenderedRelease %q: %w", rrName, err)
	}

	// Update only when resources changed (e.g., new image after a build).
	if apiequality.Semantic.DeepEqual(existing.Spec.Resources, desired.Spec.Resources) {
		return nil
	}

	existing.Spec.Resources = desired.Spec.Resources
	existing.Labels = desired.Labels
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("failed to update RenderedRelease %q: %w", rrName, err)
	}
	logger.Info("Updated RenderedRelease", "name", rrName)
	return nil
}

// isRenderedReleaseReady checks the RenderedRelease status conditions.
func (r *Reconciler) isRenderedReleaseReady(
	ctx context.Context,
	comp *openchoreov1alpha1.Component,
	env string,
) (bool, string, error) {
	rr := &openchoreov1alpha1.RenderedRelease{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      renderedReleaseName(comp.Name, env),
		Namespace: comp.Namespace,
	}, rr); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "RenderedRelease not found", nil
		}
		return false, "", err
	}

	for _, c := range rr.Status.Conditions {
		if c.Type == "ResourcesApplied" && c.Status == metav1.ConditionTrue {
			return true, c.Message, nil
		}
	}
	return false, "Waiting for resources to be applied to the data plane", nil
}

// cleanupRenderedRelease deletes the RenderedRelease for a component.
func (r *Reconciler) cleanupRenderedRelease(
	ctx context.Context,
	comp *openchoreov1alpha1.Component,
) error {
	logger := log.FromContext(ctx)

	var list openchoreov1alpha1.RenderedReleaseList
	if err := r.List(ctx, &list,
		client.InNamespace(comp.Namespace),
		client.MatchingLabels{
			labelManagedBy: managedByValue,
			labelComponent: comp.Name,
		},
	); err != nil {
		return fmt.Errorf("failed to list RenderedReleases for cleanup: %w", err)
	}

	for i := range list.Items {
		if err := r.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("failed to delete RenderedRelease %q: %w", list.Items[i].Name, err)
		}
		logger.Info("Deleted RenderedRelease", "name", list.Items[i].Name)
	}
	return nil
}

// findComponentsForRenderedRelease maps a changed RenderedRelease back to its Component.
func (r *Reconciler) findComponentsForRenderedRelease(
	_ context.Context, obj client.Object,
) []ctrl.Request {
	labels := obj.GetLabels()
	if labels[labelManagedBy] != managedByValue {
		return nil
	}
	compName := labels[labelComponent]
	if compName == "" {
		return nil
	}
	return []ctrl.Request{{
		NamespacedName: types.NamespacedName{
			Name:      compName,
			Namespace: obj.GetNamespace(),
		},
	}}
}

// ─── Resource JSON builders ─────────────────────────────────────────────────

// sandboxTemplateManifest is the JSON structure for a SandboxTemplate resource.
type sandboxTemplateManifest struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Metadata   resourceMetadata       `json:"metadata"`
	Spec       map[string]interface{} `json:"spec"`
}

// sandboxClaimManifest is the JSON structure for a SandboxClaim resource.
type sandboxClaimManifest struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Metadata   resourceMetadata       `json:"metadata"`
	Spec       map[string]interface{} `json:"spec"`
}

type resourceMetadata struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels"`
}

func buildSandboxTemplateJSON(
	params *agentParams,
	container *openchoreov1alpha1.Container,
	name, namespace, compName string,
) ([]byte, error) {
	containerSpec := map[string]interface{}{
		"name":            "agent",
		"image":           container.Image,
		"imagePullPolicy": "IfNotPresent",
		"resources": map[string]interface{}{
			"requests": map[string]interface{}{
				"memory": "256Mi",
				"cpu":    "100m",
			},
			"limits": map[string]interface{}{
				"memory": "1Gi",
				"cpu":    "1",
			},
		},
	}
	if len(container.Command) > 0 {
		containerSpec["command"] = container.Command
	}
	if len(container.Args) > 0 {
		containerSpec["args"] = container.Args
	}

	podSpec := map[string]interface{}{
		"automountServiceAccountToken": false,
		"containers":                   []interface{}{containerSpec},
	}
	if rc := runtimeClassName(params.IsolationTier); rc != "" {
		podSpec["runtimeClassName"] = rc
	}

	manifest := sandboxTemplateManifest{
		APIVersion: "extensions.agents.x-k8s.io/v1alpha1",
		Kind:       "SandboxTemplate",
		Metadata: resourceMetadata{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				labelManagedBy: managedByValue,
				labelComponent: compName,
			},
		},
		Spec: map[string]interface{}{
			"podTemplate": map[string]interface{}{
				"spec": podSpec,
			},
		},
	}
	return json.Marshal(manifest)
}

func buildSandboxClaimJSON(
	params *agentParams,
	name, namespace, compName, env string,
) ([]byte, error) {
	claimLabels := map[string]string{
		labelManagedBy:   managedByValue,
		labelComponent:   compName,
		labelEnvironment: env,
	}
	podLabels := map[string]interface{}{
		labelComponent:   compName,
		labelEnvironment: env,
	}
	if params.SandboxPolicyRef != "" {
		claimLabels[labelPolicyRef] = params.SandboxPolicyRef
		podLabels[labelPolicyRef] = params.SandboxPolicyRef
	}

	spec := map[string]interface{}{
		"sandboxTemplateRef": map[string]interface{}{
			"name": name,
		},
		"additionalPodMetadata": map[string]interface{}{
			"labels": podLabels,
		},
	}

	// Add lifecycle TTL if specified.
	if params.TTLSeconds > 0 {
		spec["lifecycle"] = map[string]interface{}{
			"ttlSecondsAfterFinished": params.TTLSeconds,
		}
	}

	manifest := sandboxClaimManifest{
		APIVersion: "extensions.agents.x-k8s.io/v1alpha1",
		Kind:       "SandboxClaim",
		Metadata: resourceMetadata{
			Name:      name,
			Namespace: namespace,
			Labels:    claimLabels,
		},
		Spec: spec,
	}
	return json.Marshal(manifest)
}

func buildSandboxWarmPoolJSON(
	params *agentParams,
	name, namespace, compName string,
) ([]byte, error) {
	manifest := struct {
		APIVersion string                 `json:"apiVersion"`
		Kind       string                 `json:"kind"`
		Metadata   resourceMetadata       `json:"metadata"`
		Spec       map[string]interface{} `json:"spec"`
	}{
		APIVersion: "extensions.agents.x-k8s.io/v1alpha1",
		Kind:       "SandboxWarmPool",
		Metadata: resourceMetadata{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				labelManagedBy: managedByValue,
				labelComponent: compName,
			},
		},
		Spec: map[string]interface{}{
			"replicas": params.WarmPoolSize,
			"sandboxTemplateRef": map[string]interface{}{
				"name": name,
			},
		},
	}
	return json.Marshal(manifest)
}

// buildNetworkPolicyJSON generates a Kubernetes NetworkPolicy from a SandboxPolicy,
// targeting the data-plane namespace so the policy applies to the Sandbox pods.
func buildNetworkPolicyJSON(
	policy *sandboxv1alpha1.SandboxPolicy,
	name, namespace, compName string,
) ([]byte, error) {
	// Always allow kube-dns (UDP+TCP 53).
	egressRules := []interface{}{
		map[string]interface{}{
			"ports": []interface{}{
				map[string]interface{}{"protocol": "UDP", "port": 53},
				map[string]interface{}{"protocol": "TCP", "port": 53},
			},
		},
	}

	// Per-host rules.
	for _, ah := range policy.Spec.AllowedHosts {
		rule := map[string]interface{}{}
		if len(ah.Ports) > 0 {
			ports := make([]interface{}, 0, len(ah.Ports))
			proto := "TCP"
			if ah.Protocol == "UDP" {
				proto = "UDP"
			}
			for _, p := range ah.Ports {
				ports = append(ports, map[string]interface{}{
					"protocol": proto,
					"port":     p,
				})
			}
			rule["ports"] = ports
		}
		if isCIDR(ah.Host) {
			rule["to"] = []interface{}{
				map[string]interface{}{
					"ipBlock": map[string]interface{}{"cidr": ah.Host},
				},
			}
		}
		egressRules = append(egressRules, rule)
	}

	// Per-MCP server rules (HTTPS 443).
	for range policy.Spec.AllowedMCPServers {
		egressRules = append(egressRules, map[string]interface{}{
			"ports": []interface{}{
				map[string]interface{}{"protocol": "TCP", "port": 443},
			},
		})
	}

	// Catch-all egress when defaultEgress is "allow".
	if policy.Spec.DefaultEgress == sandboxv1alpha1.EgressActionAllow {
		egressRules = append(egressRules, map[string]interface{}{})
	}

	manifest := map[string]interface{}{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]interface{}{
			"name":      "agent-sandbox-" + policy.Name,
			"namespace": namespace,
			"labels": map[string]string{
				labelManagedBy: managedByValue,
				labelComponent: compName,
				labelPolicyRef: policy.Name,
			},
		},
		"spec": map[string]interface{}{
			"podSelector": map[string]interface{}{
				"matchLabels": map[string]string{
					labelPolicyRef: policy.Name,
				},
			},
			"policyTypes": []string{"Egress"},
			"egress":      egressRules,
		},
	}
	return json.Marshal(manifest)
}

// isCIDR returns true when the string looks like an IP or CIDR block.
func isCIDR(s string) bool {
	for _, c := range s {
		if c == '/' || (c >= '0' && c <= '9') || c == '.' || c == ':' {
			continue
		}
		return false
	}
	return len(s) > 0
}
