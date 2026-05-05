// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

// sandbox.go manages the kubernetes-sigs/agent-sandbox resources (SandboxTemplate and
// SandboxClaim) for each agent Component.
//
// Integration model:
//   - One SandboxTemplate per Component, named "agent-<component>", encoding the isolation
//     tier (runtimeClassName) and the container spec from the Workload resource.
//   - One SandboxClaim per Component+environment, named "<component>-<env>", referencing
//     the template and carrying the sandbox-policy label so the generated NetworkPolicy
//     targets the resulting Sandbox pod.
//   - The upstream kubernetes-sigs/agent-sandbox controller fulfils the claim from its
//     pre-warmed pool; our controller reflects that bound status back on the Component.
//
// Both resources use client.Unstructured so the module compiles without a direct Go
// dependency on sigs.k8s.io/agent-sandbox.  The GVK contract is:
//   extensions.agents.x-k8s.io/v1alpha1  — SandboxTemplate, SandboxClaim
package agent

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	openchoreov1alpha1 "github.com/openchoreo/openchoreo/api/v1alpha1"
)

// GVKs for the extensions API group of kubernetes-sigs/agent-sandbox.
var (
	sandboxTemplateGVK = schema.GroupVersionKind{
		Group:   "extensions.agents.x-k8s.io",
		Version: "v1alpha1",
		Kind:    "SandboxTemplate",
	}
	sandboxClaimGVK = schema.GroupVersionKind{
		Group:   "extensions.agents.x-k8s.io",
		Version: "v1alpha1",
		Kind:    "SandboxClaim",
	}
	sandboxClaimListGVK = schema.GroupVersionKind{
		Group:   "extensions.agents.x-k8s.io",
		Version: "v1alpha1",
		Kind:    "SandboxClaimList",
	}
)

// Label keys stamped on sandbox resources managed by this controller.
const (
	// labelManagedBy identifies resources owned by the agent-sandbox-controller.
	labelManagedBy = "agent.openchoreo.dev/managed-by"
	// labelComponent links a sandbox resource back to the Component that owns it.
	labelComponent = "agent.openchoreo.dev/component"
	// labelEnvironment records the target environment on the SandboxClaim.
	labelEnvironment = "agent.openchoreo.dev/environment"
	// labelPolicyRef carries the SandboxPolicy name so the NetworkPolicy selects the pod.
	// Must match the label key used by the policy controller.
	labelPolicyRef = "agent.openchoreo.dev/sandbox-policy"
)

// sandboxTemplateName returns the SandboxTemplate resource name for a Component.
func sandboxTemplateName(compName string) string {
	return "agent-" + compName
}

// sandboxClaimName returns the SandboxClaim resource name for a Component+environment pair.
func sandboxClaimName(compName, env string) string {
	return compName + "-" + env
}

// ensureSandboxTemplate creates or updates the SandboxTemplate for an agent Component.
// The template encodes the isolation tier via runtimeClassName and the container image
// sourced from the Component's Workload resource.
func (r *Reconciler) ensureSandboxTemplate(
	ctx context.Context,
	comp *openchoreov1alpha1.Component,
	params *agentParams,
	container *openchoreov1alpha1.Container,
) error {
	logger := log.FromContext(ctx)
	name := sandboxTemplateName(comp.Name)

	podSpec := buildSandboxPodSpec(params, container)

	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(sandboxTemplateGVK)
	desired.SetName(name)
	desired.SetNamespace(comp.Namespace)
	desired.SetLabels(map[string]string{
		labelManagedBy: "agent-sandbox-controller",
		labelComponent: comp.Name,
	})
	if err := unstructured.SetNestedField(desired.Object, podSpec, "spec", "podTemplate", "spec"); err != nil {
		return fmt.Errorf("failed to build SandboxTemplate spec: %w", err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(sandboxTemplateGVK)
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: comp.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return fmt.Errorf("failed to create SandboxTemplate %q: %w", name, err)
		}
		logger.Info("Created SandboxTemplate", "name", name, "isolationTier", params.IsolationTier)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get SandboxTemplate %q: %w", name, err)
	}

	// Only patch when something meaningful has changed (e.g., new image after a build).
	existingPodSpec, _, _ := unstructured.NestedMap(existing.Object, "spec", "podTemplate", "spec")
	if fmt.Sprintf("%v", existingPodSpec) == fmt.Sprintf("%v", podSpec) {
		return nil // no change
	}

	existing.SetLabels(desired.GetLabels())
	if err := unstructured.SetNestedField(existing.Object, podSpec, "spec", "podTemplate", "spec"); err != nil {
		return fmt.Errorf("failed to set updated SandboxTemplate spec: %w", err)
	}
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("failed to update SandboxTemplate %q: %w", name, err)
	}
	logger.Info("Updated SandboxTemplate", "name", name)
	return nil
}

// ensureSandboxClaim creates or updates the SandboxClaim for a Component+environment pair.
// The claim references the component's SandboxTemplate and carries the sandbox-policy label
// via additionalPodMetadata so the resulting Sandbox pod is targeted by the NetworkPolicy.
func (r *Reconciler) ensureSandboxClaim(
	ctx context.Context,
	comp *openchoreov1alpha1.Component,
	params *agentParams,
	env string,
) error {
	logger := log.FromContext(ctx)
	name := sandboxClaimName(comp.Name, env)
	templateName := sandboxTemplateName(comp.Name)

	// Labels propagated to the Sandbox pod via additionalPodMetadata.
	podLabels := map[string]interface{}{
		labelComponent:   comp.Name,
		labelEnvironment: env,
	}
	if params.SandboxPolicyRef != "" {
		podLabels[labelPolicyRef] = params.SandboxPolicyRef
	}

	claimLabels := map[string]string{
		labelManagedBy:   "agent-sandbox-controller",
		labelComponent:   comp.Name,
		labelEnvironment: env,
	}
	if params.SandboxPolicyRef != "" {
		claimLabels[labelPolicyRef] = params.SandboxPolicyRef
	}

	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(sandboxClaimGVK)
	desired.SetName(name)
	desired.SetNamespace(comp.Namespace)
	desired.SetLabels(claimLabels)

	if err := unstructured.SetNestedField(desired.Object,
		templateName, "spec", "sandboxTemplateRef", "name"); err != nil {
		return fmt.Errorf("failed to set sandboxTemplateRef: %w", err)
	}
	if err := unstructured.SetNestedMap(desired.Object,
		podLabels, "spec", "additionalPodMetadata", "labels"); err != nil {
		return fmt.Errorf("failed to set additionalPodMetadata.labels: %w", err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(sandboxClaimGVK)
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: comp.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return fmt.Errorf("failed to create SandboxClaim %q: %w", name, err)
		}
		logger.Info("Created SandboxClaim", "name", name, "env", env, "template", templateName)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get SandboxClaim %q: %w", name, err)
	}

	// SandboxClaim.spec.sandboxTemplateRef is immutable once the claim is Bound.
	if isSandboxClaimBound(existing) {
		return nil
	}

	// Update labels (e.g., sandboxPolicyRef changed).
	existing.SetLabels(desired.GetLabels())
	if err := unstructured.SetNestedMap(existing.Object,
		podLabels, "spec", "additionalPodMetadata", "labels"); err != nil {
		return fmt.Errorf("failed to update additionalPodMetadata.labels: %w", err)
	}
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("failed to update SandboxClaim %q: %w", name, err)
	}
	logger.Info("Updated SandboxClaim", "name", name)
	return nil
}

// fetchSandboxClaim retrieves the SandboxClaim for a given Component+environment.
func (r *Reconciler) fetchSandboxClaim(
	ctx context.Context,
	compName, namespace, env string,
) (*unstructured.Unstructured, error) {
	sc := &unstructured.Unstructured{}
	sc.SetGroupVersionKind(sandboxClaimGVK)
	if err := r.Get(ctx, types.NamespacedName{
		Name:      sandboxClaimName(compName, env),
		Namespace: namespace,
	}, sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// isSandboxClaimBound returns true when the SandboxClaim has a Bound=True condition.
func isSandboxClaimBound(sc *unstructured.Unstructured) bool {
	conditions, found, _ := unstructured.NestedSlice(sc.Object, "status", "conditions")
	if !found {
		return false
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cond["type"] == "Bound" && cond["status"] == "True" {
			return true
		}
	}
	return false
}

// cleanupSandboxResources deletes all SandboxClaims labelled for the Component and
// its SandboxTemplate.  Not-found errors are ignored (idempotent).
func (r *Reconciler) cleanupSandboxResources(
	ctx context.Context,
	comp *openchoreov1alpha1.Component,
) error {
	logger := log.FromContext(ctx)

	// Delete SandboxClaims labelled with this component.
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(sandboxClaimListGVK)
	if err := r.List(ctx, list,
		client.InNamespace(comp.Namespace),
		client.MatchingLabels{labelComponent: comp.Name},
	); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to list SandboxClaims for cleanup: %w", err)
	}
	for i := range list.Items {
		if err := r.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("failed to delete SandboxClaim %q: %w", list.Items[i].GetName(), err)
		}
		logger.Info("Deleted SandboxClaim", "name", list.Items[i].GetName())
	}

	// Delete the SandboxTemplate.
	st := &unstructured.Unstructured{}
	st.SetGroupVersionKind(sandboxTemplateGVK)
	st.SetName(sandboxTemplateName(comp.Name))
	st.SetNamespace(comp.Namespace)
	if err := r.Delete(ctx, st); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete SandboxTemplate %q: %w", st.GetName(), err)
	}
	logger.Info("Deleted SandboxTemplate", "name", st.GetName())

	return nil
}

// buildSandboxPodSpec builds the pod spec map for the SandboxTemplate from the workload
// Container spec and the agent isolation parameters.
func buildSandboxPodSpec(params *agentParams, container *openchoreov1alpha1.Container) map[string]interface{} {
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
		cmds := make([]interface{}, len(container.Command))
		for i, c := range container.Command {
			cmds[i] = c
		}
		containerSpec["command"] = cmds
	}
	if len(container.Args) > 0 {
		args := make([]interface{}, len(container.Args))
		for i, a := range container.Args {
			args[i] = a
		}
		containerSpec["args"] = args
	}
	if len(container.Env) > 0 {
		envVars := make([]interface{}, 0, len(container.Env))
		for _, e := range container.Env {
			if e.Value != "" {
				envVars = append(envVars, map[string]interface{}{
					"name":  e.Key,
					"value": e.Value,
				})
			}
		}
		if len(envVars) > 0 {
			containerSpec["env"] = envVars
		}
	}

	podSpec := map[string]interface{}{
		"automountServiceAccountToken": false,
		"containers":                   []interface{}{containerSpec},
	}
	if rc := runtimeClassName(params.IsolationTier); rc != "" {
		podSpec["runtimeClassName"] = rc
	}

	return podSpec
}

// findComponentsForSandboxClaim maps a changed SandboxClaim back to its owning Component.
// Used as the watch mapper so the controller requeues when a claim is bound by the upstream.
func (r *Reconciler) findComponentsForSandboxClaim(
	_ context.Context, obj client.Object,
) []ctrl.Request {
	compName, ok := obj.GetLabels()[labelComponent]
	if !ok || compName == "" {
		return nil
	}
	return []ctrl.Request{{
		NamespacedName: types.NamespacedName{
			Name:      compName,
			Namespace: obj.GetNamespace(),
		},
	}}
}
