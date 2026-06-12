# Configuration & Architecture

The README covers installation and quick start. This document is the full parameter reference,
tuning and extension guide, architecture rationale, and troubleshooting reference for the
`autoscaling-keda` module and its `keda-based-scaling` trait.

---

## Parameter reference

All parameters live under `spec.traits[].parameters` on the component. The trait is named
`keda-based-scaling`, and the samples use the same value for `instanceName` — per-environment
overrides in the `ReleaseBinding` are keyed by whatever `instanceName` you pick.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enabled` | boolean | `false` | Activates the trait. Nothing is rendered unless this is `true` and the data plane backend is `keda`. |
| `minReplicas` | integer | `0` | Minimum replica count. `0` enables scale-to-zero. |
| `maxReplicas` | integer | `10` | Maximum replica count KEDA will scale up to. |
| `cooldownPeriod` | integer | `300` | Seconds all metrics must stay at zero before KEDA scales down to `minReplicas`. |
| `trigger.type` | string | `""` | KEDA scaler type (e.g. `cron`, `kafka`). Empty string selects HTTP mode. |
| `trigger.metadata` | map[string]string | `{}` | Scaler-specific metadata passed verbatim to the `ScaledObject` trigger. |
| `interceptorNamespace` | string | `keda` | Namespace where the KEDA HTTP Add-on interceptor is installed. |
| `interceptorService` | string | `keda-add-ons-http-interceptor-proxy` | Service name of the interceptor proxy. |
| `interceptorPort` | integer | `8080` | Port on `interceptorService` the interceptor listens on. |
| `interceptorMultiportService` | string | `keda-add-ons-http-interceptor-multiport` | Multiport front Service used for in-cluster wake (the ExternalName target). |
| `wakeablePorts` | integer[] | `[80, 3000, 5000, 8000, 8080, 8081, 8090, 9000, 9090]` | Ports the multiport Service exposes. An HTTP-mode endpoint must use one of these. |
| `interceptorScalerService` | string | `keda-add-ons-http-external-scaler` | The HTTP Add-on's external scaler Service (the companion `ScaledObject` pulls metrics from it). |
| `interceptorScalerPort` | integer | `9090` | Port on `interceptorScalerService`. |
| `requestRateTargetValue` | integer | `1` | Target requests/second per replica. KEDA scales up when rate exceeds this value. |
| `requestRateWindow` | string | `"1m"` | Sliding window over which request rate is measured. |
| `requestRateGranularity` | string | `"1s"` | Bucket granularity inside the window. |
| `readinessTimeout` | string | `"120s"` | How long the interceptor holds the first request while a pod cold-starts. |

### Per-environment overrides

The trait's `environmentConfigs` schema exposes a subset of the parameters that platform engineers
can override per environment from the `ReleaseBinding`, without touching the component definition:

```yaml
# environmentConfigs schema (minReplicas / maxReplicas / cooldownPeriod only)
environmentConfigs:
  openAPIV3Schema:
    type: object
    properties:
      minReplicas:
        type: integer
        minimum: 0
      maxReplicas:
        type: integer
        minimum: 1
      cooldownPeriod:
        type: integer
        minimum: 0
```

In the `ReleaseBinding`, set `traitEnvironmentConfigs` keyed by the trait `instanceName`
(`keda-based-scaling`):

```yaml
spec:
  traitEnvironmentConfigs:
    keda-based-scaling:
      minReplicas: 1      # never scale to zero in production
      maxReplicas: 10
      cooldownPeriod: 600
```

When any of these fields is present, it wins over the component's `parameters` value for that
environment. Fields absent from `environmentConfigs` (e.g. `trigger` or any HTTP metric knob)
cannot be overridden this way — change them on the component.

---

## How HTTP scaling behaves

HTTP mode scales on **request rate over a sliding window**, not instantaneous concurrency. The
interceptor counts all requests that arrived in the last `requestRateWindow` (default `1m`) at
`requestRateGranularity` resolution (default `1s`), and divides by the window length to get a
rate in requests/second.

KEDA scales up when that rate per replica exceeds `requestRateTargetValue` (default `1 req/s`),
and begins the scale-down countdown only when the rate reaches zero across the full window. That
means a service receiving even sparse traffic — say, one request every few seconds — holds a
non-zero rate across the window and does not prematurely scale to zero. It scales down only once
no request has arrived for the entire window, and then only after `cooldownPeriod` more seconds
of idle.

**Worked example.** One request per 10 seconds = 0.1 req/s. With `requestRateTargetValue: 1`,
that rate is below the target, so one replica is sufficient. The rate stays non-zero for 1 minute
after the last request, then KEDA starts the `cooldownPeriod` countdown before dropping to zero.
With the defaults (`window: 1m`, `cooldownPeriod: 300`), the service stays awake for roughly
6 minutes after the last request arrives.

**Cold starts and the 120s timeout.** A cold start on a warmed cluster takes 2-4 seconds. The
interceptor holds the first request in-flight until a pod passes readiness, bounded by
`readinessTimeout` (default `120s`). The trait also patches the external `HTTPRoute` to set a
`request` timeout of `120s` on the route, matching the interceptor's hold time so the gateway
does not drop the request first. Do not set `readinessTimeout` longer than your gateway's
absolute route timeout.

The first cold start after a component is deployed can be slower — KEDA registers the new scaler
against the `InterceptorRoute` during this window, and if the `ScaledObject` reconciles before
the `InterceptorRoute` exists, KEDA may briefly fall back to a CPU metric. Retry once; subsequent
cold starts are fast. See Troubleshooting for the fix if it persists.

**`cooldownPeriod` tuning.** Lower values reduce idle cost but increase cold-start frequency.
For services with a long start time, raise `readinessTimeout` and `cooldownPeriod` together to
avoid a cycle of scale-down/immediate-scale-up under light traffic.

---

## Extending the module

### Concurrency-based scaling

HTTP mode currently scales on `requestRate` (requests per second over a window). The KEDA HTTP
Add-on's `InterceptorRoute` also supports `concurrency` as a `scalingMetric`: target concurrent
in-flight requests per replica. To switch, edit the `InterceptorRoute` template in
`keda-based-scaling-trait.yaml` and replace the `requestRate` block under `scalingMetric` with
a `concurrency` block. See the add-on's `InterceptorRoute` reference:
https://github.com/kedacore/http-add-on/blob/main/docs/ref/v0.14.0/interceptor_route.md

Concurrency is useful when requests are long-lived (e.g. streaming or WebSocket) and rate
under-counts actual load.

### Any KEDA scaler for workers and event-driven services

`trigger.type` and `trigger.metadata` are passed straight through to the `ScaledObject`, so
every scaler in the KEDA catalog works: cron, kafka, rabbitmq, prometheus, aws-sqs, azure-servicebus,
and 70+ others. Full list: https://keda.sh/docs/latest/scalers/

Example — RabbitMQ worker:

```yaml
parameters:
  enabled: true
  minReplicas: 0
  maxReplicas: 5
  trigger:
    type: rabbitmq
    metadata:
      queueName: jobs
      hostFromEnv: RABBITMQ_URL   # injected by a connection binding
```

For brokered scalers, use `hostFromEnv` pointing at an env var your workload connection already
injects. Broker credentials stay out of the component definition and out of git.

### Authenticated triggers

KEDA scalers reference credentials through `triggers[].authenticationRef`, pointing at a
`TriggerAuthentication`/`ClusterTriggerAuthentication` object. The trait does not expose
`authenticationRef` as a parameter today — to use it, extend the trigger-mode `ScaledObject`
template in `keda-based-scaling-trait.yaml`. The module's RBAC already lets the cluster-agent
manage `triggerauthentications`/`clustertriggerauthentications`, so no extra permissions are
needed. See the KEDA authentication reference:
https://keda.sh/docs/latest/concepts/authentication/

Where possible, prefer `hostFromEnv`-style metadata (above) over authenticated triggers — it
needs no extra objects.

### Wakeable ports

The default wakeable ports are `[80, 3000, 5000, 8000, 8080, 8081, 8090, 9000, 9090]`. An
HTTP-mode endpoint must listen on one of these because the component's Service is turned into
an ExternalName DNS CNAME — a CNAME cannot remap ports, so the caller-facing port must be a port
the interceptor already answers on.

To add a port:

1. Add it to `keda-interceptor-multiport.yaml` (the `ports` list on the Service) and re-apply.
2. Add it to the `wakeablePorts` default in `keda-based-scaling-trait.yaml` and re-apply the
   trait.

Keep the two in sync. The trait's validation rule checks `ep.port in parameters.wakeablePorts`
and rejects a component at render time if the endpoint port is not in the list.

### Interceptor installed elsewhere

If the KEDA HTTP Add-on is installed in a namespace other than `keda`, or with non-default
Service names or ports, set the corresponding parameters on the trait attachment:

```yaml
parameters:
  interceptorNamespace: my-keda
  interceptorService: my-interceptor-proxy
  interceptorPort: 8080
  interceptorMultiportService: my-interceptor-multiport
  interceptorScalerService: my-external-scaler
  interceptorScalerPort: 9090
```

These values are per-component, so different components in the same cluster can point at
different interceptor installations.

### Other component types

The HTTP path requires a specific shape from the component type:

- A Deployment named `${metadata.name}`
- A Service named `${metadata.componentName}`
- Exactly one external `HTTPRoute` carrying the `openchoreo.dev/endpoint-visibility: external` label

Any `ClusterComponentType` that produces this shape can allow the trait by adding
`keda-based-scaling` to its `allowedTraits` list (delete + recreate, as described in the README's
Install step 3).

The trigger/worker path is simpler: it only needs the Deployment. Any deployment-based component
type works.

### Other gateways

The HTTP path is kgateway-specific. The trait routes to the interceptor through a
`gateway.kgateway.dev/Backend` in the component's namespace; this Backend requires no
cross-namespace `ReferenceGrant` because it is local to the workload namespace.

On another Gateway API implementation, adapt the `creates` entry for the Backend resource and
the HTTPRoute patch. The equivalent approach is a cross-namespace Service `backendRef` pointing
at the interceptor, plus a `ReferenceGrant` in the interceptor namespace permitting it.

### Advanced ScaledObject tuning

The trait does not expose every `ScaledObject` field as a parameter. Fields like `fallback`
(behavior when the scaler is unavailable) and
`advanced.horizontalPodAutoscalerConfig.behavior` (scale-up/scale-down stabilization windows)
can be added by editing the `ScaledObject` templates directly in the trait. Full spec reference:
https://keda.sh/docs/latest/reference/scaledobject-spec/

---

## Architecture

### Backend annotation contract

The trait reads `dataplane.annotations["openchoreo.dev/keda-based-scaling-backend"]` from the
render context (available from OpenChoreo 1.2 onward). When the value is `keda`, the trait
renders KEDA objects. When the annotation is absent or set to any other value, the trait is
completely inert — the component runs at its configured static replica count. No per-environment
forking is needed: the same component definition runs normally on a plane without KEDA and scales
to zero on a plane that advertises `keda`.

Annotate the data plane to activate:

```bash
kubectl annotate clusterdataplane default \
  openchoreo.dev/keda-based-scaling-backend=keda --overwrite
```

### Rendering modes

| Mode | Condition | Renders |
|---|---|---|
| **HTTP** | `enabled: true`, backend `keda`, `trigger.type == ""`, one external HTTP/GraphQL/WebSocket endpoint | `InterceptorRoute` + companion `ScaledObject`, kgateway `Backend`, pod-backing Service, patches to ExternalName the component's Service and repoint the `HTTPRoute` |
| **Trigger** | `enabled: true`, backend `keda`, `trigger.type != ""` | `ScaledObject` with the given trigger |

Both modes patch the Deployment to remove `spec.replicas`, handing replica ownership to KEDA.
If `spec.replicas` remained, server-side apply would reset it on every render and fight the
autoscaler.

### The 0.14 split model

HTTP scaling uses the KEDA HTTP Add-on 0.14 split model. An `InterceptorRoute`
(`http.keda.sh/v1beta1`) carries the routing rule (which hosts to intercept, which upstream to
forward to) and the scaling metric (`requestRate` with target, window, and granularity). A
companion `ScaledObject` carries the scale target (the Deployment) and the replica bounds
(`minReplicaCount: 0` is what enables scale-to-zero).

In 0.14, the `InterceptorRoute` controller does **not** create the `ScaledObject` automatically.
The trait renders both. The `InterceptorRoute` is rendered first so the external scaler has a
metric spec to report by the time the `ScaledObject` reconciles; if the `ScaledObject` arrives
first, KEDA may briefly fall back to CPU.

### In-cluster wake: the ExternalName alias mechanism

OpenChoreo connection bindings inject the callee's in-cluster Service URL
(`http://<component>.<ns>.svc.cluster.local:<port>`). Without intervention that URL goes directly
to the Deployment pods and is refused while the service sleeps.

In HTTP mode the trait closes that gap:

1. **Component Service → ExternalName.** The component's own Service (named
   `${metadata.componentName}`) is patched to `type: ExternalName`, with `externalName` pointing
   at `keda-add-ons-http-interceptor-multiport.<interceptorNamespace>.svc.cluster.local`. An
   in-cluster call to the injected URL now resolves to the interceptor.
2. **Pod-backing Service.** A separate ClusterIP Service (`${componentName}-keda-upstream`) is
   created with the original pod selector and ports. The `InterceptorRoute` forwards here after a
   pod is ready.
3. **Host-based routing.** The `InterceptorRoute` registers exactly one `rules[].hosts` entry:
   the component's FQDN (`<componentName>.<namespace>.svc.cluster.local`). The HTTPRoute patch
   adds a `urlRewrite.hostname` filter rewriting the outbound Host header to this FQDN, so
   gateway traffic and in-cluster traffic match the same interceptor rule.

The Service is born as ExternalName on first render, so there is no ClusterIP → ExternalName
mutation to trip over. If you convert a long-lived component and the data-plane apply rejects the
in-place Service type change, redeploy the component so the Service is recreated from scratch.

### Why exactly one HTTP endpoint

Two intrinsic constraints force the single-endpoint shape:

**One Service, one Host.** OpenChoreo gives a component a single Service (one DNS name, possibly
many ports). Connection bindings inject that one name. Every endpoint on the component is reached
as the same host (`<component>.<ns>.svc.cluster.local`), differing only by port. The KEDA
interceptor routes purely by the `Host` header — it strips the port first. So it cannot
distinguish two endpoints on the same component: a second endpoint would be silently forwarded to
the wrong port. Hence exactly one endpoint.

**A CNAME cannot remap ports.** An ExternalName Service is a DNS CNAME. A caller hitting the
component on its endpoint port lands on the interceptor on that same port. So the endpoint port
must be one the interceptor already answers on. The multiport Service (`keda-interceptor-multiport.yaml`)
fronts the interceptor on a set of common ports precisely to avoid pinning every service to a
single port. Hence the endpoint port must be in `wakeablePorts`.

A service that does not fit (multiple endpoints, or a port outside `wakeablePorts`) has three
escape hatches:

- Split extra endpoints into a separate component, each with its own trait attachment.
- Add the port to `keda-interceptor-multiport.yaml` and `wakeablePorts`.
- Keep `minReplicas >= 1` on the component (no scale-to-zero, no ExternalName takeover).

---

## Limitations

- **Exactly one external HTTP endpoint per service in HTTP mode.** The interceptor routes by
  `Host` only (port stripped), and the ExternalName alias is a DNS CNAME that cannot remap ports.
  See the Architecture section for the full rationale and escape hatches.
- **The HTTP path is kgateway-specific.** The trait routes to the interceptor through a
  `gateway.kgateway.dev/Backend`. On a different Gateway API implementation, adapt the Backend
  resource and the HTTPRoute patch (cross-namespace Service backendRef + `ReferenceGrant` works).
- **Component types are a 1.2 snapshot.** The `componenttypes/service.yaml` and
  `componenttypes/worker.yaml` files mirror the stock OpenChoreo 1.2 defaults. If your platform
  customizes those types, regenerate from your live types instead of applying the committed copies.
- **Mutually exclusive with HPA-style traits.** Both claim ownership of the Deployment's replica
  count. Do not attach both an HPA-style trait and `keda-based-scaling` to the same component.
- **Cilium-signal backend** (`backend: cilium`) is a future path and is not implemented.

---

## Troubleshooting

- **Nothing scales / no KEDA objects rendered.** Confirm the data plane annotation:
  ```bash
  kubectl get clusterdataplane default -o jsonpath='{.metadata.annotations}'
  ```
  Without `openchoreo.dev/keda-based-scaling-backend=keda` the trait is intentionally inert.
  Also confirm the trait is attached under `spec.traits` and that the component type allows it:
  ```bash
  kubectl get clustercomponenttype service -o jsonpath='{.spec.allowedTraits[*].name}'
  ```

- **First request returns a 504 or hangs for ~60s.** Usually the KEDA scaling pipeline warming
  up for a newly-created `InterceptorRoute`/`ScaledObject`, or the interceptor/scaler pods not
  Ready yet. Wait for the `keda-add-ons-http-*` rollouts and retry — later cold starts complete
  in a few seconds.

- **HTTP service never wakes / scale-up doesn't work; the HPA shows a `cpu` metric.** The
  companion `ScaledObject` was reconciled before its `InterceptorRoute` existed, so the external
  scaler returned an empty metric spec and KEDA fell back to CPU. The trait renders the
  `InterceptorRoute` first, but if you hit this, re-apply the binding (or delete the
  `ScaledObject`) so KEDA re-reads the route's metric. Confirm with:
  ```bash
  kubectl get scaledobject -n <wl-ns> -o jsonpath='{.items[0].spec.triggers}'
  ```

- **Every request 504s after ~60s, even though the pod wakes and is Ready.** The interceptor
  pods are missing the `openchoreo.dev/system-component` label, so the component's NetworkPolicy
  drops the interceptor's forwards. Re-install the HTTP add-on with the `additionalLabels` flag
  from the README and check:
  ```bash
  kubectl get pods -n keda --show-labels
  ```

- **Request returns 404 or 503 after scaling to zero.** Check the kgateway `Backend` exists in
  the workload namespace and the `HTTPRoute` backendRef was repointed at it:
  ```bash
  kubectl get backends.gateway.kgateway.dev,httproute -n <wl-ns> -o yaml
  ```

- **Agent can't create ScaledObjects (RBAC forbidden in cluster-agent logs).** Confirm the RBAC
  was applied and bound to the right ServiceAccount:
  ```bash
  kubectl get clusterrolebinding openchoreo-cluster-agent-keda -o yaml
  ```

- **In-cluster (service-to-service) calls don't wake the service.** Confirm the callee's Service
  became an ExternalName:
  ```bash
  kubectl get svc <component> -n <wl-ns> -o jsonpath='{.spec.type}'   # should print ExternalName
  ```
  Confirm the in-cluster FQDN appears in the `InterceptorRoute` `rules[].hosts`. Then check the
  multiport front actually selects the interceptor pods:
  ```bash
  kubectl get endpoints keda-add-ons-http-interceptor-multiport -n keda
  ```
  If that is empty, the selector in `keda-interceptor-multiport.yaml` does not match your
  add-on's interceptor pod labels. Compare with:
  ```bash
  kubectl get svc keda-add-ons-http-interceptor-proxy -n keda -o jsonpath='{.spec.selector}'
  ```
