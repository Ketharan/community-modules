# Autoscaling with KEDA

Scale OpenChoreo components on demand with [KEDA](https://keda.sh). HTTP services scale to zero
when idle and wake on the first request — the KEDA HTTP Add-on interceptor holds it, nothing is
dropped; in-cluster service-to-service calls wake them too. Workers scale on cron or queue triggers.
You attach the `keda-based-scaling` trait to a plain service or worker component and set a few
parameters.

**What's in the module:**

- `keda-based-scaling-trait.yaml` — the `ClusterTrait` that renders the right KEDA objects per
  data plane
- `cluster-agent-keda-rbac.yaml` — RBAC so the data-plane cluster-agent can manage KEDA objects
- `keda-interceptor-multiport.yaml` — a multi-port Service fronting the interceptor for in-cluster
  wake
- `componenttypes/service.yaml` + `componenttypes/worker.yaml` — reference examples showing the
  `keda-based-scaling` entry added to `allowedTraits`; patch your own types rather than applying these

KEDA core and the KEDA HTTP Add-on install from their upstream Helm charts (see Install below) —
this module provides only the OpenChoreo glue.

---

## How it works

A data plane opts in by carrying the annotation
`openchoreo.dev/keda-based-scaling-backend: keda`. The trait reads that annotation at render time;
on planes without it the trait is inert, so a component definition is portable across environments.

| Mode | When | What KEDA creates |
|------|------|-------------------|
| **HTTP** | service with one external HTTP endpoint, no `trigger.type` | `InterceptorRoute` + companion `ScaledObject`; gateway and in-cluster traffic both wake the service |
| **Trigger** | any component with `trigger.type` set | `ScaledObject` with that trigger (cron, kafka, rabbitmq, …) |

Requires OpenChoreo 1.2+. Full architecture, tuning, and extension details: [CONFIGURATION.md](./CONFIGURATION.md).

---

## Prerequisites

- An OpenChoreo 1.2+ control plane and data plane. For a fresh local cluster, follow the
  [single-cluster k3d guide](https://github.com/openchoreo/openchoreo/blob/main/install/k3d/single-cluster/README.md)
  through step 5.
- `kubectl`, `helm`, `jq`, `yq`, and cluster access.
- The data-plane gateway must be **kgateway** (the OpenChoreo default). For other Gateway API
  implementations, see [CONFIGURATION.md](./CONFIGURATION.md).

---

## Install

### 1. Install KEDA core, then the HTTP Add-on

KEDA's CRDs must be established before the HTTP Add-on's resources are applied, so install them as
two separate releases.

```bash
helm repo add kedacore https://kedacore.github.io/charts && helm repo update kedacore

# KEDA core
helm upgrade --install keda kedacore/keda --version 2.20.1 \
  --namespace keda --create-namespace
kubectl wait --for=condition=Established \
  crd/scaledobjects.keda.sh crd/triggerauthentications.keda.sh --timeout=120s
kubectl wait --for=condition=Available deployment/keda-operator -n keda --timeout=180s

# KEDA HTTP Add-on (interceptor + scaler)
helm upgrade --install keda-add-ons-http kedacore/keda-add-ons-http --version 0.14.1 \
  --namespace keda \
  --set 'additionalLabels.openchoreo\.dev/system-component=keda-http-addon'
kubectl wait --for=condition=Established crd/interceptorroutes.http.keda.sh --timeout=120s
```

> The `additionalLabels` flag is required. OpenChoreo renders a NetworkPolicy per component that
> only admits traffic from the same namespace or from pods carrying the
> `openchoreo.dev/system-component` label. Without it, on any cluster whose CNI enforces
> NetworkPolicy (k3s/k3d included), the interceptor's forwards are silently dropped and every
> request times out with a 504 — even though the wake-up itself succeeds.

### 2. Apply the trait and data-plane resources

```bash
kubectl apply --server-side -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/keda-based-scaling-trait.yaml

kubectl apply \
  -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/cluster-agent-keda-rbac.yaml \
  -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/keda-interceptor-multiport.yaml
```

### 3. Enable the trait on your component types

Patch `keda-based-scaling` into `allowedTraits` on the component types you use. No components
need to be deleted or recreated — the change takes effect immediately:

```bash
kubectl patch clustercomponenttype <your-type> --type=json \
  -p '[{"op":"add","path":"/spec/allowedTraits/-","value":{"kind":"ClusterTrait","name":"keda-based-scaling"}}]'
```

`componenttypes/service.yaml` and `componenttypes/worker.yaml` in this module are reference
examples showing the full type definition with the trait included — use them as a guide, not as
a replacement for your own types. See [CONFIGURATION.md](./CONFIGURATION.md) for the shape
requirements a component type must satisfy to be compatible with the HTTP scaling path.

### 4. Annotate the data plane

```bash
kubectl annotate clusterdataplane default \
  openchoreo.dev/keda-based-scaling-backend=keda --overwrite
```

### 5. Verify

```bash
kubectl rollout status deploy/keda-add-ons-http-interceptor -n keda --timeout=180s
kubectl rollout status deploy/keda-add-ons-http-external-scaler -n keda --timeout=180s

kubectl get clustertrait keda-based-scaling
kubectl get clustercomponenttype service worker \
  -o jsonpath='{range .items[*]}{.metadata.name}{": "}{.spec.allowedTraits[*].name}{"\n"}{end}'
kubectl get clusterrolebinding openchoreo-cluster-agent-keda

# Non-empty Endpoints means the interceptor multiport Service is working:
kubectl get endpoints keda-add-ons-http-interceptor-multiport -n keda
```

---

## Quick start

### HTTP service that scales to zero

```bash
kubectl apply -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/samples/http-service.yaml
```

Find the workload namespace OpenChoreo created (re-run if empty — it appears once the
ReleaseBinding has rendered):

```bash
WL_NS=$(kubectl get ns -o name | sed 's|namespace/||' | grep '^dp-default-default-development')
echo "$WL_NS"
```

Watch it scale to zero (after ~30s of no traffic):

```bash
kubectl get deploy -n "$WL_NS" -w        # replicas -> 0, then Ctrl-C
```

Hit it — the interceptor holds the request and wakes a pod:

```bash
curl http://development-default.openchoreoapis.localhost:19080/greeter-http/
# Hello from OpenChoreo Autoscaling with KEDA!
```

```bash
kubectl get deploy -n "$WL_NS"           # replicas back to 1 right after the request
```

The very first cold start after deploying can be slow or return a 504 while KEDA registers the new
scaler — retry once. Subsequent cold starts take ~2-4s; the trait sets a 120s route timeout so the
gateway holds the request through them.

### Cron worker that scales to zero

```bash
kubectl apply -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/samples/cron-worker.yaml
```

The worker runs at 1 replica inside its cron window (`03:00–04:00 UTC` in the sample) and sits at
zero the other 23 hours — so right after applying it you see the scaled-to-zero state:

```bash
WL_NS=$(kubectl get ns -o name | sed 's|namespace/||' | grep '^dp-default-default-development')
kubectl get scaledobject -n "$WL_NS"
kubectl get deploy -n "$WL_NS"
```

To prove scaling quickly, edit the component's `trigger.metadata.start`/`end` to a window starting
a couple of minutes from now and watch the Deployment go `0 -> 1 -> 0`. Swap `type`/`metadata` for
any [KEDA scaler](https://keda.sh/docs/latest/scalers/) (`rabbitmq`, `kafka`, `prometheus`, …).

---

## Usage

Attach the trait under `spec.traits` on your component:

```yaml
spec:
  componentType:
    kind: ClusterComponentType
    name: deployment/service          # or deployment/worker
  traits:
    - kind: ClusterTrait
      name: keda-based-scaling
      instanceName: keda-based-scaling
      parameters:
        enabled: true
        minReplicas: 0
        maxReplicas: 5
        cooldownPeriod: 300           # seconds idle before scaling down
        trigger:                      # omit for HTTP services; required for workers
          type: cron
          metadata:
            timezone: "Etc/UTC"
            start: "0 8 * * *"
            end: "0 20 * * *"
            desiredReplicas: "1"
```

Platform engineers can floor the bounds per environment from the `ReleaseBinding`:

```yaml
spec:
  traitEnvironmentConfigs:
    keda-based-scaling:
      minReplicas: 1        # never scale to zero in production
      maxReplicas: 10
```

Full parameter reference, tuning (request rate vs. concurrency), extension points, architecture,
and troubleshooting are in [CONFIGURATION.md](./CONFIGURATION.md).

---

## Limitations

- **One external HTTP endpoint per service** — the interceptor routes by `Host` only (port
  stripped), and an ExternalName alias can't remap ports; see CONFIGURATION.md.
- **HTTP path is kgateway-specific** — uses `gateway.kgateway.dev/Backend`; adapt for other
  Gateway API implementations per CONFIGURATION.md.
- **Mutually exclusive with HPA traits** — both claim the Deployment's replica count; don't attach
  both to the same component.

---

## Uninstall

```bash
# Detach the trait from your components first (remove the keda-based-scaling entry from
# spec.traits), then:
kubectl annotate clusterdataplane default openchoreo.dev/keda-based-scaling-backend-
kubectl delete \
  -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/keda-interceptor-multiport.yaml \
  -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/cluster-agent-keda-rbac.yaml \
  -f https://raw.githubusercontent.com/openchoreo/community-modules/main/autoscaling-keda/keda-based-scaling-trait.yaml
# To revert the component types, remove keda-based-scaling from allowedTraits:
#   kubectl patch clustercomponenttype service worker --type=json \
#     -p '[{"op":"remove","path":"/spec/allowedTraits/<index>"}]'
#   (find the index with: kubectl get clustercomponenttype service -o jsonpath='{.spec.allowedTraits}')
# optionally:
# helm uninstall keda-add-ons-http keda -n keda
```
