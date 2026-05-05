# Copyright 2026 The OpenChoreo Authors
# SPDX-License-Identifier: Apache-2.0

# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.26.1-alpine3.23 AS builder

WORKDIR /workspace

# Cache dependencies first — only re-downloaded when go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build the manager binary.
COPY api/       api/
COPY cmd/       cmd/
COPY internal/  internal/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -a \
      -ldflags="-w -s" \
      -o manager \
      ./cmd

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY --from=builder /workspace/manager .

USER 65532:65532

ENTRYPOINT ["/manager"]
