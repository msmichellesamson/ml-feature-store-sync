# ml-feature-store-sync

> A small reference implementation for studying train/serve skew in feature stores.

## The question I'm exploring

Train/serve skew — when the features your model sees in production don't match
what it was trained on — is one of the most-cited and least-solved problems in
production ML. Uber's Michelangelo paper, Tecton's blog posts, and Google's
TFX documentation all describe the same shape of bug: a feature pipeline
quietly diverges between offline (training) and online (serving), and model
quality silently degrades for weeks.

I wanted a sandbox where I could **reproduce** this kind of failure and
then **measure** different mitigations (versioning, lineage, dual-write
consistency checks) on the same workload.

## Why I care

Feature stores are infrastructure that nobody notices until they fail. When
they do fail, they fail in the worst possible way: silently, at the model
input layer, with no exception thrown. That's the same failure mode I find
most interesting across all of ML infra — slow, quiet quality degradation
rather than loud crashes.

This is also a place where **SRE practices** transfer well from web infra:
circuit breakers, dual writes with reconciliation, schema evolution rules,
canary deployments. I wanted to wire those into an ML context and see what
breaks.

## What's in here

A minimal but realistic setup:

- `src/core/feature_store.py` — Redis (online) + Postgres (offline) backed store
- `src/core/sync_engine.py` — handles dual writes, reconciliation, and
  consistency checks between the two layers
- `src/core/versioning.py` — schema versioning with backward-compatible
  evolution rules
- `src/core/drift_detector.py` — compares online vs offline feature
  distributions on a rolling window
- `src/api/` — FastAPI for feature reads/writes
- Kafka ingestion path for streaming features
- Prometheus metrics, structured logs, circuit breakers

Plus terraform for AWS, Dockerfile, docker-compose for local dev, k8s
manifests with HPA.

## What I'm finding (so far)

I haven't run a controlled study yet, but from building this:

- The **dual-write consistency problem** is the actual hard part. Either
  you write to Redis and Postgres atomically (slow, requires distributed
  transactions or a transactional outbox) or you accept eventual consistency
  and build reconciliation. I went with the latter because it's what most
  shops actually do, and I'm now sympathetic to why it generates incidents.
- Schema versioning is harder than it looks because feature definitions
  are code, not data. Renaming a feature is a migration that touches
  training, serving, and monitoring at the same time.
- Drift detection between online and offline feature distributions is
  cheap to compute but hard to interpret — most of the "drift" you see
  is actually the timing skew between batch ingestion and stream
  ingestion catching up.
- Backfill is a first-class concern that I underestimated when I started.
  Any change to a feature definition triggers a backfill, and backfill
  performance dominates everything else.

## What I'd do next

- Add a deliberate train/serve skew injection mode and measure how quickly
  the drift detector catches each kind (timing skew vs. logic skew vs.
  source-data skew)
- Replace the Postgres offline store with Iceberg + Spark to see whether
  the lakehouse pattern simplifies the dual-write problem
- Write up the operational runbook — the on-call experience for a feature
  store is what I actually want to understand
- Compare cost/complexity to using an existing managed solution (Feast,
  Tecton). Building this taught me a lot about why they make the
  tradeoffs they do.

## Status

The store works end-to-end as a single-node deployment. Multi-region
behaviour is unverified. The "studying skew" part of the goal hasn't
actually started — I built the substrate, not the experiment yet.

## References

- Hermann & Del Balso, *Meet Michelangelo: Uber's Machine Learning Platform* (2017)
- Polyzotis et al., *Data Lifecycle Challenges in Production Machine Learning* (SIGMOD 2018)
- Tecton blog, *The State of Feature Stores* series
- Sculley et al., *Hidden Technical Debt in Machine Learning Systems* (NeurIPS 2015)
