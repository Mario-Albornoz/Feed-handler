# Integration test

`integration_test.go` builds the real aggregator binary, starts it against a local Kafka,
publishes simulator-format JSON messages (including `Seq`) and prints a checklist of what
worked and what did not, so a failure points at a stage rather than at "the run looks wrong".

## Prerequisites

- Kafka reachable at `localhost:9092` (e.g. `docker compose -f docker-compose.test.yml up -d`, wait ~30 s)
- Go 1.22+

## Run

```bash
INTEGRATION_TEST=1 go test ./test/integration/... -v -timeout=5m
```

Without `INTEGRATION_TEST=1` the test is skipped.

## What it checks (20 items)

- the last traded price survives the wire format (simulator JSON -> handler -> vector)
- feature vectors are produced for trades and keyed by instrument (partition-key consistency)
- a rewound timestamp is quarantined, logged in the validation log, and does not look like silence
- a silence is reported exactly once, with the expected detection time, and resumes cleanly
- `Seq` is carried from the message to the vector
- the alert logs are written in the format `evaluate_thesis.py` reads
- the aggregator shuts down gracefully on SIGTERM and commits only what it processed

Each check is printed as PASS/FAIL in the checklist at the end of the test output.

## Related tools

- `scripts/smoke_run.py` (repo root): whole chain on a small slice of the data
- `rrcf-detector/scripts/verify_run.py`: stage-by-stage report on a finished run
