# YRBC-IRAP Model Service

`bank_model_server` is the Python model service of YRBC-IRAP (Yangtze River Bank Collapse Key Hazard Section Intelligent Risk Assessment Platform).

## Responsibilities

- keep the original model case execution mechanism;
- keep legacy model APIs such as `/v0/mi/risk-level`, `/v0/re/*`, `/v0/nm/*`;
- keep case/file APIs such as `/v0/mc/*` and `/v0/fs/*` that the upper layer can poll or download from;
- expose an extra synchronous wrapper API at `POST /api/v1/predict` for service-to-service use;
- do not own task CRUD, parameter-template CRUD, section CRUD, or other business workflow logic.

## Preserved original APIs

- model case APIs: `/v0/mc/*`, `/v0/mcs/*`
- model/file APIs: `/v0/fs/*`
- model execution APIs: `/v0/re/*`, `/v0/nm/*`, `/v0/mi/*`, `/v0/em/*`

## Added service APIs

- `GET /api/v1/health`
- `GET /api/v1/models`
- `POST /api/v1/predict`

## Run

```bash
uv sync
uv run python run.py
```

Swagger UI is available at `http://localhost:8088/docs`.
