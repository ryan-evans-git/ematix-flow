# ematix-flow

> Declarative table management and load strategies (SCD1, SCD2, append-only,
> merge/upsert, truncate+replace) for Postgres, with a Rust core and a Python
> API.

**Status: pre-alpha.** Phase 0 scaffolding only. Not yet usable.

See [`docs/PRD.md`](docs/PRD.md) and
[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) for the v0.1
design and roadmap.

## Quickstart (planned)

```python
from ematix_flow import ManagedTable, Column, Integer, String, Timestamp, pipeline

class CustomerDim(ManagedTable):
    __tablename__ = "dim_customers"
    __schema__ = "analytics"

    customer_id = Column(Integer, primary_key=True)
    email       = Column(String)
    status      = Column(String)
    updated_at  = Column(Timestamp)

pipeline.sync(
    source="select * from raw.customers",
    target=CustomerDim,
    mode="scd2",
    keys=["customer_id"],
    compare_columns=["email", "status"],
)
```

## Development

```sh
# Build Rust workspace
cargo build

# Build + install Python extension into a venv
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop

# Run tests
cargo test
pytest
```

## License

Apache-2.0
