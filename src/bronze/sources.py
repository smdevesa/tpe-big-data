from dataclasses import dataclass, field

INGEST_DATE = "ingest_date"


@dataclass(frozen=True)
class BatchSource:
    name: str
    key: str
    columns: tuple[str, ...]
    types: dict[str, str] = field(default_factory=dict)
    partition_by: str = INGEST_DATE

    @property
    def file_name(self):
        return f"{self.name}.csv"

    @property
    def required(self):
        if self.partition_by == INGEST_DATE:
            return (self.key,)
        return (self.key, self.partition_by)


CUSTOMERS_ORGS = BatchSource(
    name="customers_orgs",
    key="org_id",
    columns=(
        "org_id", "org_name", "industry", "hq_region", "plan_tier", "is_enterprise",
        "signup_date", "sales_rep", "lifecycle_stage", "marketing_source", "nps_score",
    ),
    types={"is_enterprise": "boolean", "signup_date": "date", "nps_score": "double"},
)

USERS = BatchSource(
    name="users",
    key="user_id",
    columns=("user_id", "org_id", "email", "role", "active", "created_at", "last_login"),
    types={"active": "boolean", "created_at": "date", "last_login": "date"},
)

RESOURCES = BatchSource(
    name="resources",
    key="resource_id",
    columns=("resource_id", "org_id", "service", "region", "created_at", "state", "tags_json"),
    types={"created_at": "date"},
)

BILLING_MONTHLY = BatchSource(
    name="billing_monthly",
    key="invoice_id",
    columns=(
        "invoice_id", "org_id", "month", "subtotal", "credits", "taxes", "currency", "exchange_rate_to_usd",
    ),
    types={
        "month": "date",
        "subtotal": "decimal(12,2)",
        "credits": "decimal(12,2)",
        "taxes": "decimal(12,2)",
        "exchange_rate_to_usd": "decimal(12,6)",
    },
    partition_by="month",
)

BATCH_SOURCES = (CUSTOMERS_ORGS, USERS, RESOURCES, BILLING_MONTHLY)
