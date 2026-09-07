"""Raw Olist CSV contracts used before every seed run."""

from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TableContract:
    table_name: str
    file_name: str
    raw_header: tuple[str, ...]
    selected_columns: tuple[str, ...]
    primary_key: tuple[str, ...]


TABLE_CONTRACTS = (
    TableContract(
        table_name="customers",
        file_name="olist_customers_dataset.csv",
        raw_header=(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
        selected_columns=("customer_id", "customer_unique_id", "customer_city", "customer_state"),
        primary_key=("customer_id",),
    ),
    TableContract(
        table_name="orders",
        file_name="olist_orders_dataset.csv",
        raw_header=(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        selected_columns=(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        primary_key=("order_id",),
    ),
    TableContract(
        table_name="order_items",
        file_name="olist_order_items_dataset.csv",
        raw_header=(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ),
        selected_columns=(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
        ),
        primary_key=("order_id", "order_item_id"),
    ),
    TableContract(
        table_name="order_payments",
        file_name="olist_order_payments_dataset.csv",
        raw_header=(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ),
        selected_columns=(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ),
        primary_key=("order_id", "payment_sequential"),
    ),
    TableContract(
        table_name="products",
        file_name="olist_products_dataset.csv",
        raw_header=(
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
        selected_columns=(
            "product_id",
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
        primary_key=("product_id",),
    ),
    TableContract(
        table_name="sellers",
        file_name="olist_sellers_dataset.csv",
        raw_header=("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
        selected_columns=("seller_id", "seller_city", "seller_state"),
        primary_key=("seller_id",),
    ),
)

CONTRACT_BY_TABLE = {contract.table_name: contract for contract in TABLE_CONTRACTS}


def validate_input_directory(input_dir: Path) -> dict[str, str]:
    """Check exact raw headers and return a deterministic checksum per required file."""
    checksums: dict[str, str] = {}
    for contract in TABLE_CONTRACTS:
        csv_path = input_dir / contract.file_name
        if not csv_path.is_file():
            raise ValueError(f"Required dataset file is missing: {csv_path}")
        with csv_path.open(encoding="utf-8-sig", newline="") as handle:
            header = tuple(next(csv.reader(handle), ()))
        if header != contract.raw_header:
            raise ValueError(
                f"Unexpected header for {contract.file_name}: expected {contract.raw_header}, got {header}"
            )
        with csv_path.open("rb") as handle:
            checksums[contract.file_name] = hashlib.file_digest(handle, "sha256").hexdigest()
    return checksums


def combined_checksum(file_checksums: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for file_name in sorted(file_checksums):
        digest.update(f"{file_name}:{file_checksums[file_name]}\n".encode())
    return digest.hexdigest()
