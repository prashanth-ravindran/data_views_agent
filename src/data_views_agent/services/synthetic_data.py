from __future__ import annotations

import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd

from data_views_agent.models.contracts import OfficeManifest, OfficeRecord, SyntheticSheetMetadata, SyntheticWorkbookMetadata


FIRST_NAMES = [
    "Aarav",
    "Aditi",
    "Akshay",
    "Ananya",
    "Bhavna",
    "Chetan",
    "Ishita",
    "Kunal",
    "Manasi",
    "Neha",
    "Omkar",
    "Prajakta",
    "Rohan",
    "Sanya",
    "Tejas",
    "Vaishnavi",
]
LAST_NAMES = [
    "Deshmukh",
    "Jadhav",
    "Kulkarni",
    "Patil",
    "Pawar",
    "Shinde",
    "Bhosale",
    "Joshi",
    "More",
    "Naik",
    "Ghorpade",
    "Chavan",
]
DOCUMENT_TYPES = ["Sale Deed", "Gift Deed", "Agreement to Sale", "Lease Deed", "Mortgage Deed", "Release Deed"]
PROPERTY_TYPES = ["Flat", "Plot", "Agricultural Land", "Shop", "Office", "Industrial Unit", "House"]
PROPERTY_USAGES = ["Residential", "Commercial", "Industrial", "Agricultural", "Mixed Use"]
STATUSES = ["Registered", "Admitted", "Pending Indexing", "Released", "Impounded"]
LOCALITY_SUFFIXES = ["Nagar", "Gaon", "Colony", "Layout", "Park", "Wadi", "Peth", "Industrial Estate"]
PROJECT_PREFIXES = ["Sai", "Shree", "Skyline", "Orchid", "Golden", "Regency", "Metro", "Lakeview"]
PROJECT_TYPES = ["Heights", "Residency", "Enclave", "Plaza", "Park", "Court", "One", "Arcade"]

DIVISION_WEIGHTS = {
    "Mumbai": 2.7,
    "Thane": 2.4,
    "Pune": 2.3,
    "Nashik": 1.5,
    "Nagpur": 1.5,
    "Aurangabad": 1.2,
    "Amravati": 1.1,
}

SCHEMA_FAMILIES: dict[str, dict[str, dict[str, str] | list[str]]] = {
    "standard_registration": {
        "columns": [
            "office_code",
            "office_name",
            "division",
            "district",
            "registration_date",
            "document_number",
            "document_type",
            "property_type",
            "property_usage",
            "locality",
            "buyer_name",
            "seller_name",
            "consideration_value",
            "market_value",
            "stamp_duty",
            "registration_fee",
            "status",
        ],
        "canonical_map": {
            "office_code": "office_code",
            "office_name": "office_name",
            "division": "division",
            "district": "district",
            "registration_date": "registration_date",
            "document_number": "document_number",
            "document_type": "document_type",
            "property_type": "property_type",
            "property_usage": "property_usage",
            "locality": "locality",
            "buyer_name": "buyer_name",
            "seller_name": "seller_name",
            "consideration_value": "consideration_value",
            "market_value": "market_value",
            "stamp_duty": "stamp_duty",
            "registration_fee": "registration_fee",
            "status": "status",
        },
    },
    "rural_land_records": {
        "columns": [
            "sr_office_code",
            "sub_registrar_office",
            "division_name",
            "district_name",
            "exec_date",
            "deed_no",
            "article_type",
            "land_use",
            "taluka",
            "village_name",
            "gut_no",
            "purchaser",
            "vendor",
            "agreement_value",
            "ready_reckoner_value",
            "stamp_amt",
            "reg_fee",
            "area_hectare",
            "mutation_flag",
        ],
        "canonical_map": {
            "office_code": "sr_office_code",
            "office_name": "sub_registrar_office",
            "division": "division_name",
            "district": "district_name",
            "registration_date": "exec_date",
            "document_number": "deed_no",
            "document_type": "article_type",
            "property_usage": "land_use",
            "village": "village_name",
            "survey_number": "gut_no",
            "buyer_name": "purchaser",
            "seller_name": "vendor",
            "consideration_value": "agreement_value",
            "market_value": "ready_reckoner_value",
            "stamp_duty": "stamp_amt",
            "registration_fee": "reg_fee",
            "area_sqft": "area_hectare",
            "status": "mutation_flag",
        },
    },
    "urban_project_sales": {
        "columns": [
            "office_id",
            "sub_registrar_office",
            "district",
            "reg_date",
            "doc_no",
            "instrument_type",
            "project_name",
            "unit_type",
            "carpet_area_sqft",
            "ward",
            "buyer",
            "seller",
            "agreement_amount",
            "rr_value",
            "stamp_duty_paid",
            "registration_charges",
            "document_status",
        ],
        "canonical_map": {
            "office_code": "office_id",
            "office_name": "sub_registrar_office",
            "district": "district",
            "registration_date": "reg_date",
            "document_number": "doc_no",
            "document_type": "instrument_type",
            "project_name": "project_name",
            "property_type": "unit_type",
            "area_sqft": "carpet_area_sqft",
            "locality": "ward",
            "buyer_name": "buyer",
            "seller_name": "seller",
            "consideration_value": "agreement_amount",
            "market_value": "rr_value",
            "stamp_duty": "stamp_duty_paid",
            "registration_fee": "registration_charges",
            "status": "document_status",
        },
    },
    "commercial_lease_registry": {
        "columns": [
            "office_label",
            "district",
            "registration_date",
            "token_no",
            "doc_type",
            "property_use",
            "neighborhood",
            "unit_identifier",
            "party1",
            "party2",
            "market_value",
            "security_deposit",
            "stamp_amt",
            "fee_paid",
            "status",
        ],
        "canonical_map": {
            "office_name": "office_label",
            "district": "district",
            "registration_date": "registration_date",
            "document_number": "token_no",
            "document_type": "doc_type",
            "property_usage": "property_use",
            "locality": "neighborhood",
            "survey_number": "unit_identifier",
            "buyer_name": "party1",
            "seller_name": "party2",
            "market_value": "market_value",
            "stamp_duty": "stamp_amt",
            "registration_fee": "fee_paid",
            "status": "status",
        },
    },
    "legacy_extract": {
        "columns": [
            "office",
            "dist",
            "document_date",
            "registration_no",
            "article_type",
            "asset_type",
            "zone",
            "claimant_name",
            "executant_name",
            "plot_no",
            "valuation",
            "sd_paid",
            "reg_fee",
            "remarks",
        ],
        "canonical_map": {
            "office_name": "office",
            "district": "dist",
            "registration_date": "document_date",
            "document_number": "registration_no",
            "document_type": "article_type",
            "property_type": "asset_type",
            "locality": "zone",
            "buyer_name": "claimant_name",
            "seller_name": "executant_name",
            "survey_number": "plot_no",
            "market_value": "valuation",
            "stamp_duty": "sd_paid",
            "registration_fee": "reg_fee",
            "status": "remarks",
        },
    },
}


def _full_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _locality(office: OfficeRecord, rng: random.Random) -> str:
    token = office.district.replace("Mumbai ", "Mumbai").split()[0]
    return f"{token} {rng.choice(LOCALITY_SUFFIXES)}"


def _project_name(rng: random.Random) -> str:
    return f"{rng.choice(PROJECT_PREFIXES)} {rng.choice(PROJECT_TYPES)}"


def _weighted_office_count(office: OfficeRecord) -> float:
    weight = DIVISION_WEIGHTS.get(office.division, 1.0)
    lowered = office.office_name.lower()
    if "mumbai city" in lowered or "haveli" in lowered or "thane" in lowered or "nagpur city" in lowered:
        weight *= 1.35
    if "sub registrar" not in lowered and "registrar" in lowered:
        weight *= 0.5
    if lowered.startswith("dy.") or "joint district registrar" in lowered:
        weight *= 0.35
    return weight


def is_transaction_office(office: OfficeRecord) -> bool:
    lowered = office.office_name.lower()
    return "sub registrar" in lowered or "sub-registrar" in lowered or "sub registra" in lowered


def select_offices(
    manifest: OfficeManifest,
    max_offices: int | None = None,
    *,
    include_admin_offices: bool = False,
) -> list[OfficeRecord]:
    offices = manifest.offices if include_admin_offices else [office for office in manifest.offices if is_transaction_office(office)]
    if not max_offices or max_offices >= len(offices):
        return offices

    buckets: dict[str, list[OfficeRecord]] = defaultdict(list)
    for office in offices:
        buckets[office.division].append(office)

    selected: list[OfficeRecord] = []
    while len(selected) < max_offices and any(buckets.values()):
        for division in sorted(buckets):
            if not buckets[division]:
                continue
            selected.append(buckets[division].pop(0))
            if len(selected) >= max_offices:
                break
    return selected


def allocate_rows(offices: list[OfficeRecord], total_rows: int) -> list[int]:
    base_rows = max(5, total_rows // max(len(offices) * 12, 1))
    remaining = max(total_rows - base_rows * len(offices), 0)
    weights = [_weighted_office_count(office) for office in offices]
    total_weight = sum(weights) or 1.0

    counts = [base_rows + int(remaining * weight / total_weight) for weight in weights]
    allocated = sum(counts)
    deficit = total_rows - allocated
    if deficit > 0:
        ranked = sorted(range(len(offices)), key=lambda index: weights[index], reverse=True)
        for idx in ranked[:deficit]:
            counts[idx] += 1
    elif deficit < 0:
        ranked = sorted(range(len(offices)), key=lambda index: weights[index])
        for idx in ranked[: abs(deficit)]:
            counts[idx] = max(1, counts[idx] - 1)
    return counts


def generate_base_record(office: OfficeRecord, rng: random.Random, row_number: int) -> dict[str, object]:
    registration_date = datetime(2021, 1, 1) + timedelta(days=rng.randint(0, 365 * 5))
    property_type = rng.choice(PROPERTY_TYPES)
    property_usage = (
        "Commercial"
        if property_type in {"Shop", "Office"}
        else "Industrial"
        if property_type == "Industrial Unit"
        else "Agricultural"
        if property_type == "Agricultural Land"
        else "Residential"
    )
    area_sqft = round(rng.uniform(350, 2400), 2) if property_usage != "Agricultural" else round(rng.uniform(12000, 150000), 2)
    consideration = round(area_sqft * rng.uniform(2500, 18000), 2)
    market_value = round(consideration * rng.uniform(1.03, 1.35), 2)
    stamp_duty = round(market_value * rng.uniform(0.045, 0.075), 2)
    registration_fee = round(min(max(market_value * 0.01, 1000), 30000), 2)

    return {
        "office_code": office.office_code,
        "office_name": office.office_name,
        "division": office.division,
        "district": office.district,
        "registration_date": registration_date.date().isoformat(),
        "document_number": f"{office.serial_number:03d}/{registration_date.year}/{row_number:06d}",
        "document_type": rng.choice(DOCUMENT_TYPES),
        "property_type": property_type,
        "property_usage": property_usage,
        "locality": _locality(office, rng),
        "village": f"{office.district.split()[0]} {rng.choice(['Khurd', 'Budruk', 'Gaothan', 'Mauje'])}",
        "survey_number": f"{rng.randint(1, 999)}/{rng.randint(1, 9)}",
        "project_name": _project_name(rng),
        "buyer_name": _full_name(rng),
        "seller_name": _full_name(rng),
        "consideration_value": consideration,
        "market_value": market_value,
        "stamp_duty": stamp_duty,
        "registration_fee": registration_fee,
        "area_sqft": area_sqft,
        "status": rng.choice(STATUSES),
    }


def row_for_family(schema_family: str, base: dict[str, object], office: OfficeRecord) -> dict[str, object]:
    if schema_family == "standard_registration":
        return {column: base.get(column) for column in SCHEMA_FAMILIES[schema_family]["columns"]}  # type: ignore[index]

    if schema_family == "rural_land_records":
        area_hectare = round(float(base["area_sqft"]) / 107639.104, 6)
        return {
            "sr_office_code": base["office_code"],
            "sub_registrar_office": base["office_name"],
            "division_name": base["division"],
            "district_name": base["district"],
            "exec_date": base["registration_date"],
            "deed_no": base["document_number"],
            "article_type": base["document_type"],
            "land_use": base["property_usage"],
            "taluka": office.district,
            "village_name": base["village"],
            "gut_no": base["survey_number"],
            "purchaser": base["buyer_name"],
            "vendor": base["seller_name"],
            "agreement_value": base["consideration_value"],
            "ready_reckoner_value": base["market_value"],
            "stamp_amt": base["stamp_duty"],
            "reg_fee": base["registration_fee"],
            "area_hectare": area_hectare,
            "mutation_flag": base["status"],
        }

    if schema_family == "urban_project_sales":
        return {
            "office_id": base["office_code"],
            "sub_registrar_office": base["office_name"],
            "district": base["district"],
            "reg_date": base["registration_date"],
            "doc_no": base["document_number"],
            "instrument_type": base["document_type"],
            "project_name": base["project_name"],
            "unit_type": base["property_type"],
            "carpet_area_sqft": base["area_sqft"],
            "ward": base["locality"],
            "buyer": base["buyer_name"],
            "seller": base["seller_name"],
            "agreement_amount": base["consideration_value"],
            "rr_value": base["market_value"],
            "stamp_duty_paid": base["stamp_duty"],
            "registration_charges": base["registration_fee"],
            "document_status": base["status"],
        }

    if schema_family == "commercial_lease_registry":
        return {
            "office_label": base["office_name"],
            "district": base["district"],
            "registration_date": base["registration_date"],
            "token_no": base["document_number"],
            "doc_type": base["document_type"],
            "property_use": base["property_usage"],
            "neighborhood": base["locality"],
            "unit_identifier": f"{base['survey_number']}-U{base['document_number'].split('/')[-1][-3:]}",
            "party1": base["buyer_name"],
            "party2": base["seller_name"],
            "market_value": base["market_value"],
            "security_deposit": round(float(base["consideration_value"]) * 0.15, 2),
            "stamp_amt": base["stamp_duty"],
            "fee_paid": base["registration_fee"],
            "status": base["status"],
        }

    return {
        "office": base["office_name"],
        "dist": base["district"],
        "document_date": base["registration_date"],
        "registration_no": base["document_number"],
        "article_type": base["document_type"],
        "asset_type": base["property_type"],
        "zone": base["locality"],
        "claimant_name": base["buyer_name"],
        "executant_name": base["seller_name"],
        "plot_no": base["survey_number"],
        "valuation": base["market_value"],
        "sd_paid": base["stamp_duty"],
        "reg_fee": base["registration_fee"],
        "remarks": base["status"],
    }


def choose_schema_family(office: OfficeRecord, rng: random.Random) -> str:
    lowered = office.office_name.lower()
    if "mumbai" in lowered or "haveli" in lowered or "city" in lowered:
        return rng.choices(
            population=["standard_registration", "urban_project_sales", "commercial_lease_registry", "legacy_extract"],
            weights=[0.2, 0.45, 0.2, 0.15],
            k=1,
        )[0]
    if "rural" in lowered or office.division in {"Amravati", "Aurangabad"}:
        return rng.choices(
            population=["rural_land_records", "standard_registration", "legacy_extract"],
            weights=[0.5, 0.3, 0.2],
            k=1,
        )[0]
    return rng.choices(
        population=list(SCHEMA_FAMILIES.keys()),
        weights=[0.3, 0.25, 0.2, 0.1, 0.15],
        k=1,
    )[0]


def generate_workbook(
    manifest: OfficeManifest,
    output_path: Path,
    metadata_path: Path,
    *,
    total_rows: int,
    seed: int,
    max_offices: int | None = None,
    include_admin_offices: bool = False,
) -> SyntheticWorkbookMetadata:
    rng = random.Random(seed)
    offices = select_offices(manifest, max_offices=max_offices, include_admin_offices=include_admin_offices)
    row_counts = allocate_rows(offices, total_rows)
    metadata_rows: list[SyntheticSheetMetadata] = []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for office, row_count in zip(offices, row_counts):
            schema_family = choose_schema_family(office, rng)
            rows = [
                row_for_family(schema_family, generate_base_record(office, rng, row_index), office)
                for row_index in range(1, row_count + 1)
            ]
            dataframe = pd.DataFrame(rows)
            dataframe.to_excel(writer, sheet_name=office.sheet_name, index=False)
            metadata_rows.append(
                SyntheticSheetMetadata(
                    office_code=office.office_code,
                    office_name=office.office_name,
                    division=office.division,
                    district=office.district,
                    sheet_name=office.sheet_name,
                    schema_family=schema_family,
                    row_count=row_count,
                    canonical_map=SCHEMA_FAMILIES[schema_family]["canonical_map"],  # type: ignore[index]
                )
            )

    metadata = SyntheticWorkbookMetadata(
        generated_at=datetime.now(UTC),
        total_rows=sum(row_counts),
        total_offices=len(offices),
        workbook_path=str(output_path),
        manifest_path=str(metadata_path.parent.parent / "data" / "maharashtra_registration_offices.json"),
        sheets=metadata_rows,
    )
    metadata_path.write_text(metadata.model_dump_json(indent=2), encoding="utf-8")
    return metadata


def dataframe_to_xlsx_bytes(dataframe: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        dataframe.to_excel(writer, sheet_name="filtered_data", index=False)
    return buffer.getvalue()
