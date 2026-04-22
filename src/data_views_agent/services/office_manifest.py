from __future__ import annotations

import io
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pdfplumber
import requests

from data_views_agent.models.contracts import OfficeManifest, OfficeRecord
from data_views_agent.utils.text import build_excel_sheet_name, compact_whitespace


OFFICE_PDF_URLS: dict[str, str] = {
    "amravati": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/Amravati.pdf",
    "aurangabad": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/Aurangabad.pdf",
    "mumbai": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/Mumbai.pdf",
    "nagpur": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/Nagpur.pdf",
    "nashik": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/Nashik.pdf",
    "pune": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/PUne.pdf",
    "thane": "https://igrmaharashtra.gov.in/pdf/documents/OfficesAddress/thane.pdf",
}

DIVISION_DISPLAY_NAMES = {
    "amravati": "Amravati",
    "aurangabad": "Aurangabad",
    "mumbai": "Mumbai",
    "nagpur": "Nagpur",
    "nashik": "Nashik",
    "pune": "Pune",
    "thane": "Thane",
}


def infer_district(office_name: str, address: str, division: str) -> str:
    district_match = re.search(r"Dist\.?\s*([A-Za-z .-]+)", address, flags=re.IGNORECASE)
    if district_match:
        return compact_whitespace(district_match.group(1)).rstrip(".")

    office_match = re.search(
        r"(Mumbai City|Mumbai Suburban|Aurangabad|Nagpur|Nashik|Pune|Thane|Raigad|Beed|Jalna|Parbhani|"
        r"Latur|Nanded|Osmanabad|Hingoli|Amravati|Yavatmal|Akola|Buldhana|Washim|Wardha|Bhandara|"
        r"Gondia|Chandrapur|Gadchiroli|Ahmednagar|Dhule|Nandurbar|Jalgaon|Kolhapur|Sangli|Satara|"
        r"Solapur|Ratnagiri|Sindhudurg|Palghar)",
        f"{office_name} {address}",
        flags=re.IGNORECASE,
    )
    if office_match:
        return compact_whitespace(office_match.group(1)).title()

    if division == "mumbai":
        if "suburban" in office_name.lower():
            return "Mumbai Suburban"
        return "Mumbai City"

    return DIVISION_DISPLAY_NAMES[division]


def extract_office_records_from_pdf(division_key: str, pdf_url: str) -> list[OfficeRecord]:
    response = requests.get(pdf_url, timeout=90)
    response.raise_for_status()

    offices: list[OfficeRecord] = []
    seen_serials: set[int] = set()
    division_name = DIVISION_DISPLAY_NAMES[division_key]

    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                for row in table:
                    if not row or not row[0]:
                        continue

                    serial_text = compact_whitespace(row[0])
                    if not serial_text.isdigit():
                        continue

                    serial_number = int(serial_text)
                    normalized = [compact_whitespace(cell) for cell in row]
                    office_name = normalized[1] if len(normalized) > 1 else f"{division_name} Office {serial_number}"
                    address = normalized[2] if len(normalized) > 2 else ""

                    if (not office_name or office_name.isdigit()) and address.isdigit():
                        continue
                    if office_name.lower() in {"office", "name of the office"}:
                        continue
                    if serial_number in seen_serials:
                        continue
                    seen_serials.add(serial_number)

                    remaining = normalized[3:]
                    email = next((cell for cell in remaining if "@" in cell), "")
                    office_time = next((cell for cell in remaining if cell and cell != email), "")
                    district = infer_district(office_name, address, division_key)
                    office_code = f"{division_key}-{serial_number:03d}"
                    sheet_name = build_excel_sheet_name(division_key, serial_number, office_name)

                    offices.append(
                        OfficeRecord(
                            office_code=office_code,
                            sheet_name=sheet_name,
                            office_name=office_name,
                            division=division_name,
                            district=district,
                            address=address,
                            email=email,
                            office_time=office_time,
                            source_url=pdf_url,
                            serial_number=serial_number,
                        )
                    )

    return offices


def build_office_manifest() -> OfficeManifest:
    offices: list[OfficeRecord] = []
    for division_key, url in OFFICE_PDF_URLS.items():
        offices.extend(extract_office_records_from_pdf(division_key, url))

    offices.sort(key=lambda office: (office.division, office.serial_number, office.office_code))

    return OfficeManifest(
        generated_at=datetime.now(UTC),
        retrieved_on=datetime.now(UTC).date().isoformat(),
        office_count=len(offices),
        source_urls=OFFICE_PDF_URLS,
        offices=offices,
    )


def write_office_manifest(path: Path) -> OfficeManifest:
    manifest = build_office_manifest()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.model_dump(mode="json"), indent=2), encoding="utf-8")
    return manifest


def load_office_manifest(path: Path) -> OfficeManifest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return OfficeManifest.model_validate(payload)


def ensure_office_manifest(path: Path, *, refresh: bool = False) -> OfficeManifest:
    if refresh or not path.exists():
        return write_office_manifest(path)
    return load_office_manifest(path)
