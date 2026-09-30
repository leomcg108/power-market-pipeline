"""Phase 00 spike: probe ENTSO-E request parameters and summarise the responses.

Answers spike questions 4 to 7: price resolutions, which domain parameter names return
data for load, generation and flows, what CH publishes, and which area codes work for
CH <-> DE-LU flows. Each probe requests one delivery day and prints one summary line.
The summary counts Points and reads series attributes. It does not build rows.

Run from the repo root: uv run python analysis/spike/probe_entsoe.py
The token comes from ENTSOE_API_TOKEN in .env and is never printed.
"""

from collections import Counter
from datetime import date, datetime

from dotenv import find_dotenv, load_dotenv
from lxml import etree

from power_pipeline import ingest
from power_pipeline.entsoe import client, datasets

ZONES = datasets.load_zones()
CH = ZONES["CH"].eic
DE_LU = ZONES["DE_LU"].eic
DE_COUNTRY = "10Y1001A1001A83F"  # Germany as a country, used in the API guide's A75 example

RECENT = date(2026, 9, 23)
HISTORY = date(2024, 5, 10)


def prices(eic: str) -> dict[str, str]:
    return {
        "documentType": "A44",
        "in_Domain": eic,
        "out_Domain": eic,
        "contract_MarketAgreement.type": "A01",
    }


def load(domain_param: str, eic: str) -> dict[str, str]:
    return {"documentType": "A65", "processType": "A16", domain_param: eic}


def generation(domain_param: str, eic: str) -> dict[str, str]:
    return {"documentType": "A75", "processType": "A16", domain_param: eic}


def flows(from_eic: str, to_eic: str) -> dict[str, str]:
    return {"documentType": "A11", "out_Domain": from_eic, "in_Domain": to_eic}


# (question, label, delivery day, params without the period)
PROBES = [
    # Q4: price resolution either side of the 1 October 2025 switch, and CH
    ("Q4", "prices DE-LU", date(2025, 9, 30), prices(DE_LU)),
    ("Q4", "prices DE-LU", date(2025, 10, 1), prices(DE_LU)),
    ("Q4", "prices CH", HISTORY, prices(CH)),
    ("Q4", "prices CH", date(2025, 10, 1), prices(CH)),
    # Q5 and Q6: actual load (A65, realised)
    ("Q5", "load DE-LU outBiddingZone_Domain", RECENT, load("outBiddingZone_Domain", DE_LU)),
    ("Q5", "load CH outBiddingZone_Domain", RECENT, load("outBiddingZone_Domain", CH)),
    ("Q5", "load CH outBiddingZone_Domain", HISTORY, load("outBiddingZone_Domain", CH)),
    ("Q5", "load DE-LU in_Domain (wrong name?)", RECENT, load("in_Domain", DE_LU)),
    # Q5 and Q6: actual generation per production type (A75, realised)
    ("Q5", "generation DE-LU in_Domain", RECENT, generation("in_Domain", DE_LU)),
    ("Q5", "generation DE country in_Domain", RECENT, generation("in_Domain", DE_COUNTRY)),
    ("Q5", "generation CH in_Domain", RECENT, generation("in_Domain", CH)),
    ("Q5", "generation CH in_Domain", HISTORY, generation("in_Domain", CH)),
    ("Q5", "generation DE-LU out_Domain (wrong name?)", RECENT, generation("out_Domain", DE_LU)),
    # Q7: physical flows (A11), one direction per request
    ("Q7", "flows CH -> DE-LU, zone codes", RECENT, flows(CH, DE_LU)),
    ("Q7", "flows DE-LU -> CH, zone codes", RECENT, flows(DE_LU, CH)),
    ("Q7", "flows CH -> DE-LU, zone codes", HISTORY, flows(CH, DE_LU)),
    ("Q7", "flows CH -> DE, country code", RECENT, flows(CH, DE_COUNTRY)),
    ("Q7", "flows DE -> CH, country code", RECENT, flows(DE_COUNTRY, CH)),
]

DOMAIN_TAGS = (
    "inBiddingZone_Domain.mRID",
    "outBiddingZone_Domain.mRID",
    "in_Domain.mRID",
    "out_Domain.mRID",
)
SEQUENCE_TAG = "classificationSequence_AttributeInstanceComponent.position"


def minutes_between(start: str, end: str) -> int:
    """Minutes between two ENTSO-E timestamps like 2026-09-22T22:00Z."""
    parse = datetime.fromisoformat
    return int(
        (parse(end.replace("Z", "+00:00")) - parse(start.replace("Z", "+00:00"))).total_seconds()
        // 60
    )


def summarise(content: bytes) -> str:
    root = etree.fromstring(content)
    ns = {"d": etree.QName(root).namespace}
    series = root.findall("d:TimeSeries", ns)
    resolutions, psr_types, domains, sequences = Counter(), Counter(), Counter(), Counter()
    points = expected = 0
    for ts in series:
        psr = ts.findtext("d:MktPSRType/d:psrType", namespaces=ns)
        if psr:
            psr_types[psr] += 1
        for tag in DOMAIN_TAGS:
            if ts.find(f"d:{tag}", ns) is not None:
                domains[tag.removesuffix(".mRID")] += 1
        sequences[ts.findtext(f"d:{SEQUENCE_TAG}", namespaces=ns) or "-"] += 1
        for period in ts.iterfind("d:Period", ns):
            resolution = period.findtext("d:resolution", namespaces=ns)
            resolutions[resolution] += 1
            step = int(resolution.removeprefix("PT").removesuffix("M"))
            start = period.findtext("d:timeInterval/d:start", namespaces=ns)
            end = period.findtext("d:timeInterval/d:end", namespaces=ns)
            expected += minutes_between(start, end) // step
            points += len(period.findall("d:Point", ns))
    parts = [
        f"{len(series)} series",
        "resolution " + ", ".join(f"{k} x{v}" for k, v in sorted(resolutions.items())),
        f"points {points} of {expected}",
    ]
    if set(sequences) != {"-"}:
        parts.append("sequences " + ", ".join(f"{k} x{v}" for k, v in sorted(sequences.items())))
    if domains:
        parts.append("domains " + ", ".join(f"{k} x{v}" for k, v in sorted(domains.items())))
    if psr_types:
        parts.append(f"{len(psr_types)} psrTypes: " + " ".join(sorted(psr_types)))
    return " | ".join(parts)


def main() -> None:
    load_dotenv(find_dotenv(usecwd=True))
    token = client.get_token()
    for question, label, day, base in PROBES:
        start, end = ingest.delivery_day_window_utc(day)
        params = {
            **base,
            "periodStart": datasets.format_period(start),
            "periodEnd": datasets.format_period(end),
        }
        try:
            result = client.fetch(params, token)
        except client.EntsoeApiError as exc:
            print(f"{question} | {label} | {day} | ERROR {exc}")
            continue
        if result.status == "no_data":
            ack = client.parse_acknowledgement(result.content)
            print(f"{question} | {label} | {day} | NO DATA ({ack.describe()[:90]})")
            continue
        print(f"{question} | {label} | {day} | {result.root_element} | {summarise(result.content)}")


if __name__ == "__main__":
    main()
