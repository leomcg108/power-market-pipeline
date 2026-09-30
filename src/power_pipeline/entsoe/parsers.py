"""Parse ENTSO-E XML documents into rows.

The tests in tests/test_parsers.py define what `parse_timeseries` must do.
"""

import re
from datetime import UTC, datetime, timedelta

import pandas as pd
from lxml import etree

ACKNOWLEDGEMENT_ROOT = "Acknowledgement_MarketDocument"
SEQUENCE_TAG = "classificationSequence_AttributeInstanceComponent.position"
VALUE_TAGS = ("price.amount", "quantity")

# No entity expansion and no network access while parsing.
_XML_PARSER = etree.XMLParser(resolve_entities=False, no_network=True)
_MINUTE_RESOLUTION = re.compile(r"^PT(\d+)M$")

PARSED_COLUMNS = [
    "time_series_mrid",
    "classification_sequence",
    "resolution",
    "interval_start_utc",
    "interval_end_utc",
    "value",
    "document_mrid",
    "revision_number",
    "created_datetime_utc",
]


class ParseError(ValueError):
    """The document breaks a rule the parser relies on, so it returns no rows."""


def parse_timeseries(xml: bytes) -> pd.DataFrame:
    """Turn one ENTSO-E XML document into one row per interval.

    Input:
        The response body exactly as received, not gzipped. Price documents have the
        root element Publication_MarketDocument. Read the XML namespace from the root
        element. It differs between document types and versions, so never hard-code it.

    Document structure for prices (simplified):
        Publication_MarketDocument
            mRID                       -> document_mrid
            revisionNumber             -> revision_number
            createdDateTime            -> created_datetime_utc, for example 2026-09-24T16:38:57Z
            TimeSeries (one or more)
                mRID                   -> time_series_mrid. "1", "2" and so on. Unique only
                                          within one document.
                classificationSequence_AttributeInstanceComponent.position
                                       -> classification_sequence. 1 is SDAC, 2 is EXAA.
                                          The element is missing for CH.
                curveType              A03 means Points can be left out (see the fill rule)
                Period (one or more)
                    timeInterval/start, timeInterval/end
                                       UTC, for example 2026-09-22T22:00Z
                    resolution         PT15M or PT60M
                    Point (one or more)
                        position       1-based index of the interval in the period
                        price.amount   -> value. Quantity documents (Phase 01) use
                                          `quantity` instead.

    The fill rule (data rule 5):
        A period has (end - start) / resolution intervals. That is 96 for a day at PT15M,
        and 23 at PT60M on the spring clock-change day. ENTSO-E leaves out a Point when
        its value equals the one before. Output every position from 1 to the number of
        intervals. A missing position takes the value of the nearest earlier position.
        That includes missing positions at the end of a period.

    Output:
        A DataFrame with exactly PARSED_COLUMNS, in that order, and one row per interval
        per time series.
        - interval_start_utc = period start + (position - 1) * resolution
        - interval_end_utc = interval_start_utc + resolution
        - All three timestamp columns are timezone-aware UTC.
        - value is float64. Negative prices are valid.
        - revision_number has an integer dtype.
        - classification_sequence has the nullable integer dtype "Int64", <NA> if absent.
        - time_series_mrid, resolution and document_mrid hold strings.
        An Acknowledgement_MarketDocument, which is ENTSO-E's "no data" reply, gives an
        empty DataFrame with the same columns.

    Raises:
        ParseError if a period has no Point at position 1, since there is no earlier
        value to carry forward, or if a position is below 1 or above the number of
        intervals in its period. Also for a repeated position, invalid XML, or a
        missing required element.
    """
    try:
        root = etree.fromstring(xml, parser=_XML_PARSER)
    except etree.XMLSyntaxError as exc:
        raise ParseError(f"Not valid XML: {exc}") from exc

    root_name = etree.QName(root)
    if root_name.localname == ACKNOWLEDGEMENT_ROOT:
        return _to_frame([])

    doc = _Reader(root_name.namespace)
    document_mrid = doc.text(root, "mRID")
    revision_number = int(doc.text(root, "revisionNumber"))
    created_datetime_utc = _utc_time(doc.text(root, "createdDateTime"))

    rows = []
    for series in doc.all(root, "TimeSeries"):
        time_series_mrid = doc.text(series, "mRID")
        sequence = doc.text(series, SEQUENCE_TAG, required=False)
        classification_sequence = int(sequence) if sequence is not None else None

        for period in doc.all(series, "Period"):
            period_start = _utc_time(doc.text(period, "timeInterval/start"))
            period_end = _utc_time(doc.text(period, "timeInterval/end"))
            resolution = doc.text(period, "resolution")
            step = _step(resolution)
            interval_count = _interval_count(period_start, period_end, step)

            points = _read_points(doc, period, interval_count, time_series_mrid)
            values = _fill_omitted_positions(points, interval_count, time_series_mrid)

            for index, value in enumerate(values):
                interval_start = period_start + index * step
                rows.append(
                    (
                        time_series_mrid,
                        classification_sequence,
                        resolution,
                        interval_start,
                        interval_start + step,
                        value,
                        document_mrid,
                        revision_number,
                        created_datetime_utc,
                    )
                )
    return _to_frame(rows)


class _Reader:
    """Element lookups in the document's own namespace, read from its root element."""

    def __init__(self, namespace: str | None):
        self.prefix = f"{{{namespace}}}" if namespace else ""

    def all(self, element, path: str) -> list:
        return element.findall("/".join(self.prefix + tag for tag in path.split("/")))

    def text(self, element, path: str, required: bool = True) -> str | None:
        found = self.all(element, path)
        if found and found[0].text is not None:
            return found[0].text.strip()
        if required:
            raise ParseError(f"Missing <{path}> in <{etree.QName(element).localname}>.")
        return None


def _utc_time(value: str) -> datetime:
    """Read an ENTSO-E timestamp such as 2026-09-22T22:00Z. It must be marked as UTC."""
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ParseError(f"Not a timestamp: {value!r}") from exc
    if moment.utcoffset() != timedelta(0):
        raise ParseError(f"Timestamp is not in UTC: {value!r}")
    return moment.astimezone(UTC)


def _step(resolution: str) -> timedelta:
    """Length of one interval, from an ISO 8601 duration such as PT15M or PT60M."""
    match = _MINUTE_RESOLUTION.match(resolution)
    if not match or int(match.group(1)) == 0:
        raise ParseError(f"Unsupported resolution {resolution!r}.")
    return timedelta(minutes=int(match.group(1)))


def _interval_count(start: datetime, end: datetime, step: timedelta) -> int:
    """Number of intervals in a period. The period must hold a whole number of them."""
    if end <= start or (end - start) % step:
        raise ParseError(f"Period {start} to {end} is not a whole number of {step} intervals.")
    return (end - start) // step


def _read_points(
    doc: _Reader, period, interval_count: int, time_series_mrid: str
) -> dict[int, float]:
    """Map position to value for the Points present in one period."""
    points = {}
    for point in doc.all(period, "Point"):
        position = int(doc.text(point, "position"))
        if not 1 <= position <= interval_count:
            raise ParseError(
                f"Position {position} is outside 1 to {interval_count} "
                f"in series {time_series_mrid}."
            )
        if position in points:
            raise ParseError(f"Position {position} appears twice in series {time_series_mrid}.")
        points[position] = _point_value(doc, point)
    return points


def _point_value(doc: _Reader, point) -> float:
    for tag in VALUE_TAGS:
        value = doc.text(point, tag, required=False)
        if value is not None:
            return float(value)
    raise ParseError(f"Point has none of {VALUE_TAGS}.")


def _fill_omitted_positions(
    points: dict[int, float], interval_count: int, time_series_mrid: str
) -> list[float]:
    """Values for positions 1 to interval_count. A missing position repeats the one before.

    ENTSO-E leaves out a Point when its value equals the previous one (data rule 5).
    """
    if 1 not in points:
        raise ParseError(f"Position 1 is missing in series {time_series_mrid}.")
    values = [points[1]]
    for position in range(2, interval_count + 1):
        values.append(points.get(position, values[-1]))
    return values


def _to_frame(rows: list[tuple]) -> pd.DataFrame:
    """Build the output frame with the promised dtypes, including when there are no rows."""
    columns = {name: [row[i] for row in rows] for i, name in enumerate(PARSED_COLUMNS)}
    return pd.DataFrame(
        {
            "time_series_mrid": pd.Series(columns["time_series_mrid"], dtype=str),
            "classification_sequence": pd.array(columns["classification_sequence"], dtype="Int64"),
            "resolution": pd.Series(columns["resolution"], dtype=str),
            "interval_start_utc": pd.to_datetime(columns["interval_start_utc"], utc=True),
            "interval_end_utc": pd.to_datetime(columns["interval_end_utc"], utc=True),
            "value": pd.Series(columns["value"], dtype="float64"),
            "document_mrid": pd.Series(columns["document_mrid"], dtype=str),
            "revision_number": pd.Series(columns["revision_number"], dtype="int64"),
            "created_datetime_utc": pd.to_datetime(columns["created_datetime_utc"], utc=True),
        }
    )
