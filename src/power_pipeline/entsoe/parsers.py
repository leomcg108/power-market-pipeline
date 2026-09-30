"""Parse ENTSO-E XML documents into rows.

Leo writes the implementation of `parse_timeseries` (Phase 00, task 5).
The tests in tests/test_parsers.py define what it must do.
"""

import pandas as pd

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
        intervals in its period.
    """
    raise NotImplementedError("Leo writes this (Phase 00, task 5).")
