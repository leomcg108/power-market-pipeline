# Test fixtures

Every fixture is a real ENTSO-E API response body, saved byte for byte with no trimming. `.gitattributes` stops git from changing their bytes. None of them contains the API token.

All price requests used `documentType=A44`, `contract_MarketAgreement.type=A01`, and `in_Domain` equal to `out_Domain`. Times are UTC. All were fetched on 2026-09-24.

| File | Request | Response |
|---|---|---|
| `prices_ch_2026-09-23.xml` | CH (`10YCH-SWISSGRIDZ`), 202609222200 to 202609232200 | HTTP 200. One TimeSeries, `PT60M`, 24 of 24 points. No classification sequence. |
| `prices_de_lu_2026-09-23.xml` | DE-LU (`10Y1001A1001A82H`), 202609222200 to 202609232200 | HTTP 200. Two TimeSeries, both `PT15M`. mRID 1 is sequence 2 (EXAA), 94 of 96 points, positions 9 and 14 omitted. mRID 2 is sequence 1 (SDAC), 96 of 96 points. |
| `prices_de_lu_2024-05-10.xml` | DE-LU, 202405092200 to 202405102200 | HTTP 200. Two TimeSeries. mRID 1 is sequence 2 (EXAA), `PT15M`, 96 of 96 points. mRID 2 is sequence 1 (SDAC), `PT60M`, 23 of 24 points, position 15 omitted. |
| `ack_no_data.xml` | DE-LU, 202912312300 to 203001012300 (a future day) | HTTP 200. Acknowledgement, reason 999, "No matching data found for Data item ENERGY_PRICES [12.1.D] ...". |
| `ack_authentication_failed.xml` | No parameters and no token | HTTP 401. Acknowledgement, reason 999, "Authentication failed." |

Sequence 1 is the SDAC day-ahead auction and sequence 2 is the separate EXAA auction. See `docs/decisions.md`.

No real response had position 1 missing or positions missing at the end of a period. Parser tests build those two cases in code by deleting Points from a real fixture, and label them as derived.
