# Test fixtures

Every fixture is a real ENTSO-E API response body. All but one are saved byte for byte. `generation_de_lu_2026-09-23_trimmed.xml` is trimmed: whole TimeSeries blocks were cut out of the original text, so every byte that remains is as received. `.gitattributes` stops git from changing their bytes. None of them contains the API token.

Times are UTC. All requests cover delivery days in Europe/Zurich time.

## Prices and acknowledgements

Fetched on 2026-09-24. Price requests used `documentType=A44`, `contract_MarketAgreement.type=A01`, and `in_Domain` equal to `out_Domain`.

| File | Request | Response |
|---|---|---|
| `prices_ch_2026-09-23.xml` | CH (`10YCH-SWISSGRIDZ`), 202609222200 to 202609232200 | HTTP 200. One TimeSeries, `PT60M`, 24 of 24 points. No classification sequence. |
| `prices_de_lu_2026-09-23.xml` | DE-LU (`10Y1001A1001A82H`), 202609222200 to 202609232200 | HTTP 200. Two TimeSeries, both `PT15M`. mRID 1 is sequence 2 (EXAA), 94 of 96 points, positions 9 and 14 omitted. mRID 2 is sequence 1 (SDAC), 96 of 96 points. |
| `prices_de_lu_2024-05-10.xml` | DE-LU, 202405092200 to 202405102200 | HTTP 200. Two TimeSeries. mRID 1 is sequence 2 (EXAA), `PT15M`, 96 of 96 points. mRID 2 is sequence 1 (SDAC), `PT60M`, 23 of 24 points, position 15 omitted. |
| `ack_no_data.xml` | DE-LU, 202912312300 to 203001012300 (a future day) | HTTP 200. Acknowledgement, reason 999, "No matching data found for Data item ENERGY_PRICES [12.1.D] ...". |
| `ack_authentication_failed.xml` | No parameters and no token | HTTP 401. Acknowledgement, reason 999, "Authentication failed." |

Sequence 1 is the SDAC day-ahead auction and sequence 2 is the separate EXAA auction. See `docs/decisions.md`.

## Load, generation and flows

Fetched on 2026-10-20, for 202609222200 to 202609232200. Values are in `MAW` (megawatts).

| File | Request | Response |
|---|---|---|
| `load_de_lu_2026-09-23.xml` | `A65`, `processType=A16`, `outBiddingZone_Domain` DE-LU | HTTP 200, `GL_MarketDocument`. One TimeSeries, `PT15M`, 96 of 96 points. |
| `generation_ch_2026-09-23.xml` | `A75`, `processType=A16`, `in_Domain` CH | HTTP 200, `GL_MarketDocument`. Six TimeSeries, `PT60M`: B10, B11, B12, B14, B16, B19, all tagged `inBiddingZone_Domain`. Solar (B16) has 15 of 24 points. The night-time zeros are omitted. |
| `generation_de_lu_2026-09-23_trimmed.xml` | `A75`, `processType=A16`, `in_Domain` DE-LU | HTTP 200, `GL_MarketDocument`, 199,952 bytes and 17 TimeSeries before trimming, 54,332 bytes after. Kept: B10 tagged `outBiddingZone_Domain` (pumping consumption), B10 tagged `inBiddingZone_Domain` (generation), B09 (one Point for the whole day), B16 (95 of 96 points), and B17 (two Periods, 22:00 to 07:45 and 08:00 to 22:00, so 07:45 to 08:00 is missing). Removed: B01, B02, B03, B04, B05, B06, B11, B12, B15, B18, B19, B20. |
| `flows_ch_de_lu_2026-09-23.xml` | `A11`, `out_Domain` CH, `in_Domain` DE-LU | HTTP 200, `Publication_MarketDocument`. One TimeSeries, `PT15M`, 92 of 96 points, with zero values. |

## Derived cases

No real response had position 1 missing, positions missing at the end of a period, or a position past the end of its period. `tests/test_parsers.py` builds these three cases in code from `prices_ch_2026-09-23.xml`, and labels them as derived:

- positions 23 and 24 removed (must be filled from position 22)
- position 1 removed (must raise `ParseError`)
- position 24 renumbered to 25 (must raise `ParseError`)
