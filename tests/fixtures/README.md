# Test fixtures

Every fixture is a real ENTSO-E API response, saved byte for byte or trimmed. `.gitattributes` stops git from changing their bytes. Derived fixtures, made by editing a real response, are marked as derived below.

| File | Source | Changes |
|---|---|---|
| `ack_authentication_failed.xml` | `GET https://web-api.tp.entsoe.eu/api` with no parameters and no token, 2026-09-24 15:57 UTC. HTTP 401. | None. Reason code 999, text "Authentication failed." |

Some tests build a "no data" acknowledgement from `ack_authentication_failed.xml` by replacing its reason text. That stand-in goes once a real "No matching data found" response is saved here.
