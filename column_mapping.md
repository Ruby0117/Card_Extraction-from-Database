# Column Mapping Reference

This document lists every source column alias used in the pipeline and the decoded values where applicable.

> Note: Source column names follow the CRM system's internal naming convention. Aliases are used throughout the pipeline for readability.

## Identifiers & Metadata

| Alias | Notes |
|---|---|
| `cardidlookup` | Internal GUID |
| `cardid` | Human-readable card number |
| `structidlookup` | FK to structure lookup |
| `surveyyear` | Survey year (partition key for export) |
| `regmailbox` | Regional mailbox |
| `address` | Address |
| `owner` | CRM owner GUID |
| `status` | Active / Inactive |
| `specsort` | Special sort code |
| `dweltype` | Decoded: Apartment / Row / Single / Semi |
| `provinceid` | Recoded to Stats Canada numeric code |
| `modifiedon` | Last modified timestamp |
| `modifiedby` | Last modified by (display name) |
| `load_date_time` | ETL load timestamp |
| `rent_grid_is_valid` | Boolean → Yes/No |
| `auto_status` | Decoded: Not Started / In Progress / Partial Data / Complete Data |
| `manual_status` | Decoded: Refusal / Hard Refusal / etc. |
| `removalreasonspermanently` | Permanent removal reason |
| `removalreasonstemporarily` | Temporary removal reason |
| `enumerator` | Assigned enumerator |

---

## Q1 — Respondent Role

| Alias | Decoded Values |
|---|---|
| `q1_ownermanager` | I am the manager / administrator / owner / coordinator / No administrative role / Refuse |
| `q1_otherrole` | Free text |

## Q2 — New Contact Details

| Alias | Decoded Values |
|---|---|
| `q2_newcontactname` | |
| `q2_newcontactrole` | Property Manager / Administrator / Owner / Coordinator |
| `q2_newcontactaddress` | |
| `q2_newcontactphone` | |
| `q2_newcontactaemail` | |
| `q2_options` | Refuse / Do not know |

## Q3 — Survey Confirmation

| Alias | Decoded Values |
|---|---|
| `q3_sahconfirm` | Yes / No / Do not know / Refuse |

## Q4 — Project Name

| Alias | Decoded Values |
|---|---|
| `q4_projnameconfirmation` | Yes / No / Do not know / Refuse |
| `q4_projname` | Free text |

## Q5 — Owner Types (boolean flags)

All columns decode `true → Yes`, `false → No`.

| Alias |
|---|
| `q5_fedrownertyp` |
| `q5_provownertyp` |
| `q5_terrownertyp` |
| `q5_muniownertyp` |
| `q5_abinownertyp` |
| `q5_nonprofitownertyp` |
| `q5_coopownertyp` |
| `q5_privateownertyp` |
| `q5_otherownertyp` |
| `q5_other` |
| `q5_options` |

## Q6 — Manager Types (boolean flags)

| Alias |
|---|
| `q6_managsameasowner` |
| `q6_fedrmanagtyp` |
| `q6_provmanagtyp` |
| `q6_terrmanagtyp` |
| `q6_munimanagtyp` |
| `q6_abinmanagtyp` |
| `q6_nonprofitmanagtyp` |
| `q6_coopmanagtyp` |
| `q6_privatemanagtyp` |
| `q6_othermanagtyp` |
| `q6_other` |
| `q6_options` |

## Q7–Q8 — Government & Funding Agreement Types

Same boolean + options pattern as Q5/Q6.

## Q9 — Main Clientele (boolean flags)

Flags for: families with children, single women/men, seniors, youth, immigrants/refugees, persons with physical/mental disabilities, veterans, First Nations, Métis, Inuit, domestic violence victims, exiting homelessness.

## Q10 — Rent Inclusions (boolean flags)

Flags for: heat, hot water, electricity, parking, cable TV, internet/WiFi, appliances, furnished, other.

## Q11 — Year Built

| Alias | Decoded Values |
|---|---|
| `q11_buildyear` | Numeric year |
| `q11_buildyear_actual_estimate` | Actual / Estimate |
| `q11_options` | Do not know / Refuse |

## Q12 — Building Construction Materials (boolean flags)

Flags for: all wood frame, concrete block walls, brick walls, steel stud walls, structured steel frame, other.

## Q13 — Heating System (boolean flags + furnace type)

| Alias | Decoded Values |
|---|---|
| `q13_forcedairfurnaceoptions` | Gas / Electric / Oil |

Plus boolean flags for: heat pump, electric baseboard, hot water/steam, heating stove, other.

## Q14 — Accessibility Features (boolean flags)

Flags for: elevators, street-level entrance, wide doorways, wheelchair ramps, handrails, stair lift, appropriate door hardware, electronic door opener, keyless entry, accessible parking, scooter/wheelchair storage, paved walkways, other.

| Alias |
|---|
| `q14_elevatorsnumber` |

## Q15–Q17 — Building Condition & Repairs

| Alias | Decoded Values |
|---|---|
| `q15_buildinglastestimation` | Less than 3 years ago / Never / 3–5 years ago / 6–10 years ago / More than 10 years ago / Do not know / Refuse |
| `q16_buildingcondition` | Excellent / Good / Average / Fair / Poor / Do not know / Refuse |

Q17 boolean flags for repairs needed: foundation, superstructure, roofing, exterior enclosure, interior construction, plumbing, electrical, HVAC, units, elevators, fire protection, exterior site improvement, other.

## Q18–Q27 — Units, Occupancy, Rent & Vacancy

| Alias | Description |
|---|---|
| `q18_totutct` | Total unit count |
| `q19_totsbzutct` | Total subsidised units |
| `q20_bachunit` – `q20_bed4unit` | Units by bedroom type |
| `q21_bachoccupiednewhh` – `q21_4bedoccupiednewhh` | New household occupancy by bedroom |
| `q22_totoccupiednewhh` | Total new households |
| `q24_vunitbach` – `q24_vunit4bed` | Vacant units by bedroom |
| `q27_bachavgrent` – `q27_bed4avgrent` | Average rent by bedroom |

### Rent Mechanism (Q23)

| Alias |
|---|
| `q23_rentoperationalcots` |
| `q23_rentgearedtoincome` |
| `q23_rentsetagainstmarket` |
| `q23_rentfixedexternal` |

### Q25 — How Rent Info Entered

| Decoded Value |
|---|
| Respondent does not know the information |
| Respondent will email the information |
| Enter using an Excel spreadsheet (Import) |
| Enter directly in the detailed rent & vacancy grid |
| Respondent refuses to provide the information |

---

## Province ID Encoding (Statistics Canada)

After joining the province lookup, `provinceid` is re-encoded to the standard Statistics Canada numeric code:

| Province / Territory | Code |
|---|---|
| Newfoundland and Labrador | 10 |
| Prince Edward Island | 11 |
| Nova Scotia | 12 |
| New Brunswick | 13 |
| Quebec | 24 |
| Ontario | 35 |
| Manitoba | 46 |
| Saskatchewan | 47 |
| Alberta | 48 |
| British Columbia | 59 |
| Yukon | 60 |
| Northwest Territories | 61 |
| Nunavut | 62 |
