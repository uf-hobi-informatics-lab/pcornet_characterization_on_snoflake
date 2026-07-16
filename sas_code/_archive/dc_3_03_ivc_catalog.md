# DC 3.03 IVC Field Catalog (from SAS newest, edc_report.sas lines 7605-8033)

**Missing definition (SAS "category")**: value NULL, blank, or UPPER(TRIM(value)) IN ('NI','UN','OT')
**Missing definition (SAS "NULL_N")**: value NULL or blank only
**Missing definition (SAS "NMISS")**: date/time value NULL

**Flag threshold**: missing_pct >= 10% AND field in flag list

## Flag fields (10)
BIRTH_DATE, SEX, DX_ORIGIN, DX_SOURCE, DIA_ENCOUNTERID, DISCHARGE_DATE,
DISCHARGE_DISPOSITION, PRO_ENCOUNTERID, PX_DATE, PX_SOURCE

## Encounter-type restricted (denom = ENC_TYPE IN ('IP','EI'))
DISCHARGE_DATE, DISCHARGE_DISPOSITION, DISCHARGE_STATUS, DRG, ADMITTING_SOURCE, PDX

## Fields (ord, table, col_name, missing_kind, denom_filter, is_flag)

| Ord | Table | Column | Missing_Kind | Denom_Filter | Is_Flag |
|-----|-------|--------|--------------|--------------|---------|
| 01 | DEMOGRAPHIC | BIRTH_DATE | NMISS | none | YES |
| 02 | DEMOGRAPHIC | BIRTH_TIME | NMISS | none | no |
| 03 | DEMOGRAPHIC | SEX | category | none | YES |
| 04 | DEMOGRAPHIC | HISPANIC | category | none | no |
| 05 | DEMOGRAPHIC | RACE | category | none | no |
| 06 | DEMOGRAPHIC | GENDER_IDENTITY | category | none | no |
| 07 | DEMOGRAPHIC | SEXUAL_ORIENTATION | category | none | no |
| 08 | DEMOGRAPHIC | PAT_PREF_LANGUAGE_SPOKEN | category | none | no |
| 09 | DEMOGRAPHIC | BIOBANK_FLAG | category | none | no |
| 10 | DEMOGRAPHIC | RACE_ETH_MISSING | category | none | no |
| 11 | DEMOGRAPHIC | RACE_ETH_AI_AN | category | none | no |
| 12 | DEMOGRAPHIC | RACE_ETH_ASIAN | category | none | no |
| 13 | DEMOGRAPHIC | RACE_ETH_BLACK | category | none | no |
| 14 | DEMOGRAPHIC | RACE_ETH_HISPANIC | category | none | no |
| 15 | DEMOGRAPHIC | RACE_ETH_ME_NA | category | none | no |
| 16 | DEMOGRAPHIC | RACE_ETH_NH_PI | category | none | no |
| 17 | DEMOGRAPHIC | RACE_ETH_WHITE | category | none | no |
| 18 | ENROLLMENT | ENR_END_DATE | NMISS | none | no |
| 19 | ENROLLMENT | CHART | category | none | no |
| 20 | ENCOUNTER | ADMIT_TIME | NMISS | none | no |
| 21 | ENCOUNTER | DISCHARGE_DATE | category | ENC_TYPE IN ('IP','EI') | YES |
| 22 | ENCOUNTER | DISCHARGE_TIME | NMISS | none | no |
| 23 | ENCOUNTER | ENC_TYPE | category | none | no |
| 24 | ENCOUNTER | PROVIDERID | NULL_N | none | no |
| 25 | ENCOUNTER | FACILITYID | NULL_N | none | no |
| 26 | ENCOUNTER | DISCHARGE_DISPOSITION | category | ENC_TYPE IN ('IP','EI') | YES |
| 27 | ENCOUNTER | DISCHARGE_STATUS | category | ENC_TYPE IN ('IP','EI') | no |
| 28 | ENCOUNTER | DRG | category | ENC_TYPE IN ('IP','EI') | no |
| 29 | ENCOUNTER | DRG_TYPE | category | none | no |
| 30 | ENCOUNTER | ADMITTING_SOURCE | category | ENC_TYPE IN ('IP','EI') | no |
| 31 | ENCOUNTER | PAYER_TYPE_PRIMARY | category | none | no |
| 32 | ENCOUNTER | PAYER_TYPE_SECONDARY | category | none | no |
| 33 | ENCOUNTER | FACILITY_TYPE | category | none | no |
| 34 | ENCOUNTER | FACILITY_LOCATION | category | none | no |
| 35 | DIAGNOSIS | DX_DATE | NMISS | none | no |
| 36 | DIAGNOSIS | DX_TYPE | category | none | no |
| 37 | DIAGNOSIS | DX_SOURCE | category | none | YES |
| 38 | DIAGNOSIS | DX_ORIGIN | category | none | YES |
| 39 | DIAGNOSIS | PDX | category | ENC_TYPE IN ('IP','EI') | no |
| 40 | DIAGNOSIS | ENCOUNTERID | NULL_N | none | YES (as DIA_ENCOUNTERID) |
| 41 | DIAGNOSIS | ADMIT_DATE | NMISS | none | no |
| 42 | DIAGNOSIS | ENC_TYPE | category | none | no |
| 43 | DIAGNOSIS | DX_POA | category | none | no |
| 44 | DIAGNOSIS | PROVIDERID | NULL_N | none | no |
| 45 | PROCEDURES | PX_DATE | NMISS | none | YES |
| 46 | PROCEDURES | PX_TYPE | category | none | no |
| 47 | PROCEDURES | PX_SOURCE | category | none | YES |
| 48 | PROCEDURES | ENCOUNTERID | NULL_N | none | YES (as PRO_ENCOUNTERID) |
| 49 | PROCEDURES | PPX | category | none | no |
| 50 | PROCEDURES | PROVIDERID | NULL_N | none | no |
| 51 | PROCEDURES | ADMIT_DATE | NMISS | none | no |
| 52 | PROCEDURES | ENC_TYPE | category | none | no |

**Total IVC**: 52 fields across 5 tables (DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES).
