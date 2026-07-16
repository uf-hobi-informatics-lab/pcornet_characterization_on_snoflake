"""
Parse SAS edc_report.sas (newest infolder) to extract the DC 3.03 IVC and IVD
field catalogs used to check for "missing or unknown" values.

Output: sas_code/dc_3_03_catalog.json with keys:
  ivc: list of {table, column, sas_var, dc_name, missing_kind, denom_filter, is_flag}
  ivd: list of same

missing_kind:
  category  -> COUNT_IF(col IS NULL OR TRIM(col::STRING) IN ('','NI','UN','OT') via UPPER)
  null_n    -> COUNT_IF(col IS NULL OR TRIM(col::STRING) = '')
  nmiss     -> COUNT_IF(col IS NULL)  # SAS NMISS on numerics/dates

denom_filter:
  none         -> COUNT(*) of parent table
  ip_ei        -> COUNT_IF(UPPER(TRIM(ENC_TYPE)) IN ('IP','EI')) on parent

is_flag: whether this field triggers thresholdflag=1 when perc >= 10.

Usage:
  python3 sas_code/parse_dc_3_03_catalog.py
"""
import json
import os
import re
import sys

SAS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "extracted_newest", "infolder", "edc_report.sas",
)

# ---------------------------------------------------------------------------
# Section extractor
# ---------------------------------------------------------------------------
def read_sas():
    with open(SAS_FILE, "r", encoding="utf-8", errors="replace") as f:
        return f.read()

def extract_section(src, start_marker, end_marker):
    i = src.index(start_marker)
    j = src.index(end_marker, i)
    return src[i:j]

# ---------------------------------------------------------------------------
# Format parsers: extract "value $x_field" and "value $x_source" maps
# ---------------------------------------------------------------------------
KV_RE = re.compile(r'"([^"]+)"\s*=\s*"([^"]+)"')

def parse_format(src, format_name):
    """Extract 'value $format_name' block and return dict {left: right}."""
    m = re.search(r"value\s+\$" + format_name + r"\b", src, re.IGNORECASE)
    if not m:
        raise ValueError(f"format ${format_name} not found")
    tail = src[m.end():]
    end = tail.index(";")
    body = tail[:end]
    return dict(KV_RE.findall(body))

# ---------------------------------------------------------------------------
# IVC parsing
# ---------------------------------------------------------------------------
def parse_ivc(src):
    ivc_block = extract_section(
        src,
        "TABLE IVC MISSING OR UNKNOWN VALUES",
        "TABLE IVD MISSING OR UNKNOWN VALUES",
    )

    r_field = parse_format(ivc_block, "r_field")     # var -> ord
    r_source = parse_format(ivc_block, "r_source")   # ord -> dc_name

    # Reverse r_field so ord -> var
    ord_to_var = {}
    for var, ordv in r_field.items():
        ord_to_var[ordv] = var

    # dum_field table-partitioning by ordinal (lines 7738-7744 in newest)
    # if 1<=ordn<=17 then table="DEMOGRAPHIC";
    # else if 18<=ordn<=19 then table="ENROLLMENT";
    # else if 20<=ordn<=34 then table="ENCOUNTER";
    # else if 35<=ordn<=44 then table="DIAGNOSIS";
    # else if 45<=ordn<=52 then table="PROCEDURES";
    def ordn_to_table(o):
        n = int(o)
        if 1 <= n <= 17:  return "DEMOGRAPHIC"
        if 18 <= n <= 19: return "ENROLLMENT"
        if 20 <= n <= 34: return "ENCOUNTER"
        if 35 <= n <= 44: return "DIAGNOSIS"
        if 45 <= n <= 52: return "PROCEDURES"
        raise ValueError(f"IVC ord {o} out of range")

    # SAS var-name renames (lines 7814-7821):
    # DIA_L3_N/ENCOUNTERID -> DIA_ENCOUNTERID
    # PRO_L3_N/ENCOUNTERID -> PRO_ENCOUNTERID
    # DIA_L3_N/PROVIDERID  -> DIA_PROVIDERID
    # PRO_L3_N/PROVIDERID  -> PRO_PROVIDERID
    # DIA_L3_ENCTYPE/ENC_TYPE -> DIA_ENC_TYPE
    # PRO_L3_ENCTYPE/ENC_TYPE -> PRO_ENC_TYPE
    # DIAGNOSIS/ADMIT_DATE -> DIA_ADMIT_DATE
    # PROCEDURES/ADMIT_DATE -> PRO_ADMIT_DATE
    # These are SAS-side aliases; on the CDM side the actual column is still
    # ENCOUNTERID / PROVIDERID / ENC_TYPE / ADMIT_DATE. We need the real column
    # to query, but keep the SAS alias for traceability.
    SAS_ALIAS_TO_CDM = {
        "DIA_ENCOUNTERID": "ENCOUNTERID",
        "PRO_ENCOUNTERID": "ENCOUNTERID",
        "DIA_PROVIDERID":  "PROVIDERID",
        "PRO_PROVIDERID":  "PROVIDERID",
        "DIA_ENC_TYPE":    "ENC_TYPE",
        "PRO_ENC_TYPE":    "ENC_TYPE",
        "DIA_ADMIT_DATE":  "ADMIT_DATE",
        "PRO_ADMIT_DATE":  "ADMIT_DATE",
    }

    # SAS "missing_kind" is derived from which numerator clause matches the
    # dc_name for a field:
    #   category clauses (RECORD_N + category in NULL/NI/UN/OT):
    #     lines 7790-7802 in newest SAS
    #   NULL_N clauses:
    #     ENC_L3_N / DIA_L3_N / PRO_L3_N with NULL_N and specific variables
    #   NMISS clauses:
    #     XTBL_L3_DATES / XTBL_L3_TIMES
    # Note: r_source (line 7679) uses "DEM_L3_RACE" for ord 05, but the numerator
    # clause uses "DEM_L3_RACEDIST". Treat both as category-kind.
    CATEGORY_DCNAMES = {
        "DEM_L3_RACE",  # ord 05 uses this in r_source, aliased to DEM_L3_RACEDIST
        "DEM_L3_SEXDIST","DEM_L3_HISPDIST","DEM_L3_RACEDIST","DEM_L3_GENDERDIST",
        "DEM_L3_ORIENTDIST","DEM_L3_PATPREFLANG","DEM_L3_BIOBANK","DEM_L3_RACEETHMISS",
        "DEM_L3_RACEETHAIAN","DEM_L3_RACEETHASIAN","DEM_L3_RACEETHBLACK","DEM_L3_RACEETHHISPANIC",
        "DEM_L3_RACEETHMENA","DEM_L3_RACEETHNHPI","DEM_L3_RACEETHWHITE",
        "ENC_L3_DRG_TYPE","ENC_L3_ENCTYPE","ENR_L3_CHART",
        "ENC_L3_PAYERTYPE1","ENC_L3_PAYERTYPE2","ENC_L3_FACILITYTYPE","ENC_L3_FACILITYLOC",
        "DIA_L3_DXSOURCE","DIA_L3_ORIGIN","DIA_L3_DXPOA","DIA_L3_ENCTYPE",
        "PRO_L3_PXSOURCE","PRO_L3_PPX","PRO_L3_ENCTYPE",
        # cross_category clauses (IP/EI restricted):
        "ENC_L3_ENCTYPE_DDATE_YM","ENC_L3_ENCTYPE_DISDISP","ENC_L3_ENCTYPE_DISSTAT",
        "ENC_L3_ENCTYPE_DRG","ENC_L3_ENCTYPE_ADMSRC","DIA_L3_PDX_ENCTYPE",
        # single-category clauses:
        "DIA_L3_DXTYPE","PRO_L3_PXTYPE",
    }
    IP_EI_DCNAMES = {
        "ENC_L3_ENCTYPE_DDATE_YM","ENC_L3_ENCTYPE_DISDISP","ENC_L3_ENCTYPE_DISSTAT",
        "ENC_L3_ENCTYPE_DRG","ENC_L3_ENCTYPE_ADMSRC","DIA_L3_PDX_ENCTYPE",
    }
    NULL_N_DCNAMES = {"ENC_L3_N","DIA_L3_N","PRO_L3_N"}
    NMISS_DCNAMES  = {"XTBL_L3_DATES","XTBL_L3_TIMES"}

    # IVC flag list (line 7893-7894 in newest SAS):
    IVC_FLAG_VARS = {
        "BIRTH_DATE","SEX","DX_ORIGIN","DX_SOURCE","DIA_ENCOUNTERID",
        "DISCHARGE_DATE","DISCHARGE_DISPOSITION","PRO_ENCOUNTERID",
        "PX_DATE","PX_SOURCE",
    }

    rows = []
    for ordv in sorted(r_source.keys(), key=lambda x: float(x)):
        dc_name = r_source[ordv]
        sas_var = ord_to_var.get(ordv)
        if sas_var is None:
            raise ValueError(f"IVC ord {ordv} has dc_name but no var in $r_field")

        table = ordn_to_table(ordv)
        cdm_column = SAS_ALIAS_TO_CDM.get(sas_var, sas_var)

        if dc_name in NMISS_DCNAMES:
            missing_kind = "nmiss"
        elif dc_name in NULL_N_DCNAMES:
            missing_kind = "null_n"
        elif dc_name in CATEGORY_DCNAMES:
            missing_kind = "category"
        else:
            raise ValueError(f"IVC ord {ordv} dc_name {dc_name}: unknown missing_kind")

        denom_filter = "ip_ei" if dc_name in IP_EI_DCNAMES else "none"

        rows.append({
            "table": table,
            "column": cdm_column,
            "sas_var": sas_var,
            "dc_name": dc_name,
            "ord": ordv,
            "missing_kind": missing_kind,
            "denom_filter": denom_filter,
            "is_flag": sas_var in IVC_FLAG_VARS,
        })
    return rows

# ---------------------------------------------------------------------------
# IVD parsing
# ---------------------------------------------------------------------------
def parse_ivd(src):
    ivd_block = extract_section(
        src,
        "TABLE IVD MISSING OR UNKNOWN VALUES",
        "%mend print_ivd",
    )

    o_field  = parse_format(ivd_block, "o_field")
    o_source = parse_format(ivd_block, "o_source")

    ord_to_var = {ordv: var for var, ordv in o_field.items()}

    # dum_field partitioning (lines 8392-8408 in newest):
    def ordn_to_table(o):
        # SAS uses best8. so 88.1 -> 88.1 (float)
        n = float(o)
        if   0 <= n <= 2:    return "VITAL"
        elif 3 <= n <= 6:    return "DEATH"
        elif 7 <= n <= 27:   return "LAB_RESULT_CM"
        elif 28 <= n <= 46:  return "PRESCRIBING"
        elif 47 <= n <= 53:  return "DISPENSING"
        elif 54 <= n <= 60:  return "CONDITION"
        elif 77 <= n <= 80:  return "PROVIDER"
        elif 81 <= n <= 89.9: return "MED_ADMIN"
        elif 90 <= n <= 101: return "OBS_CLIN"
        elif 102 <= n <= 113: return "OBS_GEN"
        elif 114 <= n <= 122: return "LDS_ADDRESS_HISTORY"
        elif 123 <= n <= 139: return "IMMUNIZATION"
        elif 140 <= n <= 150: return "LAB_HISTORY"
        elif 151 <= n < 161:  return "EXTERNAL_MEDS"
        elif 161 <= n <= 162: return "PAT_RELATIONSHIP"
        elif 163 <= n <= 172: return "PRO_CM"
        elif 173 <= n <= 176: return "DEATH_CAUSE"
        else:
            raise ValueError(f"IVD ord {o} out of range")

    # SAS var renames (lines 8509-8524 in newest):
    #   LAB_L3_PX_PXTYPE cross_variable is used but LAB_L3_PX_PXTYPE isn't in the flag list.
    #   PRES_L3_N ENCOUNTERID -> RX_ENCOUNTERID
    #   VIT_L3_N ENCOUNTERID  -> VITAL_ENCOUNTERID (removed from renames in newest? check)
    #   COND_L3_N ENCOUNTERID -> COND_ENCOUNTERID
    #   MEDADM_L3_N ENCOUNTERID -> MEDADMIN_ENCOUNTERID
    #   OBSCLIN_L3_N ENCOUNTERID -> OBSCLIN_ENCOUNTERID
    #   OBSGEN_L3_N ENCOUNTERID -> OBSGEN_ENCOUNTERID
    #   IMMUNE_L3_N ENCOUNTERID -> IMMUNE_ENCOUNTERID
    #   PROCM_L3_N ENCOUNTERID -> PROCM_ENCOUNTERID
    #   MEDADM_L3_N PRESCRIBINGID -> MEDADMIN_PRESCRIBINGID
    #   LAB_L3_LOINC_RESULT_NUM LAB_LOINC -> RESULT_NUM (this maps the LOINC column
    #     to a variable ord that lives in LAB_RESULT_CM but with alias RESULT_NUM;
    #     the *actual* CDM column is LAB_LOINC evaluated as a numeric result count)
    #   LABHIST_L3_UNIT RESULT_UNIT -> LH_RESULT_UNIT (real column: RESULT_UNIT in LAB_HISTORY)
    #   LABHIST_L3_LOW  NORM_MODIFIER_LOW  -> LH_NORM_MODIFIER_LOW
    #   LABHIST_L3_HIGH NORM_MODIFIER_HIGH -> LH_NORM_MODIFIER_HIGH
    #   EXTMED_L3_RXCUI RXNORM_CUI -> EXTMED_RXNORM_CUI
    SAS_ALIAS_TO_CDM = {
        "RX_ENCOUNTERID":       "ENCOUNTERID",
        "VITAL_ENCOUNTERID":    "ENCOUNTERID",
        "COND_ENCOUNTERID":     "ENCOUNTERID",
        "MEDADMIN_ENCOUNTERID": "ENCOUNTERID",
        "OBSCLIN_ENCOUNTERID":  "ENCOUNTERID",
        "OBSGEN_ENCOUNTERID":   "ENCOUNTERID",
        "IMMUNE_ENCOUNTERID":   "ENCOUNTERID",
        "PROCM_ENCOUNTERID":    "ENCOUNTERID",
        "MEDADMIN_PRESCRIBINGID": "PRESCRIBINGID",
        "RESULT_NUM":           "RESULT_NUM",   # LAB_L3_LOINC_RESULT_NUM: NMISS of RESULT_NUM (rows where LAB_LOINC present but RESULT_NUM null). Real CDM column is RESULT_NUM in LAB_RESULT_CM.
        "LH_RESULT_UNIT":       "RESULT_UNIT",
        "LH_NORM_MODIFIER_LOW": "NORM_MODIFIER_LOW",
        "LH_NORM_MODIFIER_HIGH":"NORM_MODIFIER_HIGH",
        "EXTMED_RXNORM_CUI":    "RXNORM_CUI",
    }

    # Category dcnames (numerator clause 1, lines 8454-8475 in newest):
    CATEGORY_DCNAMES = {
        "VIT_L3_VITAL_SOURCE","DEATH_L3_IMPUTE","DEATH_L3_MATCH","DEATH_L3_SOURCE",
        "LAB_L3_NAME","LAB_L3_SOURCE","LAB_L3_PRIORITY","LAB_L3_LOC","LAB_L3_PX_TYPE",
        "LAB_L3_QUAL","LAB_L3_MOD","LAB_L3_UNIT","LAB_L3_LOW","LAB_L3_HIGH","LAB_L3_ABN",
        "LAB_L3_LSOURCE","LAB_L3_RSOURCE","LAB_L3_SNOMED",
        "PRES_L3_FREQ","PRES_L3_BASIS","PRES_L3_RXDOSEFORM",
        "PRES_L3_ROUTE","PRES_L3_SOURCE","PRES_L3_DISPASWRTN","PRES_L3_RXDOSEODRUNIT",
        "DISP_L3_DOSEUNIT","DISP_L3_ROUTE","DISP_L3_SOURCE",
        "COND_L3_STATUS","COND_L3_SOURCE","COND_L3_TYPE",
        "PROV_L3_SPECIALTY","PROV_L3_SEX",
        "MEDADM_L3_DOSEADMUNIT","MEDADM_L3_ROUTE","MEDADM_L3_SOURCE","MEDADM_L3_TYPE",
        "OBSCLIN_L3_TYPE","OBSCLIN_L3_QUAL","OBSCLIN_L3_MOD","OBSCLIN_L3_RUNIT","OBSCLIN_L3_ABN",
        "OBSGEN_L3_TYPE","OBSGEN_L3_QUAL","OBSGEN_L3_MOD","OBSGEN_L3_RUNIT","OBSGEN_L3_ABN",
        "OBSCLIN_L3_SOURCE","OBSGEN_L3_SOURCE","LDSADRS_L3_ADRSUSE","LDSADRS_L3_ADRSTYPE",
        "IMMUNE_L3_CODETYPE","IMMUNE_L3_STATUS","IMMUNE_L3_STATUSREASON","IMMUNE_L3_SOURCE",
        "IMMUNE_L3_DOSEUNIT","IMMUNE_L3_ROUTE","IMMUNE_L3_BODYSITE","IMMUNE_L3_MANUFACTURER",
        "LABHIST_L3_RACEDIST","LABHIST_L3_UNIT","LABHIST_L3_LOW","LABHIST_L3_HIGH",
        "EXTMED_L3_RXDOSEFORM","EXTMED_L3_RXDOSE_UNIT","EXTMED_L3_ROUTE","EXTMED_L3_SOURCE","EXTMED_L3_BASIS",
        "PROCM_L3_CAT","PROCM_L3_METHOD","PROCM_L3_MODE","PROCM_L3_SOURCE","PROCM_L3_TYPE",
        "DEATHC_L3_CODE","DEATHC_L3_CONF","DEATHC_L3_SOURCE","DEATHC_L3_TYPE",
    }
    # Category-only NULL-or-missing (numerator clause 2, lines 8476-8479):
    CATEGORY_NULL_ONLY_DCNAMES = {
        "PRES_L3_RXCUI","PRES_L3_SUPDIST2","LAB_L3_LOINC","PRES_L3_PRNFLAG",
        "MEDADM_L3_CODE_TYPE","OBSCLIN_L3_CODE_TYPE","OBSGEN_L3_CODE_TYPE",
        "EXTMED_L3_RXCUI","DISP_L3_SUPDIST2","PROV_L3_NPIFLAG",
        "LDSADRS_L3_ADRSCITY","LDSADRS_L3_ADRSSTATE","LDSADRS_L3_ADRSZIP5",
        "LDSADRS_L3_COUNTYFIPS","LDSADRS_L3_CURRENT","LDSADRS_L3_RUCAZIP","LDSADRS_L3_STATEFIPS",
        "PROCM_L3_CODE_TYPE","PROCM_L3_FULLNAME","PROCM_L3_NAME",
        "LDSADRS_L3_ADRSZIP9","LABHIST_L3_PDSTART_Y","LABHIST_L3_PDEND_Y",
        "IMMUNE_L3_LOTNUM","LABHIST_L3_SEXDIST",
        # EXTMED_L3_RDATE_Y is commented out in newest SAS
    }
    # cross_category NULL-or-missing (only LAB_L3_PX_PXTYPE, line 8481):
    CROSS_CATEGORY_DCNAMES = {"LAB_L3_PX_PXTYPE"}
    # NULL_N clauses (lines 8482-8492):
    NULL_N_DCNAMES = {
        "VIT_L3_N","LAB_L3_N","PRES_L3_N","COND_L3_N","PROCM_L3_N",
        "DISP_L3_N","PROV_L3_N","MEDADM_L3_N","OBSCLIN_L3_N","OBSGEN_L3_N",
        "IMMUNE_L3_N","LABHIST_L3_N","LDSADRS_L3_N",
    }
    # NMISS (dates & times) - lines 8493-8499:
    NMISS_DCNAMES = {"XTBL_L3_DATES","XTBL_L3_TIMES","LAB_L3_LOINC_RESULT_NUM"}
    # Special "NULL or missing" *statistic* (numeric distributions), line 8500-8502:
    NULL_OR_MISSING_STAT_DCNAMES = {
        "PRES_L3_RXQTY_DIST","PRES_L3_RXREFILL_DIST","PRES_L3_RXDOSEODR_DIST",
        "MEDADM_L3_DOSEADM","DISP_L3_DOSE_DIST","DISP_L3_DISPAMT_DIST",
        "IMMUNE_L3_DOSE_DIST","LABHIST_L3_MIN_WKS","LABHIST_L3_MAX_WKS",
    }

    # IVD flag list (line 8597-8598 in newest):
    IVD_FLAG_VARS = {
        "DEATH_SOURCE","ENCOUNTERID","LAB_RESULT_SOURCE","MEDADMIN_ENCOUNTERID",
        "MEDADMIN_CODE","MEDADMIN_SOURCE","RX_ENCOUNTERID","RX_ORDER_DATE","RX_SOURCE",
        "PRO_CODE","PRO_SOURCE","VITAL_ENCOUNTERID","VITAL_SOURCE",
    }

    rows = []
    for ordv in sorted(o_source.keys(), key=lambda x: float(x)):
        dc_name = o_source[ordv]
        sas_var = ord_to_var.get(ordv)
        if sas_var is None:
            # Some ordinals like "88.1" for MEDADMIN_START_TIME:
            # look them up in o_field
            for k, v in o_field.items():
                if v == ordv:
                    sas_var = k
                    break
        if sas_var is None:
            print(f"WARN: IVD ord {ordv} has dc_name {dc_name} but no matching var", file=sys.stderr)
            continue

        # Skip PROVIDERID (77) in PROVIDER - it's the denominator's own PK,
        # not a numerator field. SAS drops it via if n and d.
        # Actually PROV_L3_N NULL_N of PROVIDERID isn't in the numerator list
        # (only PROVIDER_NPI is), so PROVIDERID as ord 77 won't have a numerator
        # match. Let's flag such fields as missing_kind=None so we can skip them.

        table = ordn_to_table(ordv)
        cdm_column = SAS_ALIAS_TO_CDM.get(sas_var, sas_var)

        # Determine missing_kind from dc_name
        if dc_name in CATEGORY_DCNAMES:
            missing_kind = "category"       # NULL or missing + NI + UN + OT
        elif dc_name in CATEGORY_NULL_ONLY_DCNAMES:
            missing_kind = "null_or_missing"  # only "NULL or missing" category, not NI/UN/OT
        elif dc_name in CROSS_CATEGORY_DCNAMES:
            missing_kind = "cross_null_or_missing"
        elif dc_name in NULL_N_DCNAMES:
            missing_kind = "null_n"
        elif dc_name in NMISS_DCNAMES:
            missing_kind = "nmiss"
        elif dc_name in NULL_OR_MISSING_STAT_DCNAMES:
            missing_kind = "stat_null_or_missing"  # numeric distributions
        else:
            # dc_name has no numerator clause (e.g. PROV_L3_N used only as denom
            # and only for PROVIDER_NPI, not PROVIDERID). Skip.
            print(f"SKIP: IVD ord {ordv} var {sas_var} dc_name {dc_name}: no numerator clause", file=sys.stderr)
            continue

        # Denominator filter for IVD: none of the IVD fields have IP/EI restrictions
        # (that's IVC-only). Denominators in IVD are always all rows of the parent table.
        # BUT: some IVD dc_names carry their own filter (e.g., LAB_L3_PX_PXTYPE is a
        # cross-tab on LAB_PX_TYPE x LAB_PX and only reports missing LAB_PX given LAB_PX_TYPE
        # is present). For now, treat all IVD denoms as "none" (whole-table count).
        denom_filter = "none"

        rows.append({
            "table": table,
            "column": cdm_column,
            "sas_var": sas_var,
            "dc_name": dc_name,
            "ord": ordv,
            "missing_kind": missing_kind,
            "denom_filter": denom_filter,
            "is_flag": sas_var in IVD_FLAG_VARS,
        })
    return rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    src = read_sas()
    ivc = parse_ivc(src)
    ivd = parse_ivd(src)

    out = {"ivc": ivc, "ivd": ivd}
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dc_3_03_catalog.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

    # Print summary
    print(f"IVC: {len(ivc)} fields")
    print(f"  flag fields: {sorted(r['sas_var'] for r in ivc if r['is_flag'])}")
    ivc_tables = {}
    for r in ivc:
        ivc_tables.setdefault(r['table'], 0)
        ivc_tables[r['table']] += 1
    print(f"  by table: {ivc_tables}")

    print(f"\nIVD: {len(ivd)} fields")
    print(f"  flag fields: {sorted(r['sas_var'] for r in ivd if r['is_flag'])}")
    ivd_tables = {}
    for r in ivd:
        ivd_tables.setdefault(r['table'], 0)
        ivd_tables[r['table']] += 1
    print(f"  by table: {ivd_tables}")

    print(f"\nWrote {out_path}")

if __name__ == "__main__":
    main()
