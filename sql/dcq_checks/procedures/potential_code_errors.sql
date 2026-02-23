CREATE OR REPLACE PROCEDURE "POTENTIAL_CODE_ERRORS"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_LIST" VARCHAR DEFAULT 'ALL')
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    // ================================================================================
    // Potential Code Errors - Snowflake JavaScript Function
    // PCORnet Data Curation Query Package v7.01
    // Validates medical codes against data quality heuristics
    // 
    // Parameters:
    //   DB_PARAM: Database name containing PCORnet CDM tables
    //   SCHEMA_NAME: Schema name containing source data tables
    //   table_list: Comma-separated list of tables to validate, or ''ALL'' for all tables
    //              Options: DIAGNOSIS, PROCEDURES, PRESCRIBING, DISPENSING, MED_ADMIN, LAB_RESULT_CM, CONDITION, IMMUNIZATION, ALL
    // ================================================================================

    // Parse table list parameter
    const tables_to_check = TABLE_LIST.toUpperCase() === ''ALL'' ?
    [''DIAGNOSIS'', ''PROCEDURES'', ''PRESCRIBING'', ''DISPENSING'', ''MED_ADMIN'', ''LAB_RESULT_CM'', ''CONDITION'', ''IMMUNIZATION''] :
    TABLE_LIST.toUpperCase().split('','').map(t => t.trim());
    // ================================================================================
    // Potential Code Errors - Snowflake JavaScript Function
    // PCORnet Data Curation Query Package v7.01
    // Validates medical codes against data quality heuristics
    // 
    // Parameters:
    //   DB_PARAM: Database name containing PCORnet CDM tables
    //   SCHEMA_NAME: Schema name containing source data tables
    // ================================================================================



    // Create/replace characterization schema for intermediate tables in target database
    snowflake.execute({sqlText: `CREATE SCHEMA IF NOT EXISTS ${DB_PARAM}.CHARACTERIZATION`});   

    // Drop existing intermediate and output tables for this run only
    // (Do not delete tables unrelated to TABLE_LIST.)
    try {
        const dropTableIfExists = (qualifiedName) => {
            snowflake.execute({ sqlText: `DROP TABLE IF EXISTS ${qualifiedName}` });
        };

        if (tables_to_check.includes(''DIAGNOSIS'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.DIAGNOSIS_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_DX`);
        }

        if (tables_to_check.includes(''PROCEDURES'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.PROCEDURES_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_PX`);
        }

        if (tables_to_check.includes(''PRESCRIBING'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.PRESCRIBING_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_PRES`);
        }

        if (tables_to_check.includes(''DISPENSING'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.DISPENSING_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_DISP`);
        }

        if (tables_to_check.includes(''MED_ADMIN'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.MED_ADMIN_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_MEDADMIN`);
        }

        if (tables_to_check.includes(''LAB_RESULT_CM'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.LAB_RESULT_CM_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_LAB`);
        }

        if (tables_to_check.includes(''CONDITION'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.CONDITION_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_CONDITION`);
        }

        if (tables_to_check.includes(''IMMUNIZATION'')) {
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.IMMUNIZATION_VALIDATION`);
            dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.BAD_IMMUNIZATION`);
        }

        // Summary is always regenerated for the selected tables.
        dropTableIfExists(`${DB_PARAM}.CHARACTERIZATION.CODE_SUMMARY`);
    } catch (err) {
        // Ignore errors when tables don''t exist
    }

    // ================================================================================
    // 1. DIAGNOSIS Table Validation (ICD9 and ICD10 Diagnosis Codes)
    // ================================================================================

    // Create diagnosis validation table if requested
    if (tables_to_check.includes(''DIAGNOSIS'')) {
        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.DIAGNOSIS_VALIDATION AS
        SELECT 
            ''DIAGNOSIS'' AS table_name,
            diagnosisid,
            dx AS code,
            dx_type AS code_type,
            UPPER(REGEXP_REPLACE(dx, ''[.,\\\\\\\\s]'', '''')) AS code_clean,
            LENGTH(UPPER(REGEXP_REPLACE(dx, ''[.,\\\\\\\\s]'', ''''))) AS code_length,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(dx, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                ELSE 0 
            END AS has_alpha,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(dx, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                ELSE 0 
            END AS has_digit,
            REGEXP_INSTR(UPPER(REGEXP_REPLACE(dx, ''[.,\\\\s]'', '''')), ''[A-Z]'') AS alpha_pos,
            REGEXP_INSTR(UPPER(REGEXP_REPLACE(dx, ''[.,\\\\s]'', '''')), ''[0-9]'') AS digit_pos,
            CURRENT_TIMESTAMP() AS processed_date
        FROM ${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS 
        WHERE dx IS NOT NULL 
          AND dx_type IN (''09'', ''10'')
            `
        });

        // Create bad diagnosis records with validation rules
        snowflake.execute({
             sqlText: `
             CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_DX AS
        SELECT *
        FROM (
            SELECT 
                table_name,
                diagnosisid,
                code,
                code_type,
                code_clean,
                code_length,
                CASE 
                    WHEN code_type = ''09'' THEN CASE WHEN code_length NOT IN (3,4,5) THEN 1 ELSE 0 END
                    WHEN code_type = ''10'' THEN CASE WHEN code_length NOT IN (3,4,5,6,7) THEN 1 ELSE 0 END
                    ELSE 0
                END AS unexp_length,
                CASE 
                    WHEN code_type = ''09'' THEN CASE WHEN alpha_pos > 0 AND SUBSTR(code_clean, 1, 1) NOT IN (''E'', ''V'') THEN 1 ELSE 0 END
                    /* SAS PCE v15: unexp_alpha=1 when anydigit(code_clean)=1 */
                    WHEN code_type = ''10'' THEN CASE WHEN digit_pos = 1 THEN 1 ELSE 0 END
                    ELSE 0
                END AS unexp_alpha,
                CASE 
                    WHEN code_type = ''09'' THEN CASE WHEN SUBSTR(code_clean, 1, 3) IN (''000'') THEN 1 ELSE 0 END
                    WHEN code_type = ''10'' THEN CASE WHEN SUBSTR(code_clean, 1, 3) IN (''000'', ''999'') THEN 1 ELSE 0 END
                    ELSE 0
                END AS unexp_string,
                CASE 
                    /* SAS PCE v15: unexp_numeric=1 when anydigit(code_clean)=0 */
                    WHEN digit_pos = 0 THEN 1 ELSE 0
                END AS unexp_numeric,
                processed_date
            FROM ${DB_PARAM}.CHARACTERIZATION.DIAGNOSIS_VALIDATION
        ) s
        WHERE GREATEST(unexp_length, unexp_alpha, unexp_string, unexp_numeric) = 1
             `
         });
    }

    // ================================================================================
    // 2. PROCEDURES Table Validation (ICD, CPT/HCPCS, NDC)
    // ================================================================================

    if (tables_to_check.includes(''PROCEDURES'')) {
        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.PROCEDURES_VALIDATION AS
        SELECT 
            ''PROCEDURES'' AS table_name,
            proceduresid,
            px AS code,
            px_type AS code_type,
            CASE 
                WHEN px_type = ''CH'' THEN SUBSTR(UPPER(REGEXP_REPLACE(px, ''[.,\\\\\\\\s]'', '''')), 1, 5)
                ELSE UPPER(REGEXP_REPLACE(px, ''[.,\\\\\\\\s]'', ''''))
            END AS code_clean,
            CASE 
                WHEN px_type = ''CH'' THEN LENGTH(SUBSTR(UPPER(REGEXP_REPLACE(px, ''[.,\\\\\\\\s]'', '''')), 1, 5))
                ELSE LENGTH(UPPER(REGEXP_REPLACE(px, ''[.,\\\\\\\\s]'', '''')))
            END AS code_length,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(px, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                ELSE 0 
            END AS has_alpha,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(px, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                ELSE 0 
            END AS has_digit,
            CURRENT_TIMESTAMP() AS processed_date
        FROM ${DB_PARAM}.${SCHEMA_NAME}.PROCEDURES 
        WHERE px IS NOT NULL 
          AND px_type IN (''CH'', ''09'', ''10'', ''ND'')
        `
    });

    snowflake.execute({
        sqlText: `
        CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_PX AS
        SELECT 
            table_name,
            proceduresid,
            code,
            code_type,
            code_clean,
            code_length,
            CASE 
                WHEN code_type = ''CH'' THEN
                    CASE 
                        WHEN code_length < 5 THEN 1 ELSE 0
                    END
                WHEN code_type = ''09'' THEN
                    CASE 
                        WHEN code_length NOT IN (3,4) THEN 1 ELSE 0
                    END
                WHEN code_type = ''10'' THEN
                    CASE 
                        WHEN code_length != 7 THEN 1 ELSE 0
                    END
                WHEN code_type = ''ND'' THEN
                    CASE 
                        WHEN code_length != 11 THEN 1 ELSE 0
                    END
                ELSE 0
            END AS unexp_length,
            CASE 
                WHEN code_type = ''09'' THEN
                    CASE 
                        WHEN has_alpha > 0 THEN 1 ELSE 0
                    END
                WHEN code_type = ''ND'' THEN
                    CASE 
                        WHEN has_alpha > 0 THEN 1 ELSE 0
                    END
                ELSE NULL
            END AS unexp_alpha,
            CASE 
                WHEN code_type = ''CH'' THEN
                    CASE 
                        WHEN SUBSTR(code_clean, 1, 5) IN (''00000'', ''99999'') THEN 1 ELSE 0
                    END
                WHEN code_type = ''09'' THEN
                    CASE 
                        WHEN code_clean = ''0000'' THEN 1 ELSE 0
                    END
                WHEN code_type = ''10'' THEN
                    CASE 
                        WHEN code_clean IN (''0000000'', ''9999999'') THEN 1 ELSE 0
                    END
                WHEN code_type = ''ND'' THEN
                    CASE 
                        WHEN code_clean IN (''00000000000'', ''99999999999'') THEN 1 ELSE 0
                    END
                ELSE NULL
            END AS unexp_string,
            CASE 
                WHEN code_type = ''CH'' THEN
                    CASE 
                        WHEN has_digit = 0 THEN 1 ELSE 0
                    END
                ELSE NULL
            END AS unexp_numeric,
            processed_date
        FROM ${DB_PARAM}.CHARACTERIZATION.PROCEDURES_VALIDATION
        WHERE (CASE 
                WHEN code_type = ''CH'' THEN
                    CASE 
                        WHEN (code_length < 5 OR
                             SUBSTR(code_clean, 1, 5) IN (''00000'', ''99999'') OR
                             has_digit = 0) THEN 1
                        ELSE 0
                    END
                WHEN code_type = ''09'' THEN
                    CASE 
                        WHEN (code_length NOT IN (3,4) OR
                             has_alpha > 0 OR
                             code_clean = ''0000'') THEN 1
                        ELSE 0
                    END
                WHEN code_type = ''10'' THEN
                    CASE 
                        WHEN (code_length != 7 OR
                             code_clean IN (''0000000'', ''9999999'')) THEN 1
                        ELSE 0
                    END
                WHEN code_type = ''ND'' THEN
                    CASE 
                        WHEN (code_length != 11 OR
                             has_alpha > 0 OR
                             code_clean IN (''00000000000'', ''99999999999'')) THEN 1
                        ELSE 0
                    END
                ELSE 0
            END) = 1
            `
        });
    }

    // ================================================================================
    // 3. PRESCRIBING Table Validation (RXCUI)
    // ================================================================================

    if (tables_to_check.includes(''PRESCRIBING'')) {
        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.PRESCRIBING_VALIDATION AS
        SELECT 
            ''PRESCRIBING'' AS table_name,
            prescribingid,
            rxnorm_cui AS code,
            ''RX'' AS code_type,
            UPPER(REGEXP_REPLACE(rxnorm_cui, ''[.,\\\\\\\\s]'', '''')) AS code_clean,
            LENGTH(UPPER(REGEXP_REPLACE(rxnorm_cui, ''[.,\\\\\\\\s]'', ''''))) AS code_length,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(rxnorm_cui, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                ELSE 0 
            END AS has_alpha,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(rxnorm_cui, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                ELSE 0 
            END AS has_digit,
            CURRENT_TIMESTAMP() AS processed_date
        FROM ${DB_PARAM}.${SCHEMA_NAME}.PRESCRIBING 
        WHERE rxnorm_cui IS NOT NULL
        `
    });

    snowflake.execute({
        sqlText: `
        CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_PRES AS
        SELECT 
            table_name,
            prescribingid,
            code,
            code_type,
            code_clean,
            code_length,
            CASE 
                WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0
            END AS unexp_length,
            CASE 
                WHEN has_alpha > 0 THEN 1 ELSE 0
            END AS unexp_alpha,
            NULL AS unexp_string,
            NULL AS unexp_numeric,
            processed_date
        FROM ${DB_PARAM}.CHARACTERIZATION.PRESCRIBING_VALIDATION
        WHERE (code_length < 2 OR code_length > 7 OR has_alpha > 0)
            `
        });
    }

    // ================================================================================
    // 4. DISPENSING Table Validation (NDC)
    // ================================================================================

    if (tables_to_check.includes(''DISPENSING'')) {
        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.DISPENSING_VALIDATION AS
        SELECT 
            ''DISPENSING'' AS table_name,
            dispensingid,
            ndc AS code,
            ''ND'' AS code_type,
            UPPER(REGEXP_REPLACE(ndc, ''[.,\\\\\\\\s]'', '''')) AS code_clean,
            LENGTH(UPPER(REGEXP_REPLACE(ndc, ''[.,\\\\\\\\s]'', ''''))) AS code_length,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(ndc, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                ELSE 0 
            END AS has_alpha,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(ndc, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                ELSE 0 
            END AS has_digit,
            CURRENT_TIMESTAMP() AS processed_date
        FROM ${DB_PARAM}.${SCHEMA_NAME}.DISPENSING 
        WHERE ndc IS NOT NULL
        `
    });

    snowflake.execute({
        sqlText: `
        CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_DISP AS
        SELECT 
            table_name,
            dispensingid,
            code,
            code_type,
            code_clean,
            code_length,
            CASE 
                WHEN code_length != 11 THEN 1 ELSE 0
            END AS unexp_length,
            CASE 
                WHEN has_alpha > 0 THEN 1 ELSE 0
            END AS unexp_alpha,
            CASE 
                WHEN code_clean IN (''00000000000'', ''99999999999'') THEN 1 ELSE 0
            END AS unexp_string,
            NULL AS unexp_numeric,
            processed_date
        FROM ${DB_PARAM}.CHARACTERIZATION.DISPENSING_VALIDATION
        WHERE (code_length != 11 OR has_alpha > 0 OR code_clean IN (''00000000000'', ''99999999999''))
            `
        });
    }

    // ==============================================================================
    // 4b. MED_ADMIN Table Validation (RXCUI and NDC) - matches SAS PCE v15
    // ==============================================================================

    if (tables_to_check.includes(''MED_ADMIN'')) {
        try {
            snowflake.execute({
                sqlText: `
                CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.MED_ADMIN_VALIDATION AS
            SELECT 
                ''MED_ADMIN'' AS table_name,
                medadminid,
                medadmin_code AS code,
                medadmin_type AS code_type,
                UPPER(REGEXP_REPLACE(medadmin_code, ''[.,\\\\s]'', '''')) AS code_clean,
                LENGTH(UPPER(REGEXP_REPLACE(medadmin_code, ''[.,\\\\s]'', ''''))) AS code_length,
                CASE 
                    WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(medadmin_code, ''[.,\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                    ELSE 0 
                END AS has_alpha,
                CASE 
                    WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(medadmin_code, ''[.,\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                    ELSE 0 
                END AS has_digit,
                REGEXP_INSTR(UPPER(REGEXP_REPLACE(medadmin_code, ''[.,\\\\s]'', '''')), ''[A-Z]'') AS alpha_pos,
                REGEXP_INSTR(UPPER(REGEXP_REPLACE(medadmin_code, ''[.,\\\\s]'', '''')), ''[0-9]'') AS digit_pos,
                CURRENT_TIMESTAMP() AS processed_date
            FROM ${DB_PARAM}.${SCHEMA_NAME}.MED_ADMIN
            WHERE medadmin_code IS NOT NULL
              AND medadmin_type IN (''RX'', ''ND'')
                `
            });

            snowflake.execute({
                sqlText: `
                CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_MEDADMIN AS
            SELECT *
            FROM (
                SELECT 
                    table_name,
                    medadminid,
                    code,
                    code_type,
                    code_clean,
                    code_length,
                    CASE 
                        WHEN code_type = ''RX'' THEN CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END
                        WHEN code_type = ''ND'' THEN CASE WHEN code_length != 11 THEN 1 ELSE 0 END
                        ELSE 0
                    END AS unexp_length,
                    CASE 
                        WHEN code_type IN (''RX'', ''ND'') THEN CASE WHEN alpha_pos > 0 THEN 1 ELSE 0 END
                        ELSE 0
                    END AS unexp_alpha,
                    CASE 
                        WHEN code_type = ''ND'' THEN CASE WHEN code_clean IN (''00000000000'', ''99999999999'') THEN 1 ELSE 0 END
                        ELSE NULL
                    END AS unexp_string,
                    NULL AS unexp_numeric,
                    processed_date
                FROM ${DB_PARAM}.CHARACTERIZATION.MED_ADMIN_VALIDATION
            ) s
            WHERE GREATEST(
                COALESCE(unexp_length, 0),
                COALESCE(unexp_alpha, 0),
                COALESCE(unexp_string, 0),
                COALESCE(unexp_numeric, 0)
            ) = 1
                `
            });
        } catch (err) {
            // Table might not exist, skip validation
        }
    }

    // ================================================================================
    // 5. LAB_RESULT_CM Table Validation (LOINC)
    // ================================================================================

    if (tables_to_check.includes(''LAB_RESULT_CM'')) {
        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.LAB_RESULT_CM_VALIDATION AS
        SELECT 
            ''LAB_RESULT_CM'' AS table_name,
            lab_result_cm_id,
            lab_loinc AS code,
            ''LC'' AS code_type,
            UPPER(REGEXP_REPLACE(lab_loinc, ''[.,\\\\\\\\s]'', '''')) AS code_clean,
            REVERSE(UPPER(REGEXP_REPLACE(lab_loinc, ''[.,\\\\\\\\s]'', ''''))) AS code_clean_reversed,
            LENGTH(UPPER(REGEXP_REPLACE(lab_loinc, ''[.,\\\\\\\\s]'', ''''))) AS code_length,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(lab_loinc, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                ELSE 0 
            END AS has_alpha,
            CASE 
                WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(lab_loinc, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                ELSE 0 
            END AS has_digit,
            CURRENT_TIMESTAMP() AS processed_date
        FROM ${DB_PARAM}.${SCHEMA_NAME}.LAB_RESULT_CM 
        WHERE lab_loinc IS NOT NULL
        `
    });

    snowflake.execute({
        sqlText: `
        CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_LAB AS
        SELECT 
            table_name,
            lab_result_cm_id,
            code,
            code_type,
            code_clean,
            code_length,
            CASE 
                WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0
            END AS unexp_length,
            CASE 
                WHEN has_alpha > 0 THEN 1 ELSE 0
            END AS unexp_alpha,
            CASE 
                WHEN SUBSTR(code_clean_reversed, 2, 1) != ''-'' THEN 1 ELSE 0
            END AS unexp_string,
            NULL AS unexp_numeric,
            processed_date
        FROM ${DB_PARAM}.CHARACTERIZATION.LAB_RESULT_CM_VALIDATION
        WHERE (code_length < 3 OR code_length > 8 OR has_alpha > 0 OR SUBSTR(code_clean_reversed, 2, 1) != ''-'')
        `
    });
    }

    // ================================================================================
    // 6. Additional Table Validations (with error handling)
    // ================================================================================

    // CONDITION Table Validation
    if (tables_to_check.includes(''CONDITION'')) {
        try {
            snowflake.execute({
                sqlText: `
                CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.CONDITION_VALIDATION AS
            SELECT 
                ''CONDITION'' AS table_name,
                conditionid,
                condition AS code,
                condition_type AS code_type,
                UPPER(REGEXP_REPLACE(condition, ''[.,\\\\\\\\s]'', '''')) AS code_clean,
                LENGTH(UPPER(REGEXP_REPLACE(condition, ''[.,\\\\\\\\s]'', ''''))) AS code_length,
                CASE 
                    WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(condition, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                    ELSE 0 
                END AS has_alpha,
                CASE 
                    WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(condition, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                    ELSE 0 
                END AS has_digit,
                REGEXP_INSTR(UPPER(REGEXP_REPLACE(condition, ''[.,\\\\s]'', '''')), ''[A-Z]'') AS alpha_pos,
                REGEXP_INSTR(UPPER(REGEXP_REPLACE(condition, ''[.,\\\\s]'', '''')), ''[0-9]'') AS digit_pos,
                CURRENT_TIMESTAMP() AS processed_date
            FROM ${DB_PARAM}.${SCHEMA_NAME}.CONDITION 
            WHERE condition IS NOT NULL 
              AND condition_type IN (''09'', ''10'')
            `
        });

        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_CONDITION AS
            SELECT *
            FROM (
                SELECT 
                    table_name,
                    conditionid,
                    code,
                    code_type,
                    code_clean,
                    code_length,
                    CASE 
                        WHEN code_type = ''09'' THEN CASE WHEN code_length NOT IN (3,4,5) THEN 1 ELSE 0 END
                        WHEN code_type = ''10'' THEN CASE WHEN code_length NOT IN (3,4,5,6,7) THEN 1 ELSE 0 END
                        ELSE 0
                    END AS unexp_length,
                    CASE 
                    WHEN code_type = ''09'' THEN CASE WHEN alpha_pos > 0 AND SUBSTR(code_clean, 1, 1) NOT IN (''E'', ''V'') THEN 1 ELSE 0 END
                        /* SAS PCE v15: unexp_alpha=1 when anydigit(code_clean)=1 */
                    WHEN code_type = ''10'' THEN CASE WHEN digit_pos = 1 THEN 1 ELSE 0 END
                        ELSE 0
                    END AS unexp_alpha,
                    CASE 
                        WHEN code_type = ''09'' THEN CASE WHEN SUBSTR(code_clean, 1, 3) IN (''000'') THEN 1 ELSE 0 END
                        WHEN code_type = ''10'' THEN CASE WHEN SUBSTR(code_clean, 1, 3) IN (''000'', ''999'') THEN 1 ELSE 0 END
                        ELSE 0
                    END AS unexp_string,
                    CASE 
                    WHEN digit_pos = 0 THEN 1 ELSE 0
                    END AS unexp_numeric,
                    processed_date
                FROM ${DB_PARAM}.CHARACTERIZATION.CONDITION_VALIDATION
            ) s
            WHERE GREATEST(unexp_length, unexp_alpha, unexp_string, unexp_numeric) = 1
                `
            });
        } catch (err) {
            // Table might not exist, skip validation
        }
    }

    // IMMUNIZATION Table Validation
    if (tables_to_check.includes(''IMMUNIZATION'')) {
        try {
            snowflake.execute({
                sqlText: `
                CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.IMMUNIZATION_VALIDATION AS
            SELECT 
                ''IMMUNIZATION'' AS table_name,
                immunizationid,
                vx_code AS code,
                vx_code_type AS code_type,
                CASE 
                    WHEN vx_code_type = ''CH'' THEN SUBSTR(UPPER(REGEXP_REPLACE(vx_code, ''[.,\\\\\\\\s]'', '''')), 1, 5)
                    ELSE UPPER(REGEXP_REPLACE(vx_code, ''[.,\\\\\\\\s]'', ''''))
                END AS code_clean,
                CASE 
                    WHEN vx_code_type = ''CH'' THEN LENGTH(SUBSTR(UPPER(REGEXP_REPLACE(vx_code, ''[.,\\\\\\\\s]'', '''')), 1, 5))
                    ELSE LENGTH(UPPER(REGEXP_REPLACE(vx_code, ''[.,\\\\\\\\s]'', '''')))
                END AS code_length,
                CASE 
                    WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(vx_code, ''[.,\\\\\\\\s]'', '''')), ''[A-Z]'') > 0 THEN 1 
                    ELSE 0 
                END AS has_alpha,
                CASE 
                    WHEN REGEXP_COUNT(UPPER(REGEXP_REPLACE(vx_code, ''[.,\\\\\\\\s]'', '''')), ''[0-9]'') > 0 THEN 1 
                    ELSE 0 
                END AS has_digit,
                CURRENT_TIMESTAMP() AS processed_date
            FROM ${DB_PARAM}.${SCHEMA_NAME}.IMMUNIZATION 
            WHERE vx_code IS NOT NULL 
              AND vx_code_type IN (''CX'', ''ND'', ''RX'', ''CH'')
            `
        });

        snowflake.execute({
            sqlText: `
            CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.BAD_IMMUNIZATION AS
            SELECT 
                table_name,
                immunizationid,
                code,
                code_type,
                code_clean,
                code_length,
                CASE 
                    WHEN code_type = ''RX'' THEN
                        CASE 
                            WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0
                        END
                    WHEN code_type = ''ND'' THEN
                        CASE 
                            WHEN code_length != 11 THEN 1 ELSE 0
                        END
                    WHEN code_type = ''CH'' THEN
                        CASE 
                            WHEN code_length < 5 THEN 1 ELSE 0
                        END
                    WHEN code_type = ''CX'' THEN
                        CASE 
                            WHEN code_length NOT IN (2,3) THEN 1 ELSE 0
                        END
                    ELSE 0
                END AS unexp_length,
                CASE 
                    WHEN code_type IN (''RX'', ''ND'', ''CX'') THEN
                        CASE 
                            WHEN has_alpha > 0 THEN 1 ELSE 0
                        END
                    ELSE NULL
                END AS unexp_alpha,
                CASE 
                    WHEN code_type = ''ND'' THEN
                        CASE 
                            WHEN code_clean IN (''00000000000'', ''99999999999'') THEN 1 ELSE 0
                        END
                    WHEN code_type = ''CH'' THEN
                        CASE 
                            WHEN SUBSTR(code_clean, 1, 5) IN (''00000'', ''99999'') THEN 1 ELSE 0
                        END
                    ELSE NULL
                END AS unexp_string,
                CASE 
                    WHEN code_type = ''CH'' THEN
                        CASE 
                            WHEN has_digit = 0 THEN 1 ELSE 0
                        END
                    ELSE NULL
                END AS unexp_numeric,
                processed_date
            FROM ${DB_PARAM}.CHARACTERIZATION.IMMUNIZATION_VALIDATION
            WHERE (CASE 
                    WHEN code_type = ''RX'' THEN
                        CASE 
                            WHEN (code_length < 2 OR code_length > 7 OR has_alpha > 0) THEN 1 ELSE 0
                        END
                    WHEN code_type = ''ND'' THEN
                        CASE 
                            WHEN (code_length != 11 OR has_alpha > 0 OR code_clean IN (''00000000000'', ''99999999999'')) THEN 1 ELSE 0
                        END
                    WHEN code_type = ''CH'' THEN
                        CASE 
                            WHEN (code_length < 5 OR SUBSTR(code_clean, 1, 5) IN (''00000'', ''99999'') OR has_digit = 0) THEN 1 ELSE 0
                        END
                    WHEN code_type = ''CX'' THEN
                        CASE 
                            WHEN (has_alpha > 0 OR code_length NOT IN (2,3)) THEN 1 ELSE 0
                        END
                    ELSE 0
                END) = 1
                `
            });
        } catch (err) {
            // Table might not exist, skip validation
        }
    }

    // ================================================================================
// 7. Summary Statistics
// ================================================================================

// Build the base_counts query dynamically
let base_counts_queries = [];
if (tables_to_check.includes(''DIAGNOSIS'')) {
    base_counts_queries.push(`
    SELECT
    ''DIAGNOSIS'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.DIAGNOSIS_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''PROCEDURES'')) {
    base_counts_queries.push(`
    SELECT
    ''PROCEDURES'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.PROCEDURES_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''PRESCRIBING'')) {
    base_counts_queries.push(`
    SELECT
    ''PRESCRIBING'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.PRESCRIBING_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''DISPENSING'')) {
    base_counts_queries.push(`
    SELECT
    ''DISPENSING'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.DISPENSING_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''MED_ADMIN'')) {
    base_counts_queries.push(`
    SELECT
    ''MED_ADMIN'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.MED_ADMIN_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''LAB_RESULT_CM'')) {
    base_counts_queries.push(`
    SELECT
    ''LAB_RESULT_CM'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.LAB_RESULT_CM_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''CONDITION'')) {
    base_counts_queries.push(`
    SELECT
    ''CONDITION'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.CONDITION_VALIDATION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''IMMUNIZATION'')) {
    base_counts_queries.push(`
    SELECT
    ''IMMUNIZATION'' AS table_name,
    code_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT code_clean) AS total_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.IMMUNIZATION_VALIDATION
    GROUP BY code_type`);
}

// Build the bad_counts query dynamically
let bad_counts_queries = [];
if (tables_to_check.includes(''DIAGNOSIS'')) {
    bad_counts_queries.push(`
    SELECT
    ''DIAGNOSIS'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_DX
    GROUP BY code_type`);
}

if (tables_to_check.includes(''PROCEDURES'')) {
    bad_counts_queries.push(`
    SELECT
    ''PROCEDURES'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_PX
    GROUP BY code_type`);
}

if (tables_to_check.includes(''PRESCRIBING'')) {
    bad_counts_queries.push(`
    SELECT
    ''PRESCRIBING'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_PRES
    GROUP BY code_type`);
}

if (tables_to_check.includes(''DISPENSING'')) {
    bad_counts_queries.push(`
    SELECT
    ''DISPENSING'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_DISP
    GROUP BY code_type`);
}

if (tables_to_check.includes(''MED_ADMIN'')) {
    bad_counts_queries.push(`
    SELECT
    ''MED_ADMIN'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_MEDADMIN
    GROUP BY code_type`);
}

if (tables_to_check.includes(''LAB_RESULT_CM'')) {
    bad_counts_queries.push(`
    SELECT
    ''LAB_RESULT_CM'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_LAB
    GROUP BY code_type`);
}

if (tables_to_check.includes(''CONDITION'')) {
    bad_counts_queries.push(`
    SELECT
    ''CONDITION'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_CONDITION
    GROUP BY code_type`);
}

if (tables_to_check.includes(''IMMUNIZATION'')) {
    bad_counts_queries.push(`
    SELECT
    ''IMMUNIZATION'' AS table_name,
    code_type,
    COUNT(*) AS bad_records,
    COUNT(DISTINCT code_clean) AS bad_codes
    FROM ${DB_PARAM}.CHARACTERIZATION.BAD_IMMUNIZATION
    GROUP BY code_type`);
}

// Create the final query
if (base_counts_queries.length === 0) {
    snowflake.execute({
        sqlText: `
        CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.CODE_SUMMARY (
            table_name STRING,
            code_type STRING,
            total_records NUMBER,
            total_codes NUMBER,
            bad_records NUMBER,
            bad_codes NUMBER,
            bad_record_pct NUMBER,
            datamartid STRING,
            query_date DATE
        )
        `
    });
} else {
    snowflake.execute({
        sqlText: `
        CREATE OR REPLACE TABLE ${DB_PARAM}.CHARACTERIZATION.CODE_SUMMARY AS
        WITH base_counts AS (
        ${base_counts_queries.join('' UNION ALL '')}
        ),
        bad_counts AS (
        ${bad_counts_queries.join('' UNION ALL '')}
        )
        SELECT
        COALESCE(bc.table_name, a.table_name) AS table_name,
        COALESCE(bc.code_type, a.code_type) AS code_type,
        a.total_records,
        a.total_codes,
        COALESCE(bc.bad_records, 0) AS bad_records,
        COALESCE(bc.bad_codes, 0) AS bad_codes,
        CASE
        WHEN a.total_records > 0 THEN (COALESCE(bc.bad_records, 0)::DECIMAL / a.total_records::DECIMAL) * 100
        ELSE 0
        END AS bad_record_pct,
        ''${DB_PARAM}'' AS datamartid,
        CURRENT_DATE() AS query_date
        FROM base_counts a
        LEFT JOIN bad_counts bc
        ON a.table_name = bc.table_name
        AND a.code_type = bc.code_type
        ORDER BY a.table_name, a.code_type
        `
    });
}

    // ================================================================================
    // Validation Summary Report (returned as result)
    // ================================================================================

    // Get the count of tables with validation issues
    const rs = snowflake.execute({
        sqlText: `
        SELECT COUNT(*) AS issue_count 
        FROM ${DB_PARAM}.CHARACTERIZATION.CODE_SUMMARY 
        WHERE bad_records > 0
        `
    });

    let issue_count = 0;
    if (rs.next()) {
        issue_count = rs.getColumnValue(1);
    }

            return ''Potential Code Errors validation completed successfully. Found '' + 
            issue_count + 
            '' tables with validation issues. Results stored in '' + DB_PARAM + ''.CHARACTERIZATION schema.'';

';