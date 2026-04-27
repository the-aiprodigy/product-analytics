SELECT * FROM pbi_user.stage_accounts;

SELECT * FROM pbi_user.stage_users;

SELECT * FROM pbi_user.stage_deals;

SELECT * FROM pbi_user.stage_product_events;

--  check the number of rows and columns for all tables 
SELECT 
    owner,
    table_name,
    TO_NUMBER(EXTRACTVALUE(XMLTYPE(
        DBMS_XMLGEN.GETXML('SELECT COUNT(*) c FROM "' || owner || '"."' || table_name || '"')
    ), '/ROWSET/ROW/C')) AS exact_row_count,
    (SELECT COUNT(*) FROM all_tab_columns c 
     WHERE c.table_name = t.table_name AND c.owner = t.owner) AS column_count
FROM all_tables t
WHERE table_name LIKE 'STAGE_%'
  AND owner IN ('SCHEMA_OWNER', 'PBI_USER')
ORDER BY owner, table_name;
