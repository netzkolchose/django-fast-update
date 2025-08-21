--
-- CTE to insert RETURNING pks in the temp table
--

WITH
ins AS (
  INSERT INTO exampleapp_multibase (b1, b2)
  SELECT b1,b2
  FROM temp_cu_exampleapp_multisub
  ORDER BY copy_id RETURNING id
),

t1 AS (
  SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS copy_id
  FROM ins
)

UPDATE temp_cu_exampleapp_multisub
SET id = t1.id, multibase_ptr_id = t1.id
FROM t1
WHERE temp_cu_exampleapp_multisub.copy_id = t1.copy_id;


--
-- update all dependent sub pk fields
--

UPDATE temp_cu_exampleapp_multisub
SET multibase_ptr_id = id;



DELETE FROM temp_cu_exampleapp_multisub;
DELETE FROM exampleapp_multisub;
DELETE FROM exampleapp_multibase;
INSERT INTO temp_cu_exampleapp_multisub (b1,b2,s1,s2,copy_id) VALUES (1,2,3,4,1), (11,22,33,44,2), (111,222,333,444,3);





WITH
ins AS (
  INSERT INTO table1 (b1, b2)
  SELECT b1, b2
  FROM tmp_table
  ORDER BY copy_id RETURNING id
),
t1 AS (
  SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS copy_id
FROM ins
)
UPDATE tmp_table
SET id = t1.id
FROM t1
WHERE tmp_table.copy_id = t1.copy_id AND tmp_table.copy_id IS NOT NULL






-- https://stackoverflow.com/questions/68597038/postgresql-return-auto-generated-ids-from-copy-from-insertion
BEGIN;

COPY foo(col1,col2) FROM STDIN;

SELECT id FROM foo
WHERE xmin::text = (txid_current() % (2^32)::bigint)::text
ORDER BY id;

COMMIT;

-- better:
SELECT * FROM foo_table
WHERE  xmin = pg_current_xact_id_if_assigned()::xid;
