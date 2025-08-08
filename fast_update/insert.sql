;;
;; CTE to insert RETURNING pks in the temp table
;;

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


;;
;; update all dependent sub pk fields
;;

UPDATE temp_cu_exampleapp_multisub
SET multibase_ptr_id = id;



DELETE FROM temp_cu_exampleapp_multisub;
DELETE FROM exampleapp_multisub;
DELETE FROM exampleapp_multibase;
INSERT INTO temp_cu_exampleapp_multisub (b1,b2,s1,s2,copy_id) VALUES (1,2,3,4,1), (11,22,33,44,2), (111,222,333,444,3);
