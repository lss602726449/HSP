CREATE EXTENSION HSP;

CREATE TABLE test_gph(id SERIAL PRIMARY KEY, val hmcode(@dim));

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  

INSERT INTO test_gph(val) SELECT gen_uint8_arr(@len) FROM generate_series(1,@datasize);

SELECT set_m(@m);

SELECT set_alpha(@alpha);

SELECT create_index('test_gph', 'val');

SELECT gen_uint8_arr(@len) as q into query;

SELECT * FROM query;

\timing ON 

CREATE TABLE test AS SELECT id, dis from query, gph_search_knn(query.q, 'test_gph', 'val', @k);

CREATE TABLE gt AS SELECT id, hamming_distance(val, query.q) AS dis from test_gph , query ORDER BY dis LIMIT @k;


\timing OFF

SELECT count(*) FROM
gt,test
WHERE test.id = gt.id;

SELECT drop_index('test_gph', 'val');

DROP TABLE test_gph;

DROP TABLE query;

DROP TABLE test;

DROP TABLE gt;

DROP EXTENSION gph CASCADE; 