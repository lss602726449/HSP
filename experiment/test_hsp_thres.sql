CREATE EXTENSION hsp;
CREATE EXTENSION hstore;

CREATE TABLE test_hsp(id SERIAL PRIMARY KEY, val hmcode);

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  

INSERT INTO test_hsp(val) SELECT gen_uint8_arr(7) FROM generate_series(1,10000000);

SELECT set_m(3);

SELECT create_index('test_hsp', 'val');


-- SELECT gen_uint8_arr(8) as q into query;

-- SELECT * FROM query;

-- \timing ON 
SELECT gen_uint8_arr(8) as q into query;

SELECT * FROM query;

\timing ON 

CREATE TABLE test AS SELECT id, hamming_distance(val, query.q) from query, search_thres(query.q, NULL::test_hsp, 'val', 10) ;

CREATE TABLE gt AS SELECT id, hamming_distance(val, query.q) AS dis from test_hsp , query WHERE hamming_distance(val, query.q) < 10 ORDER BY dis;

SELECT * from (SELECT gen_uint8_arr(8)::hmcode as q) query, search_thres(query.q, NULL::test_hsp, 'val', 10) ;
-- \timing OFF

SELECT count(*) FROM
gt,test
WHERE test.id = gt.id;

-- SELECT drop_index('test_gph', 'val');

-- DROP TABLE test_gph;

-- DROP TABLE query;

-- DROP TABLE test;

-- DROP TABLE gt;

-- DROP EXTENSION gph CASCADE; 


CREATE OR REPLACE FUNCTION gen_hamming_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random())::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  