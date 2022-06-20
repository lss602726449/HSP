CREATE EXTENSION hsp;
CREATE EXTENSION hstore;

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;

CREATE TABLE test_hsp_10(val hmcode);

INSERT INTO test_hsp_10(val) SELECT gen_uint8_arr(16) FROM generate_series(1,100000);

CREATE TABLE test_hsp_100(val hmcode);

INSERT INTO test_hsp_100(val) SELECT gen_uint8_arr(16) FROM generate_series(1,1000000);

CREATE TABLE test_hsp_1000(val hmcode);

INSERT INTO test_hsp_1000(val) SELECT gen_uint8_arr(16) FROM generate_series(1,10000000);

CREATE TABLE test_hsp_10000(val hmcode);

INSERT INTO test_hsp_10000(val) SELECT gen_uint8_arr(16) FROM generate_series(1,100000000);


-- array
CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*4294967295)::uint4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;
CREATE TABLE test_array_10(val uint4[]);

INSERT INTO test_array_10(val) SELECT gen_uint8_arr(4) FROM generate_series(1,100000);

CREATE TABLE test_array_100(val hmcode);

INSERT INTO test_array_100(val) SELECT gen_uint8_arr(4) FROM generate_series(1,1000000);

CREATE TABLE test_array_1000(val hmcode);

INSERT INTO test_array_1000(val) SELECT gen_uint8_arr(4) FROM generate_series(1,10000000);

CREATE TABLE test_array_10000(val hmcode);

INSERT INTO test_array_10000(val) SELECT gen_uint8_arr(4) FROM generate_series(1,100000000);

--string 
create or replace function random_string(integer)
returns text as
$$
    select array_to_string(array(select substring('01' FROM (ceil(random()*2))::int FOR 1) FROM generate_series(1, $1)),'');
$$
language sql volatile;

CREATE TABLE test_str_10(val char(128));

INSERT INTO test_str_10(val) SELECT gen_uint8_arr(4) FROM generate_series(1,100000);

CREATE TABLE test_str_100(val hmcode);

INSERT INTO test_str_100(val) SELECT gen_uint8_arr(4) FROM generate_series(1,1000000);

CREATE TABLE test_str_1000(val hmcode);

INSERT INTO test_str_1000(val) SELECT gen_uint8_arr(4) FROM generate_series(1,10000000);

CREATE TABLE test_str_10000(val hmcode);

INSERT INTO test_str_10000(val) SELECT gen_uint8_arr(4) FROM generate_series(1,100000000);

