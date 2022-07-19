-- USE_PGXS=1 make
-- USE_PGXS=1 make install
--hmval字段为SimHash，hmarr字段为把SimHash切分为4片的数组
CREATE TABLE test_cube (id INT, hmval BIT(64), hmarr TEXT[]);

-- create or replace function gen_rand_sig (int, int) returns text as $$  
--   select string_agg((random()*$1)::int::text,',') from generate_series(1,$2);  
-- $$ language sql strict;  

-- CREATE OR REPLACE FUNCTION gen_sig(t text, part int) RETURNS text[] as $$  
-- DECLARE
-- ans text[];
-- n integer;
-- BEGIN
--   n = length(t)/part;
-- 	FOR I in 1..n LOOP
--     ans[I] = add_delim(substr(t,(I-1)*part+1, part));
--   END LOOP;
-- RETURN ans;
-- END
-- $$
-- LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION add_delim(arr text) RETURNS text as $$  
  select string_agg(foo.unnest,',') from (select unnest(regexp_split_to_array(arr,''))) foo;
$$
LANGUAGE sql strict; 

-- CREATE OR REPLACE FUNCTION gen_sig(nums int[]) RETURNS text[] as $$  
-- DECLARE
-- temp text;
-- ans text[];
-- BEGIN
--   SELECT gen_bit(nums) INTO temp;
--   SELECT gen_sig(temp, 16) INTO ans;
-- RETURN ans;
-- END
-- $$
-- LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gen_bit(nums int[]) RETURNS text as $$  
  select string_agg(foo.unnest::BIT(8)::text,'') from (select unnest(nums)) foo;
$$
LANGUAGE sql strict; 

CREATE OR REPLACE FUNCTION add_delim(arr text) RETURNS text as $$  
  select string_agg(foo.unnest,',') from (select unnest(regexp_split_to_array(arr,''))) foo;
$$
LANGUAGE sql strict; 

create extension cube;  

create table test_cube (  
  id int primary key,   -- 主键 
  val text    
);  

insert into test_cube select id, '('||add_delim(val)||')' from  
(select id, gen_bit(val::int[]) val
from test_hsp) foo;

create index idx_test_cube on test_cube using gist((val::cube)); 

select id, pow(dis, 2)::int dis
from 
(select id, val, 
val::cube <->   
'(0,0,0,0,1,1,0,0,0,0,1,0,1,1,1,1,1,1,0,0,0,0,1,1,1,0,0,1,0,0,1,0,0,1,0,0,1,0,1,0,0,0,1,1,1,0,1,1,0,0,0,0,1,0,0,1,0,1,1,0,0,0,0,1)'::cube dis
from test_cube 
order by dis
limit 10) foo;


create extension vector;

CREATE TABLE test_vector (id int, val vector(64));

INSERT INTO test_vector SELECT id, ('['||add_delim(gen_bit(val::int[]))||']')::vector(64)
from test_hsp; 

CREATE INDEX ON test_vector USING ivfflat (val vector_l2_ops) WITH (lists = 1000);

SET max_parallel_workers_per_gather = 1;

SET ivfflat.probes = 10;

SELECT id, pow(dis,2)::int dis from (SELECT id,val <-> '[0,0,0,0,1,1,0,0,0,0,1,0,1,1,1,1,1,1,0,0,0,0,1,1,1,0,0,1,0,0,1,0,0,1,0,0,1,0,1,0,0,0,1,1,1,0,1,1,0,0,0,0,1,0,0,1,0,1,1,0,0,0,0,1]'::vector(64) dis
FROM test_vector ORDER BY dis LIMIT 10) foo;



drop index test_vector_val_idx;

create table test_cube (  
  id int primary key,   -- 主键 
  c1 text,   -- 第一组，16个float4，逗号分隔  
  c2 text,   
  c3 text,   
  c4 text    
);  

insert into test_cube select id, sig[1],sig[2],sig[3],sig[4] from  
(select id, gen_sig(val::int[]) sig
from test_hsp) foo; 

create index idx_test_cube_1 on test_cube using gist ((('('||c1||')')::cube));  
create index idx_test_cube_2 on test_cube using gist ((('('||c2||')')::cube));  
create index idx_test_cube_3 on test_cube using gist ((('('||c3||')')::cube));  
create index idx_test_cube_4 on test_cube using gist ((('('||c4||')')::cube));  

create index idx_test_cube_5 on test_cube using gist ((('('||c1||','||c2||','||c3||','||c4||')')::cube)); 

with   
a as (select id from test_cube order by (('('||c1||')')::cube) <-> cube '(0,1,0,0,1,1,0,1,1,1,1,0,0,0,0,0)' limit 100),  
b as (select id from test_cube order by (('('||c2||')')::cube) <-> cube '(0,1,0,1,1,0,1,0,1,1,0,0,0,0,0,1)' limit 100),  
c as (select id from test_cube order by (('('||c3||')')::cube) <-> cube '(1,1,1,1,1,1,0,1,1,1,1,0,1,0,1,1)' limit 100),  
d as (select id from test_cube order by (('('||c4||')')::cube) <-> cube '(1,0,0,0,1,0,0,1,1,0,1,1,0,1,1,0)' limit 100)  
select id, (('('||c1||','||c2||','||c3||','||c4||')')::cube) sig, 
(('('||c1||','||c2||','||c3||','||c4||')')::cube) <->   
cube '(0,1,0,0,1,1,0,1,1,1,1,0,0,0,0,0,0,1,0,1,1,0,1,0,1,1,0,0,0,0,0,1,1,1,1,1,1,1,0,1,1,1,1,0,1,0,1,1,1,0,0,0,1,0,0,1,1,0,1,1,0,1,1,0)' dis
from test_cube where id = any (array(  
select id from a   
union all   
select id from b   
union all   
select id from c   
union all   
select id from d     
)) 
order by dis
limit 10; 

select id, (('('||c1||','||c2||','||c3||','||c4||')')::cube) sig, 
(('('||c1||','||c2||','||c3||','||c4||')')::cube) <->   
cube '(0,1,0,0,1,1,0,1,1,1,1,0,0,0,0,0,0,1,0,1,1,0,1,0,1,1,0,0,0,0,0,1,1,1,1,1,1,1,0,1,1,1,1,0,1,0,1,1,1,0,0,0,1,0,0,1,1,0,1,1,0,1,1,0)' dis
from test_cube 
order by dis
limit 10;


CREATE TABLE test_smlar (id INT, hmval BIT(64), hmarr TEXT[]);

INSERT INTO test_smlar (id, hmval, hmarr)
SELECT id,
val::bit(64),
regexp_split_to_array('1_' || substring(val, 1, 16) || ',2_' ||
substring(val, 17, 16) || ',3_' ||
substring(val, 33, 16) || ',4_' ||
substring(val, 49, 16), ',')
FROM (SELECT id,
(sqrt(random())::NUMERIC * 9223372036854775807 * 2 - 9223372036854775807::NUMERIC)::int8::bit(64)::text AS val
FROM generate_series(1, 1000000) t(id)) t;


--查询海明距离小于3的数据，不切分，不建索引需要近3秒
SELECT * FROM test_smlar WHERE LENGTH(REPLACE(BITXOR(BIT'0110010110010001111010100101000000110010111100110110110110100111', hmval)::TEXT,'0','')) < 3;


CREATE INDEX idx_hmarr_test ON test_smlar USING GIN(hmarr _text_sml_ops );


--设置smlar参数，查询至少需要2个切分的块一样
set smlar.type = overlap;

set smlar.threshold = 0.25;

smlar( hmarr, '{1_0010111100011111,2_1011000101000100,3_0111001010010001,4_0100010001001101}')
--通过切分，使用数组相似性加持索引，收敛数据，查询不到1毫秒
SELECT id
FROM test_smlar_4
WHERE hmarr % '{1_0010111100011111,2_1011000101000100,3_0111001010010001,4_0100010001001101}'
AND LENGTH(REPLACE(BITXOR(BIT'0010111100011111101100010100010001110010100100010100010001001101', hmval)::TEXT, '0', '')) < 3; 

--执行计划
set smlar.threshold = 0.1;
select    
    *,    
    smlar( hmarr, '{1_0110010110010001,2_1110101001010000,3_0011001011110011,4_1111001101101101}')    
  from    
    hm3  
  where    
    hmarr % '{1_0110010110010001,2_1110101001010000,3_0011001011110011,4_1111001101101101}'      
    and length(replace(bitxor(bit'0110010110010001111010100101000000110010111100110110110110100111', hmval)::text,'0','')) < 10  
  limit 100;



CREATE TABLE test_smlar_8 (id INT, hmval BIT(64), hmarr TEXT[]);

INSERT INTO test_smlar_8 (id, hmval, hmarr)
SELECT id,
val::bit(64),
regexp_split_to_array('1_' || substring(val, 1, 8) 
|| ',2_' ||substring(val, 9, 8) 
|| ',3_' ||substring(val, 17, 8) 
|| ',4_' ||substring(val, 25, 8)
|| ',5_' ||substring(val, 33, 8)
|| ',6_' ||substring(val, 41, 8)
|| ',7_' ||substring(val, 49, 8)
|| ',8_' ||substring(val, 57, 8)
, ',')
FROM (SELECT id,
(sqrt(random())::NUMERIC * 9223372036854775807 * 2 - 9223372036854775807::NUMERIC)::int8::bit(64)::text AS val
FROM generate_series(1, 1000000) t(id)) t;

CREATE INDEX idx_hmarr_test_8 ON test_smlar_8 USING GIN(hmarr _text_sml_ops );

SELECT id
FROM test_smlar_8
WHERE hmarr % '{1_00000101,2_01111011,3_10001010,4_01110100,5_00100000,6_11110011,7_01111001,8_11110011}'
