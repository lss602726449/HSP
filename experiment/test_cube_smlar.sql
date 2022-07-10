-- USE_PGXS=1 make
-- USE_PGXS=1 make install
--hmval字段为SimHash，hmarr字段为把SimHash切分为4片的数组
CREATE TABLE test (id INT, hmval BIT(64), hmarr TEXT[]);


INSERT INTO TEST (id, hmval, hmarr)
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
SELECT * FROM test WHERE LENGTH(REPLACE(BITXOR(BIT'0110010110010001111010100101000000110010111100110110110110100111', hmval)::TEXT,'0','')) < 3;


CREATE INDEX idx_hmarr_test ON test USING GIN(hmarr _text_sml_ops );


--设置smlar参数，查询至少需要2个切分的块一样
set smlar.type = overlap;

set smlar.threshold = 0.5;


--通过切分，使用数组相似性加持索引，收敛数据，查询不到1毫秒
SELECT *,
smlar( hmarr, '{1_1101101011001111,2_0011101110111110,3_0100001110000101,4_1000000111010101}')
FROM test
WHERE hmarr % '{1_1101101011001111,2_0011101110111110,3_0100001110000101,4_1000000111010101}'
AND LENGTH(REPLACE(BITXOR(BIT'1101101011001111001110111011111001000011100001011000000111010101', hmval)::TEXT, '0', '')) < 3; 

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


create extension cube;  
create or replace function gen_rand_sig (int, int) returns text as $$  
  select string_agg((random()*$1)::int::text,',') from generate_series(1,$2);  
$$ language sql strict;  

create table test_sig (  
  id int primary key,   -- 主键  
  c1 text,   -- 第一组，16个float4，逗号分隔  
  c2 text,   
  c3 text,   
  c4 text    
);  

insert into test_sig select id,   
  gen_rand_sig(1,16),  
  gen_rand_sig(1,16),  
  gen_rand_sig(1,16),  
  gen_rand_sig(1,16)  
from generate_series(1,1000000) t(id); 

create index idx_test_sig_1 on test_sig using gist ((('('||c1||')')::cube));  
create index idx_test_sig_2 on test_sig using gist ((('('||c2||')')::cube));  
create index idx_test_sig_3 on test_sig using gist ((('('||c3||')')::cube));  
create index idx_test_sig_4 on test_sig using gist ((('('||c4||')')::cube));  

create index idx_test_sig_6 on test_sig using gist ((('('||c1||','||c2||','||c3||','||c4||')')::cube)); 

with   
a as (select id from test_sig order by (('('||c1||')')::cube) <-> cube '(0,1,0,0,1,1,0,1,1,1,1,0,0,0,0,0)' limit 100),  
b as (select id from test_sig order by (('('||c2||')')::cube) <-> cube '(0,1,0,1,1,0,1,0,1,1,0,0,0,0,0,1)' limit 100),  
c as (select id from test_sig order by (('('||c3||')')::cube) <-> cube '(1,1,1,1,1,1,0,1,1,1,1,0,1,0,1,1)' limit 100),  
d as (select id from test_sig order by (('('||c4||')')::cube) <-> cube '(1,0,0,0,1,0,0,1,1,0,1,1,0,1,1,0)' limit 100)  
select id, (('('||c1||','||c2||','||c3||','||c4||')')::cube) sig, 
(('('||c1||','||c2||','||c3||','||c4||')')::cube) <->   
cube '(0,1,0,0,1,1,0,1,1,1,1,0,0,0,0,0,0,1,0,1,1,0,1,0,1,1,0,0,0,0,0,1,1,1,1,1,1,1,0,1,1,1,1,0,1,0,1,1,1,0,0,0,1,0,0,1,1,0,1,1,0,1,1,0)' dis
from test_sig where id = any (array(  
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
from test_sig 
order by dis
limit 10;