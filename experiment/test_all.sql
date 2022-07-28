CREATE EXTENSION hsp;
CREATE EXTENSION hstore;

\i sift1m_64.sql

SELECT pg_catalog.set_config('search_path', '"$user",public',false);

SET client_min_messages = notice;

SELECT set_m(3);

SELECT create_index('test_hsp','val');

CREATE EXTENSION bktree;

CREATE TABLE  test_bktree(id SERIAL PRIMARY KEY, val bigint);

CREATE OR REPLACE FUNCTION gen_64(nums int[]) RETURNS bigint as $$  
DECLARE
temp bigint;
BEGIN
	temp = 0;
    FOR I IN 1..(array_upper(nums, 1)-1) LOOP
        temp = temp*256 + nums[I];
    END LOOP;
	temp = temp*128 + (nums[array_upper(nums, 1)]/2);
RETURN temp;
END
$$
LANGUAGE plpgsql;

INSERT INTO test_bktree select id, gen_64(val::int[]) from test_hsp;

CREATE INDEX bk_index_name ON test_bktree USING spgist (val bktree_ops);


CREATE OR REPLACE FUNCTION gen_bit(nums int[]) RETURNS text as $$  
  select string_agg(foo.unnest::BIT(8)::text,'') from (select unnest(nums)) foo;
$$
LANGUAGE sql strict; 

CREATE OR REPLACE FUNCTION gen_arr(t text, part int) RETURNS text[] as $$  
DECLARE
ans text[];
n integer;
BEGIN
  n = length(t)/part;
  FOR I in 1..n LOOP
    ans[I] = substr(t,(I-1)*part+1, part);
  END LOOP;
RETURN ans;
END
$$
LANGUAGE plpgsql;

CREATE EXTENSION smlar;

CREATE TABLE test_smlar_4 (id INT, hmval BIT(64), hmarr TEXT[]);

INSERT INTO test_smlar_4 (id, hmval, hmarr)
SELECT id, gen_bit(val::int[])::bit(64), gen_arr(gen_bit(val::int[]),16)
from test_hsp;

CREATE INDEX idx_hmarr_test_4 ON test_smlar_4 USING GIN(hmarr _text_sml_ops );

CREATE TABLE test_smlar_8 (id INT, hmval BIT(64), hmarr TEXT[]);

INSERT INTO test_smlar_8 (id, hmval, hmarr)
SELECT id, gen_bit(val::int[])::bit(64), gen_arr(gen_bit(val::int[]),8)
from test_hsp;

CREATE INDEX idx_hmarr_test_8 ON test_smlar_8 USING GIN(hmarr _text_sml_ops );

CREATE OR REPLACE FUNCTION test(num int, r int) RETURNS void as $$  
DECLARE
start_time timestamp;
end_time timestamp;
bf_time interval;
thres_time interval;
mih_time interval;
bktree_time interval;
smlar_4_time interval;
smlar_8_time interval;
query hmcode;
divide float;
temp bigint;
rand integer;
smlar_4_query text;
smlar_8_query text;
smlar_4_arr text[];
smlar_8_arr text[];
BEGIN
    bf_time = '0 second';
    thres_time = '0 second';
    bktree_time = '0 second';
	mih_time = '0 second';
    smlar_4_time = '0 second';
    smlar_8_time = '0 second';
    -- RAISE NOTICE 'bf_time: %',bf_time;
    -- RAISE NOTICE 'thres_time: %',thres_time;
    start_time = clock_timestamp(); 
    EXECUTE format('SELECT * from test_hsp where hamming_distance(val, $1) < $2 ')using '{93,67,144,178,39,208,103,247}'::hmcode, r ;
    end_time = clock_timestamp();
    bf_time = bf_time + age(end_time,start_time);
    -- RAISE NOTICE '%',1.0/(1+r);
    FOR I IN 1..num LOOP
		rand = random()*1000000;
        EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using rand into query;

		start_time = clock_timestamp();
        EXECUTE format('SELECT * from search_thres_mih($1, NULL::test_hsp, $2, $3)') using query, 'val', r;
        end_time = clock_timestamp();
        mih_time = mih_time + age(end_time,start_time);

        start_time = clock_timestamp();
        EXECUTE format('SELECT * from search_thres($1, NULL::test_hsp, $2, $3)') using query, 'val', r;
        end_time = clock_timestamp();
        thres_time = thres_time + age(end_time,start_time);

        EXECUTE format('SELECT val from test_bktree WHERE id = $1 ')using rand into temp;
        start_time = clock_timestamp();
        EXECUTE format('SELECT * FROM test_bktree WHERE val <@ ($1, $2);') using temp,r;
        end_time = clock_timestamp();
        bktree_time = bktree_time + age(end_time,start_time);

        IF r<4 THEN
            EXECUTE format('set smlar.threshold = %s;', 1.0-1.0/4*r);
            EXECUTE format('SELECT hmval from test_smlar_4 WHERE id = $1 ')using rand into smlar_4_query;
            EXECUTE format('SELECT hmarr from test_smlar_4 WHERE id = $1 ')using rand into smlar_4_arr;
            start_time = clock_timestamp();
            EXECUTE format('SELECT id FROM test_smlar_4
            WHERE hmarr %% $1
            AND LENGTH(REPLACE(BITXOR($2::bit(64), hmval)::TEXT, $4, $5)) <= $3') using smlar_4_arr,smlar_4_query,r,'0','';
            end_time = clock_timestamp();
            smlar_4_time = smlar_4_time + age(end_time,start_time);
        END IF;

        IF r<8 THEN
            EXECUTE format('set smlar.threshold = %s;', 1.0-1.0/8*r);
            EXECUTE format('SELECT hmval from test_smlar_8 WHERE id = $1 ')using rand into smlar_8_query;
            EXECUTE format('SELECT hmarr from test_smlar_8 WHERE id = $1 ')using rand into smlar_8_arr;
            start_time = clock_timestamp();
            EXECUTE format('SELECT id FROM test_smlar_8
            WHERE hmarr %% $1
            AND LENGTH(REPLACE(BITXOR($2::bit(64), hmval)::TEXT, $4, $5)) <= $3') using smlar_8_arr,smlar_8_query,r,'0','';
            end_time = clock_timestamp();
            smlar_8_time = smlar_8_time + age(end_time,start_time);
        END IF;
        
    END LOOP;  
    divide = num;
    bf_time = bf_time;
    thres_time = thres_time/divide;
	mih_time = mih_time/divide;
    bktree_time = bktree_time/divide;
    RAISE NOTICE 'bf_time: %',bf_time;
    RAISE NOTICE 'thres_time: %',thres_time;
	RAISE NOTICE 'mih_time: %',mih_time;
    RAISE NOTICE 'bktree_time: %',bktree_time;
    IF r<4 THEN
        RAISE NOTICE 'smlar_4_time: %',smlar_4_time/divide;
    END IF;
    IF r<8 THEN
        RAISE NOTICE 'smlar_8_time: %',smlar_8_time/divide;
    END IF;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test() RETURNS void as $$  
DECLARE
    thres  int[] := '{0,1,2,3,4,5,6,7,8}';
BEGIN
    FOR I IN 1..array_upper(thres, 1) LOOP
        RAISE NOTICE 'r: %', thres[I];
        PERFORM test(100,thres[I]);
    END LOOP; 
END
$$
LANGUAGE plpgsql;

select test();

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

create extension vector;

CREATE TABLE test_vector (id int, val vector(64));

INSERT INTO test_vector SELECT id, ('['||add_delim(gen_bit(val::int[]))||']')::vector(64)
from test_hsp; 

CREATE INDEX ON test_vector USING ivfflat (val vector_l2_ops) WITH (lists = 1000);

SET max_parallel_workers_per_gather = 1;

CREATE OR REPLACE FUNCTION test_knn(query_arr int[], k int) RETURNS void as $$  
DECLARE
recall float;
recall_mih float;
start_time timestamp;
end_time timestamp;
bf_time interval;
knn_time interval;
knn_mih_time interval;
query hmcode;
temp integer;
divide float;
rand integer;
num integer;
BEGIN
    recall = 0;
    recall_mih = 0;
    bf_time = '0 second';
    knn_time = '0 second';
    knn_mih_time = '0 second';
    num = array_upper(query_arr, 1);
    -- RAISE NOTICE 'bf_time: %',bf_time;
    -- RAISE NOTICE 'knn_time: %',knn_time;
    FOR I IN 1..array_upper(query_arr, 1) LOOP
        rand = query_arr[I];
        EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using rand into query;
        
        start_time = clock_timestamp(); 
        EXECUTE format('SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
        end_time = clock_timestamp();
        bf_time = bf_time + age(end_time,start_time);
        EXECUTE format('CREATE TEMP TABLE gt AS SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
        
        start_time = clock_timestamp();
        EXECUTE format('SELECT temp.id , hamming_distance(val, $2) AS dis  from search_knn_mih($2, NULL::test_hsp, $3, $1) as temp') using k,query,'val';
        end_time = clock_timestamp();
        knn_mih_time = knn_mih_time + age(end_time,start_time);
        EXECUTE format('CREATE TEMP TABLE test_mih AS SELECT temp.id , hamming_distance(val, $2) AS dis  from search_knn_mih($2, NULL::test_hsp, $3, $1) as temp') using k,query,'val';

        start_time = clock_timestamp();
        EXECUTE format('SELECT temp.id , hamming_distance(val, $2) AS dis  from search_knn($2, NULL::test_hsp, $3, $1) as temp') using k,query,'val';
        end_time = clock_timestamp();
        knn_time = knn_time + age(end_time,start_time);
        EXECUTE format('CREATE TEMP TABLE test AS SELECT temp.id , hamming_distance(val, $2) AS dis  from search_knn($2, NULL::test_hsp, $3, $1) as temp') using k,query,'val';
        
        SELECT count(*) 
        FROM test 
        WHERE test.dis < (SELECT max(dis) from gt) INTO temp;
        recall = recall + temp;
        SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                    ELSE foo1.count
                END
        FROM
        (SELECT count(*) 
        FROM test 
        WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
        (SELECT count(*) 
        FROM gt 
        WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
        recall = recall + temp;

        SELECT count(*) 
        FROM test_mih 
        WHERE test_mih.dis < (SELECT max(dis) from gt) INTO temp;
        recall_mih = recall_mih + temp;
        SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                    ELSE foo1.count
                END
        FROM
        (SELECT count(*) 
        FROM test_mih 
        WHERE test_mih.dis = (SELECT max(dis) from gt)) as foo1,
        (SELECT count(*) 
        FROM gt 
        WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
        recall_mih = recall_mih + temp;
        
        drop table gt;
        drop table test;
        drop table test_mih;
    END LOOP;  
    recall = recall/num/k;
    recall_mih = recall_mih/num/k;
    divide = num;
    bf_time = bf_time/divide;
    knn_time = knn_time/divide;
    knn_mih_time = knn_mih_time/divide;
    RAISE NOTICE 'bf_time: %',bf_time;
    RAISE NOTICE 'knn_time: %',knn_time;
    RAISE NOTICE 'recall: %',recall;
    RAISE NOTICE 'knn_mih_time: %',knn_mih_time;
    RAISE NOTICE 'recall_mih: %',recall_mih;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_bktree(query_arr int[],thres int[], k int) RETURNS void as $$
DECLARE
start_time timestamp;
end_time timestamp;
recall float;
bktree_time interval;
pre interval;
num int;
cnt int;
temp bigint;
BEGIN
    pre = '0 second';
    num = array_upper(query_arr,1);
    recall = 0;
    FOR I IN 1..array_upper(thres, 1) LOOP
        bktree_time = '0 second';
        recall = 0;
        RAISE NOTICE 'thres: %',thres[I];
        FOR J IN 1..array_upper(query_arr, 1) LOOP
            EXECUTE format('SELECT val from test_bktree WHERE id = $1 ')using query_arr[J] into temp;
            start_time = clock_timestamp();
            EXECUTE format('SELECT * FROM test_bktree WHERE val <@ ($1, $2);') using temp,thres[I];
            end_time = clock_timestamp();
            bktree_time = bktree_time + age(end_time,start_time);
            EXECUTE format('SELECT count(*) FROM test_bktree WHERE val <@ ($1, $2);') using temp,thres[I] INTO cnt;
            IF cnt <= k THEN
                recall = recall + cnt;
            ELSE
                recall = recall + k;
            END IF;
        END LOOP;
        -- RAISE NOTICE 'recall %',recall;
        -- RAISE NOTICE 'num %',num;
        -- RAISE NOTICE 'k %',k;
        recall = recall/num/k;
        bktree_time = pre + bktree_time/num;
        pre = bktree_time;
        RAISE NOTICE 'bktree_time: %',bktree_time;
        RAISE NOTICE 'recall: %',recall;
    END LOOP;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_cube(query_arr int[], k int) RETURNS void as $$
DECLARE
start_time timestamp;
end_time timestamp;
recall float;
cube_time interval;
bf_time interval;
num int;
cnt int;
query_val text;
query hmcode;
temp float;
BEGIN
    num = array_upper(query_arr,1);
    recall = 0;
    cube_time = '0 second';
    bf_time = '0 second';
    FOR I IN 1..array_upper(query_arr, 1) LOOP
        EXECUTE format('SELECT val from test_cube WHERE id = $1 ')using query_arr[I] into query_val;
        EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using query_arr[I] into query;

        start_time = clock_timestamp();
        EXECUTE format('select id,
            val::cube <-> $1::cube dis
            from test_cube 
            order by dis limit 10') using query_val;
        end_time = clock_timestamp();
        cube_time = cube_time + age(end_time,start_time);

        EXECUTE format('CREATE TEMP TABLE test AS select id, pow(dis, 2)::int dis from (select id,
            val::cube <-> $1::cube dis
            from test_cube 
            order by dis limit 10) foo') using query_val;

        start_time = clock_timestamp(); 
        EXECUTE format('SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
        end_time = clock_timestamp();
        bf_time = bf_time + age(end_time,start_time);

        EXECUTE format('CREATE TEMP TABLE gt AS SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;


        SELECT count(*) 
        FROM test 
        WHERE test.dis < (SELECT max(dis) from gt) INTO temp;
        recall = recall + temp;
        SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                    ELSE foo1.count
                END
        FROM
        (SELECT count(*) 
        FROM test 
        WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
        (SELECT count(*) 
        FROM gt 
        WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
        recall = recall + temp;

        drop table test;
        drop table gt;
        
    END LOOP;
        -- RAISE NOTICE 'recall %',recall;
        -- RAISE NOTICE 'num %',num;
        -- RAISE NOTICE 'k %',k;
        recall = recall/num/k;
        cube_time = cube_time/num;
        bf_time = bf_time/num;
        RAISE NOTICE 'bf_time: %',bf_time;
        RAISE NOTICE 'cube_time: %',cube_time;
        RAISE NOTICE 'recall: %',recall;
        
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_vector(query_arr int[], probes int[], k int) RETURNS void as $$
DECLARE
start_time timestamp;
end_time timestamp;
recall float;
vector_time interval;
bf_time interval;
num int;
cnt int;
query_val vector;
query hmcode;
temp float;
BEGIN
    num = array_upper(query_arr,1);
    FOR I IN 1..array_upper(probes, 1) LOOP
        recall = 0;
        vector_time = '0 second';
        bf_time = '0 second';
        -- RAISE NOTICE '%', probes[I];
        EXECUTE format('SET ivfflat.probes = %s', probes[I]);
        -- RAISE NOTICE '%', probes[I];
        FOR J IN 1..array_upper(query_arr, 1) LOOP
            EXECUTE format('SELECT val from test_vector WHERE id = $1 ')using query_arr[J] into query_val;
            EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using query_arr[J] into query;

            start_time = clock_timestamp();
            EXECUTE format('SELECT id, val <-> $1::vector(64) dis
            FROM test_vector ORDER BY dis LIMIT 10') using query_val;
            end_time = clock_timestamp();
            vector_time = vector_time + age(end_time,start_time);

            EXECUTE format('CREATE TEMP TABLE test AS select id, pow(dis, 2)::int dis from (SELECT id, val <-> $1::vector(64) dis
            FROM test_vector ORDER BY dis LIMIT 10) foo') using query_val;

            start_time = clock_timestamp(); 
            EXECUTE format('SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
            end_time = clock_timestamp();
            bf_time = bf_time + age(end_time,start_time);

            EXECUTE format('CREATE TEMP TABLE gt AS SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;


            SELECT count(*) 
            FROM test 
            WHERE test.dis < (SELECT max(dis) from gt) INTO temp;
            recall = recall + temp;
            SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                        ELSE foo1.count
                    END
            FROM
            (SELECT count(*) 
            FROM test 
            WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
            (SELECT count(*) 
            FROM gt 
            WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
            recall = recall + temp;

            drop table test;
            drop table gt;
            
        END LOOP;
            -- RAISE NOTICE 'recall %',recall;
            -- RAISE NOTICE 'num %',num;
            -- RAISE NOTICE 'k %',k;
            RAISE NOTICE 'probes: %',probes[I];
            recall = recall/num/k;
            vector_time = vector_time/num;
            bf_time = bf_time/num;
            RAISE NOTICE 'bf_time: %',bf_time;
            RAISE NOTICE 'vector_time: %',vector_time;
            RAISE NOTICE 'recall: %',recall;
            
    END LOOP;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_smlar4(query_arr int[], k int) RETURNS void as $$
DECLARE
start_time timestamp;
end_time timestamp;
recall float;
smlar_4_time interval;
bf_time interval;
smlar_4_query text;
smlar_4_arr text[];
query hmcode;
num integer;
temp integer;
BEGIN
    num = array_upper(query_arr,1);
    FOR I IN 0..3 LOOP
        recall = 0;
        smlar_4_time = '0 second';
        bf_time = '0 second';
        -- RAISE NOTICE '%', probes[I];
        EXECUTE format('set smlar.threshold = %s;', 1.0-1.0/4*I);
        -- RAISE NOTICE '%', probes[I];
        FOR J IN 1..array_upper(query_arr, 1) LOOP
            
            EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using query_arr[J] into query;
            EXECUTE format('SELECT hmval from test_smlar_4 WHERE id = $1 ')using query_arr[J] into smlar_4_query;
            EXECUTE format('SELECT hmarr from test_smlar_4 WHERE id = $1 ')using query_arr[J] into smlar_4_arr;
            
            start_time = clock_timestamp();
            EXECUTE format('SELECT id, LENGTH(REPLACE(BITXOR($2::bit(64), hmval)::TEXT, $4, $5)) dis  FROM test_smlar_4
            WHERE hmarr %% $1
            order by dis limit $3') using smlar_4_arr,smlar_4_query,k,'0','';
            end_time = clock_timestamp();
            smlar_4_time = smlar_4_time + age(end_time,start_time);

            EXECUTE format('CREATE TEMP TABLE test AS SELECT id, LENGTH(REPLACE(BITXOR($2::bit(64), hmval)::TEXT, $4, $5)) dis  FROM test_smlar_4
            WHERE hmarr %% $1
            order by dis limit $3') using smlar_4_arr,smlar_4_query,k,'0','';

            start_time = clock_timestamp(); 
            EXECUTE format('SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
            end_time = clock_timestamp();
            bf_time = bf_time + age(end_time,start_time);

            EXECUTE format('CREATE TEMP TABLE gt AS SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;


            SELECT count(*) 
            FROM test 
            WHERE test.dis < (SELECT max(dis) from gt) INTO temp;
            recall = recall + temp;
            SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                        ELSE foo1.count
                    END
            FROM
            (SELECT count(*) 
            FROM test 
            WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
            (SELECT count(*) 
            FROM gt 
            WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
            recall = recall + temp;

            drop table test;
            drop table gt;
            
        END LOOP;
            -- RAISE NOTICE 'recall %',recall;
            -- RAISE NOTICE 'num %',num;
            -- RAISE NOTICE 'k %',k;
            RAISE NOTICE 'thres: %', 1.0-1.0/4*I;
            recall = recall/num/k;
            smlar_4_time = smlar_4_time/num;
            bf_time = bf_time/num;
            RAISE NOTICE 'bf_time: %',bf_time;
            RAISE NOTICE 'smlar_4_time: %',smlar_4_time;
            RAISE NOTICE 'recall: %',recall;
            
    END LOOP;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_smlar8(query_arr int[], k int) RETURNS void as $$
DECLARE
start_time timestamp;
end_time timestamp;
recall float;
smlar_8_time interval;
bf_time interval;
smlar_8_query text;
smlar_8_arr text[];
query hmcode;
num integer;
temp integer;
BEGIN
    num = array_upper(query_arr,1);
    FOR I IN 0..7 LOOP
        recall = 0;
        smlar_8_time = '0 second';
        bf_time = '0 second';
        -- RAISE NOTICE '%', probes[I];
        EXECUTE format('set smlar.threshold = %s;', 1.0-1.0/8*I);
        -- RAISE NOTICE '%', probes[I];
        FOR J IN 1..array_upper(query_arr, 1) LOOP
            
            EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using query_arr[J] into query;
            EXECUTE format('SELECT hmval from test_smlar_8 WHERE id = $1 ')using query_arr[J] into smlar_8_query;
            EXECUTE format('SELECT hmarr from test_smlar_8 WHERE id = $1 ')using query_arr[J] into smlar_8_arr;
            
            start_time = clock_timestamp();
            EXECUTE format('SELECT id, LENGTH(REPLACE(BITXOR($2::bit(64), hmval)::TEXT, $4, $5)) dis  FROM test_smlar_8
            WHERE hmarr %% $1
            order by dis limit $3') using smlar_8_arr,smlar_8_query,k,'0','';
            end_time = clock_timestamp();
            smlar_8_time = smlar_8_time + age(end_time,start_time);

            EXECUTE format('CREATE TEMP TABLE test AS SELECT id, LENGTH(REPLACE(BITXOR($2::bit(64), hmval)::TEXT, $4, $5)) dis  FROM test_smlar_8
            WHERE hmarr %% $1
            order by dis limit $3') using smlar_8_arr,smlar_8_query,k,'0','';

            start_time = clock_timestamp(); 
            EXECUTE format('SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
            end_time = clock_timestamp();
            bf_time = bf_time + age(end_time,start_time);

            EXECUTE format('CREATE TEMP TABLE gt AS SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;


            SELECT count(*) 
            FROM test 
            WHERE test.dis < (SELECT max(dis) from gt) INTO temp;
            recall = recall + temp;
            SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                        ELSE foo1.count
                    END
            FROM
            (SELECT count(*) 
            FROM test 
            WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
            (SELECT count(*) 
            FROM gt 
            WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
            recall = recall + temp;

            drop table test;
            drop table gt;
            
        END LOOP;
            -- RAISE NOTICE 'recall %',recall;
            -- RAISE NOTICE 'num %',num;
            -- RAISE NOTICE 'k %',k;
            RAISE NOTICE 'thres: %', 1.0-1.0/8*I;
            recall = recall/num/k;
            smlar_8_time = smlar_8_time/num;
            bf_time = bf_time/num;
            RAISE NOTICE 'bf_time: %',bf_time;
            RAISE NOTICE 'smlar_8_time: %',smlar_8_time;
            RAISE NOTICE 'recall: %',recall;
            
    END LOOP;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test() RETURNS void as $$  
DECLARE
    pv_num float[] := '{10, 50, 100, 200, 500, 1000, 2000, 3000, 5000, 10000, 15000, 20000}';
    thres  int[] := '{0,1,2,3,4,5,6,7,8,9,10}';
    probes  int[] := '{1,5,10,20,50,100,200}';
    query int[];
    num int;
    k int;
BEGIN
    num = 100;
    k = 10;
    SELECT array_agg((random()*1000000)::int4) from generate_series(1,num) into query;


    FOR I IN 1..array_upper(pv_num, 1) LOOP
        PERFORM set_pv_num(pv_num[I]);
        RAISE NOTICE 'pv_num: %', pv_num[I];
        PERFORM test_knn(query,10);
    END LOOP;

    PERFORM test_smlar4(query,k);
    PERFORM test_smlar8(query,k);

    PERFORM test_vector(query, probes, k);

    PERFORM test_cube(query[1:1], k);

    PERFORM test_bktree(query,thres,10);

END
$$
LANGUAGE plpgsql;

select test();