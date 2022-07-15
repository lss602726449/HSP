CREATE EXTENSION hsp;
CREATE EXTENSION hstore;

CREATE TABLE test_hsp(id SERIAL PRIMARY KEY, val hmcode);

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  

INSERT INTO test_hsp(val) SELECT gen_uint8_arr(8) FROM generate_series(1,1000000);

SELECT set_m(3);

SELECT create_index('test_hsp', 'val');

SELECT set_pv_num(1000);

-- test for correctness

-- SELECT foo.id,foo.arr[2] from (SELECT id, hmcode_split(val,8) as arr FROM test_gph)as foo;

-- SELECT hmcode_split(ARRAY[1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0],8);

-- SELECT UNNEST(get_query_cand(513,8,0));

-- SELECT UNNEST(get_query_cand(513,8,1));

-- SELECT hi.id 
-- FROM hm_index1 as hi, (SELECT UNNEST(get_query_cand(513,8,0))as q) as foo 
-- WHERE hi.code = foo.q;

-- SELECT hi.id 
-- FROM hm_index1 as hi, (SELECT UNNEST(get_query_cand(513,8,1))as q) as foo 
-- WHERE hi.code = foo.q;

-- EXPLAIN ANALYZE SELECT hi.id 
-- FROM hm_index1 as hi, (SELECT UNNEST(get_query_cand(513,8,1))as q) as foo 
-- WHERE hi.code = foo.q;

SELECT gen_uint8_arr(8)::hmcode as q into query;

SELECT * FROM query;

\timing ON 

CREATE TABLE gt AS SELECT id, hamming_distance(val, query.q) AS dis from test_hsp , query ORDER BY dis LIMIT 10 ;

CREATE TABLE test AS SELECT temp.* , hamming_distance(val, query.q) AS dis  from query, search_knn(query.q, NULL::test_hsp, 'val', 10) as temp ;

SELECT * from (SELECT gen_uint8_arr(8)::hmcode as q) as query, search_knn(query.q, NULL::test_hsp, 'val', 10); 

SELECT * from test_hsp, (SELECT gen_uint8_arr(8)::hmcode as q) query ORDER BY hamming_distance(val, query.q) LIMIT 10;

SELECT id, hamming_distance(val, '{12,47,195,146,74,59,9,97}') AS dis from test_hsp ORDER BY dis LIMIT 10 ;

SELECT *, hamming_distance(val, '{12,47,195,146,74,59,9,97}') AS dis  from search_knn('{12,47,195,146,74,59,9,97}', NULL::test_hsp, 'val', 10) ORDER BY dis; 

SELECT *, hamming_distance(val, '{12,47,195,146,74,59,9,97}') AS dis  from search_knn_mih('{12,47,195,146,74,59,9,97}', NULL::test_hsp, 'val', 10) ORDER BY dis; 


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
        RAISE NOTICE 'recall: %',recall;
        RAISE NOTICE 'bktree_time: %',bktree_time;
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
        RAISE NOTICE 'recall: %',recall;
        RAISE NOTICE 'cube_time: %',cube_time;
        RAISE NOTICE 'bf_time: %',bf_time;
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
            RAISE NOTICE 'recall: %',recall;
            RAISE NOTICE 'vector_time: %',vector_time;
            RAISE NOTICE 'bf_time: %',bf_time;
    END LOOP;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test() RETURNS void as $$  
DECLARE
    pv_num float[] := '{10, 50, 100, 200, 500, 1000, 2000, 3000, 5000}';
    thres  int[] := '{0,1,2,3,4,5,6,7,8,9,10}';
    probes  int[] := '{1,5,10,20,50,100,200}';
    query int[];
    num int;
    k int;
BEGIN
    num = 500;
    k = 10;
    SELECT array_agg((random()*1000000)::int4) from generate_series(1,num) into query;

    FOR I IN 1..array_upper(pv_num, 1) LOOP
        PERFORM set_pv_num(pv_num[I]);
        RAISE NOTICE 'pv_num: %', pv_num[I];
        PERFORM test_knn(query,10);
    END LOOP;

    PERFORM test_vector(query, probes, k);

    PERFORM test_cube(query, k);

    PERFORM test_bktree(query,thres,10);

END
$$
LANGUAGE plpgsql;

-- \timing OFF

SELECT count(*) FROM
test 
WHERE test.dis < (SELECT max(dis) from gt);

SELECT CASE WHEN foo1.count>foo2.count THEN foo2.count
            ELSE foo1.count
       END
FROM
(SELECT count(*) FROM
test 
WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
(SELECT count(*) FROM gt 
WHERE gt.dis = (SELECT max(dis) from gt)) as foo2;


-- SELECT drop_index('test_gph', 'val');

-- DROP TABLE test_gph;

-- DROP TABLE query;

-- DROP TABLE test;

-- DROP TABLE gt;

-- DROP EXTENSION gph CASCADE; 
