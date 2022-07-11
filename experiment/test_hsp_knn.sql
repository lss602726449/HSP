CREATE EXTENSION hsp;
CREATE EXTENSION hstore;

CREATE TABLE test_hsp(id SERIAL PRIMARY KEY, val hmcode);

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  

INSERT INTO test_hsp(val) SELECT gen_uint8_arr(8) FROM generate_series(1,1000000);

SELECT set_m(3);

SELECT create_index('test_hsp', 'val');

SELECT set_alpha(0.001);

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

CREATE OR REPLACE FUNCTION test(num int, k int) RETURNS void as $$  
DECLARE
recall float;
start_time timestamp;
end_time timestamp;
bf_time interval;
knn_time interval;
query hmcode;
temp integer;
divide float;
rand integer;
BEGIN
    recall = 0;
    bf_time = '0 second';
    knn_time = '0 second';
    -- RAISE NOTICE 'bf_time: %',bf_time;
    -- RAISE NOTICE 'knn_time: %',knn_time;
    FOR I IN 1..num LOOP
        EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using I into query;
        
        
        start_time = clock_timestamp(); 
        EXECUTE format('CREATE TABLE gt AS SELECT id, hamming_distance(val, $2) AS dis from test_hsp ORDER BY dis LIMIT $1')using k,query ;
        end_time = clock_timestamp();
        bf_time = bf_time + age(end_time,start_time);
        start_time = clock_timestamp();
        EXECUTE format('CREATE TABLE test AS SELECT temp.* , hamming_distance(val, $2) AS dis  from search_knn($2, NULL::test_hsp, $3, $1) as temp') using k,query,'val';
        end_time = clock_timestamp();
        knn_time = knn_time + age(end_time,start_time);

        SELECT count(*) FROM
        test 
        WHERE test.dis < (SELECT max(dis) from gt) INTO temp;

        recall = recall + temp;
        SELECT  CASE WHEN foo1.count>foo2.count THEN foo2.count
                    ELSE foo1.count
                END
        FROM
        (SELECT count(*) FROM
        test 
        WHERE test.dis = (SELECT max(dis) from gt)) as foo1,
        (SELECT count(*) FROM gt 
        WHERE gt.dis = (SELECT max(dis) from gt)) as foo2 INTO temp;
        recall = recall + temp;
        drop table gt;
        drop table test;
    END LOOP;  
    recall = recall/num/k;
    divide = num;
    bf_time = bf_time/divide;
    knn_time = knn_time/divide;
    RAISE NOTICE 'bf_time: %',bf_time;
    RAISE NOTICE 'knn_time: %',knn_time;
    RAISE NOTICE 'recall: %',recall;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test() RETURNS void as $$  
DECLARE
    pv_num float[] := '{10, 50, 100, 200, 500, 1000, 2000, 5000, 8000, 10000}';
BEGIN
    FOR I IN 1..array_upper(pv_num, 1) LOOP
        PERFORM set_pv_num(pv_num[I]);
        RAISE NOTICE 'pv_num: %', pv_num[I];
        PERFORM test(100,10);
    END LOOP; 
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

CREATE OR REPLACE FUNCTION search_knn(query hmcode, _tbl_type anyelement, clomnname varchar, k int4) RETURNS SETOF anyelement AS $$
DECLARE
m integer;
count integer;
s integer ;
query_arr bigint[];
dim integer;
query_temp bigint[];
mplus integer;
b integer;
curb integer;
res integer[];
res_temp integer[];
thres integer;
alpha float;
start_time timestamp;
end_time timestamp;
start_t timestamp;
end_t timestamp;
cand_size bigint;
total integer; 
BEGIN

start_t = clock_timestamp(); 
SELECT get_m() INTO m;
SELECT get_hmcode_dim(query) INTO dim; --dimension of the query
s = 0; -- serach radis
count = 0; -- search result
cand_size = 1; -- initial by s=0, only itself
SELECT get_total() INTO total;
IF k > total THEN
	k = total;
END IF;
SELECT hmcode_split(query,m) INTO query_arr;
SELECT ceil(dim::float4/m) INTO b;
mplus = dim - m * (b-1);
SELECT get_pv_num() INTO thres;
-- RAISE NOTICE 'thres = %',thres;
-- RAISE NOTICE 'm = %',m;
-- RAISE NOTICE 'b = %',b;
WHILE count < thres LOOP
	-- RAISE NOTICE 's = %',s;
	-- RAISE NOTICE 'count= %',count;
   	FOR i IN 1..m LOOP
		IF i <= mplus THEN
			curb = b;
		ELSE
			curb = b-1;
		END IF; 
		IF s <= curb THEN
			IF cand_size<total THEN
				-- start_time = clock_timestamp();
				-- SELECT get_query_cand(query_arr[i],curb,s) INTO query_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of candidate generation = %',age(end_time, start_time);
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi 
				WHERE hi.code = ANY(get_query_cand($1,$2,$3)::uint4[])',i) INTO res_temp using query_arr[i],curb,s;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
			ELSE
				RAISE EXCEPTION 'The enumerate number exceed the number of total number of the databse, please use the brutefoce algorithm'; 
			END IF;
			IF count < k THEN 
				SELECT ARRAY(SELECT DISTINCT UNNEST(array_cat(res,res_temp))) INTO res;--remove duplicate
			ELSE
				-- start_time = clock_timestamp();
				res = array_cat(res,res_temp);
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of only concatenate = %',age(end_time, start_time);
			END IF;
			count = coalesce(array_upper(res, 1),0);
			-- RAISE NOTICE 'count= %',count;
		END IF;
		EXIT WHEN count >= thres;
   	END LOOP;
	cand_size = (cand_size * (curb-s))/(s+1);
   	s = s + 1; 
END LOOP;
-- SELECT ARRAY(SELECT DISTINCT UNNEST(res)) INTO res;
end_t = clock_timestamp(); 
RAISE NOTICE 'time of search = %',age(end_t, start_t);
RETURN QUERY EXECUTE format('SELECT distinct on (id) *
FROM %s as t
WHERE t.id = ANY($2) 
ORDER BY id, hamming_distance(%s, $1) LIMIT $3 ',pg_typeof(_tbl_type),clomnname)
using query,res,k;
END
$$
LANGUAGE plpgsql;