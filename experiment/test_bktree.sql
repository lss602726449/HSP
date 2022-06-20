CREATE EXTENSION bktree;

CREATE TABLE  test_bktree(id SERIAL PRIMARY KEY, val bigint);

CREATE INDEX bk_index_name ON test_bktree USING spgist (val bktree_ops);

INSERT INTO test_bktree(val) SELECT (random()*(pow(2,64)-1))::bigint from generate_series(1,100);

CREATE EXTENSION hsp;

CREATE EXTENSION hstore;

CREATE TABLE test_hsp(id SERIAL PRIMARY KEY, val hmcode);

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  


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

INSERT INTO test_hsp(val) SELECT gen_uint8_arr(8) FROM generate_series(1,1000000);

SELECT set_m(3);

SELECT create_index('test_hsp', 'val');

-- SELECT * FROM test_bktree WHERE val <@ (0, 10);

CREATE OR REPLACE FUNCTION test(num int, r int) RETURNS void as $$  
DECLARE
start_time timestamp;
end_time timestamp;
bf_time interval;
knn_time interval;
bktree_time interval;
query hmcode;
divide float;
temp bigint;
BEGIN
    bf_time = '0 second';
    knn_time = '0 second';
    bktree_time = '0 second';
    -- RAISE NOTICE 'bf_time: %',bf_time;
    -- RAISE NOTICE 'knn_time: %',knn_time;
    start_time = clock_timestamp(); 
    EXECUTE format('SELECT * from test_hsp where hamming_distance(val, $1) < $2 ')using '{0,0,12,0,37,0,0,1}'::hmcode, r ;
    end_time = clock_timestamp();
    bf_time = bf_time + age(end_time,start_time);
    FOR I IN 1..num LOOP
        EXECUTE format('SELECT val from test_hsp WHERE id = $1 ')using I into query;

        start_time = clock_timestamp();
        EXECUTE format('SELECT * from search_thres($1, NULL::test_hsp, $2, $3)') using query, 'val', r;
        end_time = clock_timestamp();
        knn_time = knn_time + age(end_time,start_time);

        EXECUTE format('SELECT val from test_bktree WHERE id = $1 ')using I into temp;
        -- temp = random()*(pow(2,63)-1);

        start_time = clock_timestamp();
        EXECUTE format('SELECT * FROM test_bktree WHERE val <@ ($1, $2);') using temp,r;
        end_time = clock_timestamp();
        bktree_time = bktree_time + age(end_time,start_time);
    END LOOP;  
    divide = num;
    bf_time = bf_time;
    knn_time = knn_time/divide;
    bktree_time = bktree_time/divide;
    RAISE NOTICE 'bf_time: %',bf_time;
    RAISE NOTICE 'knn_time: %',knn_time;
    RAISE NOTICE 'bktree_time: %',bktree_time;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test() RETURNS void as $$  
DECLARE
    thres  int[] := '{0,1,2,3,4,5,6,7}';
BEGIN
    FOR I IN 1..array_upper(thres, 1) LOOP
        RAISE NOTICE 'r: %', thres[I];
        PERFORM test(100,I);
    END LOOP; 
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION search_thres(query hmcode, _tbl_type anyelement, clomnname varchar, t integer) RETURNS SETOF anyelement AS $$
DECLARE
m integer;
count integer;
total integer;
s integer;
query_arr bigint[];
dim integer;
query_temp bigint[];
mplus integer;
b integer;
curb integer;
res integer[];
res_temp integer[];
start_time timestamp;
end_time timestamp;
start_t timestamp;
end_t timestamp;
cand_size bigint; 
histgram integer[];
slots integer[];
BEGIN
start_t = clock_timestamp(); 
SELECT get_m() INTO m;
SELECT get_total() INTO total;
SELECT get_hmcode_dim(query) INTO dim; --dimension of the query
s = 0; -- serach radis
count = 0; -- search result
cand_size = 1; -- initial by s=0, only itself
SELECT hmcode_split(query,m) INTO query_arr;
SELECT ceil(dim::float4/m) INTO b;
mplus = dim - m * (b-1);
-- RAISE NOTICE 'm = %',m;
-- RAISE NOTICE 'b = %',b;
-- start_time = clock_timestamp();
SELECT * FROM hist into histgram;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of get slots = %',age(end_time, start_time);
-- start_time = clock_timestamp();
select get_slots(histgram, query_arr, dim, m, t) INTO slots;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of get dis = %',age(end_time, start_time);
-- FOR I IN array_lower(slots, 1)..array_upper(slots, 1) LOOP
-- 	RAISE NOTICE 'the th % part = %', I , slots[I];
-- END LOOP;

FOR i IN 1..m LOOP
	IF i <= mplus THEN
		curb = b;
	ELSE
		curb = b-1;
	END IF;
	cand_size = 1;
	FOR s IN 0..slots[i] LOOP
		-- RAISE NOTICE 'the th i = %',i;
		-- RAISE NOTICE 'the th s = %',s;
		IF s <= curb THEN
			IF cand_size<total THEN
				-- start_time = clock_timestamp();
				SELECT get_query_cand(query_arr[i],curb,s) INTO query_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of candidate generation = %',age(end_time, start_time);
				-- FOR I IN array_lower(query_temp, 1)..array_upper(query_temp, 1) LOOP
				-- 	RAISE NOTICE 'the th query = %',query_temp[I];
				-- END LOOP;
				-- start_time = clock_timestamp();
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi, (SELECT UNNEST(get_query_cand($1,$2,$3)) as q) as foo 
				WHERE hi.code = foo.q',i) INTO res_temp using query_arr[i],curb,s;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
				-- FOR I IN array_lower(res_temp, 1)..array_upper(res_temp, 1) LOOP
				-- 	RAISE NOTICE 'the th id = %',res_temp[I];
				-- END LOOP;
			ELSE
				RAISE EXCEPTION 'The enumerate number exceed the number of total number of the databse, please use the brutefoce algorithm'; 
				-- RAISE NOTICE 'SCAN';
				-- start_time = clock_timestamp();
				-- EXECUTE format('SELECT array_agg(hi.id) 
				-- FROM hm_index%s as hi WHERE uint4_hamming(hi.code, $1)= $2',i) using query_arr[i],s INTO res_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
			END IF;
			-- start_time = clock_timestamp();
			res = array_cat(res,res_temp);
			-- end_time = clock_timestamp();
			-- RAISE NOTICE 'time of only concatenate = %',age(end_time, start_time);
			count = coalesce(array_upper(res, 1),0);
			-- RAISE NOTICE 'count= %',count;
		END IF;
		cand_size = (cand_size * (curb-s))/(s+1);
	END LOOP;
END LOOP;
SELECT ARRAY(SELECT DISTINCT UNNEST(res)) INTO res;
end_t = clock_timestamp(); 
-- RAISE NOTICE 'the time of candidate generation = %',age(end_t, start_t);
RAISE NOTICE 'count=%', array_upper(res, 1);
RETURN QUERY EXECUTE format('SELECT t.*
FROM %s as t, (SELECT UNNEST($2) as id) as foo 
WHERE foo.id = t.id AND hamming_distance(%s, $1)<$3',pg_typeof(_tbl_type),clomnname)
using query,res,t;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION search_knn(query hmcode, _tbl_type anyelement, clomnname varchar, k int4) RETURNS SETOF anyelement AS $$
DECLARE
m integer;
count integer;
total integer;
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
BEGIN

-- start_t = clock_timestamp(); 
SELECT get_m() INTO m;
SELECT get_alpha() INTO alpha;
SELECT get_total() INTO total;
SELECT get_hmcode_dim(query) INTO dim; --dimension of the query
s = 0; -- serach radis
count = 0; -- search result
cand_size = 1; -- initial by s=0, only itself
IF k > total THEN
	k = total;
END IF;
SELECT hmcode_split(query,m) INTO query_arr;
SELECT ceil(dim::float4/m) INTO b;
mplus = dim - m * (b-1);
SELECT ceil(total*alpha) INTO thres;

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
				SELECT get_query_cand(query_arr[i],curb,s) INTO query_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of candidate generation = %',age(end_time, start_time);
				-- FOR I IN array_lower(query_temp, 1)..array_upper(query_temp, 1) LOOP
				-- 	RAISE NOTICE 'the th query = %',query_temp[I];
				-- END LOOP;
				-- start_time = clock_timestamp();
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi, (SELECT UNNEST(get_query_cand($1,$2,$3)) as q) as foo 
				WHERE hi.code = foo.q',i) INTO res_temp using query_arr[i],curb,s;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
				-- FOR I IN array_lower(res_temp, 1)..array_upper(res_temp, 1) LOOP
				-- 	RAISE NOTICE 'the th id = %',res_temp[I];
				-- END LOOP;
			ELSE
				RAISE EXCEPTION 'The enumerate number exceed the number of total number of the databse, please use the brutefoce algorithm'; 
				-- RAISE NOTICE 'SCAN';
				-- start_time = clock_timestamp();
				-- EXECUTE format('SELECT array_agg(hi.id) 
				-- FROM hm_index%s as hi WHERE uint4_hamming(hi.code, $1)= $2',i) using query_arr[i],s INTO res_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
			END IF;
			IF count < k THEN 
				-- start_time = clock_timestamp();
				SELECT ARRAY(SELECT DISTINCT UNNEST(array_cat(res,res_temp))) INTO res;--remove duplicate
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of remove duplicate = %',age(end_time, start_time);
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
SELECT ARRAY(SELECT DISTINCT UNNEST(res)) INTO res;
RAISE NOTICE 'r=%', s;
-- end_t = clock_timestamp(); 
-- RAISE NOTICE 'time of search = %',age(end_t, start_t);
RETURN QUERY EXECUTE format('SELECT t.*
FROM %s as t, (SELECT UNNEST($1) as id) as foo WHERE foo.id = t.id ORDER BY hamming_distance(%s, $2) LIMIT $3 ',pg_typeof(_tbl_type),clomnname)
using res,query,k;
END
$$
LANGUAGE plpgsql;