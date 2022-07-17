-- INSERT INTO test_bktree(val) SELECT (random()*(pow(2,64)-1))::bigint from generate_series(1,100);

CREATE EXTENSION hsp;

CREATE EXTENSION hstore;

CREATE TABLE test_hsp(id SERIAL PRIMARY KEY, val hmcode);

CREATE OR REPLACE FUNCTION gen_uint8_arr(int) RETURNS int[] as $$  
    SELECT array_agg((random()*255)::int4) from generate_series(1,$1);  
$$ LANGUAGE SQL STRICT ;  

INSERT INTO test_hsp(val) SELECT gen_uint8_arr(8) FROM generate_series(1,10000000);

SELECT set_m(3);

SELECT create_index('test_hsp', 'val');


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


SELECT * FROM test_bktree WHERE val <@ (0, 10);

SELECT * FROM test_bktree WHERE val <@ (6814781905043772971, 4);

SELECT * from search_thres('{12,47,195,146,74,59,9,97}', NULL::test_hsp, 'val',10);

SELECT * from test_hsp where hamming_distance('{12,47,195,146,74,59,9,97}', val)<=10;


SELECT *
FROM test_hsp as t
WHERE t.id in (SELECT (random()*100000)::int from generate_series(1,1000));

CREATE OR REPLACE FUNCTION test(num int, r int) RETURNS void as $$  
DECLARE
start_time timestamp;
end_time timestamp;
bf_time interval;
thres_time interval;
mih_time interval;
bktree_time interval;
query hmcode;
divide float;
temp bigint;
rand integer;
BEGIN
    bf_time = '0 second';
    thres_time = '0 second';
    bktree_time = '0 second';
	mih_time = '0 second';
    -- RAISE NOTICE 'bf_time: %',bf_time;
    -- RAISE NOTICE 'thres_time: %',thres_time;
    start_time = clock_timestamp(); 
    EXECUTE format('SELECT * from test_hsp where hamming_distance(val, $1) < $2 ')using '{93,67,144,178,39,208,103,247}'::hmcode, r ;
    end_time = clock_timestamp();
    bf_time = bf_time + age(end_time,start_time);
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
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test() RETURNS void as $$  
DECLARE
    thres  int[] := '{0,1,2,3,4,5,6,7,8}';
BEGIN
    FOR I IN 1..array_upper(thres, 1) LOOP
        RAISE NOTICE 'r: %', thres[I];
        PERFORM test(1000,I);
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
-- SELECT * FROM hist into histgram;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of get slots = %',age(end_time, start_time);
-- start_time = clock_timestamp();
select get_slots((SELECT * FROM hist), query_arr, dim, m, t) INTO slots;
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
				-- SELECT get_query_cand(query_arr[i],curb,s) INTO query_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of candidate generation = %',age(end_time, start_time);
				-- FOR I IN array_lower(query_temp, 1)..array_upper(query_temp, 1) LOOP
				-- 	RAISE NOTICE 'the th query = %',query_temp[I];
				-- END LOOP;
				-- start_time = clock_timestamp();
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi 
				WHERE hi.code = ANY(get_query_cand($1,$2,$3)::uint4[])',i) INTO res_temp using query_arr[i],curb,s;
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
-- end_t = clock_timestamp(); 
-- RAISE NOTICE 'the time of candidate generation = %',age(end_t, start_t);
-- RAISE NOTICE 'count = %', array_upper(res, 1);
RETURN QUERY EXECUTE format('SELECT *
FROM %s as t
WHERE t.id = ANY($2) AND hamming_distance(%s, $1)<=$3',pg_typeof(_tbl_type),clomnname)
using query,res,t;
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
-- SELECT * FROM hist into histgram;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of get slots = %',age(end_time, start_time);
start_time = clock_timestamp();
select get_slots((SELECT * FROM hist), query_arr, dim, m, t) INTO slots;
end_time = clock_timestamp();
RAISE NOTICE 'time of get dis = %',age(end_time, start_time);
FOR I IN array_lower(slots, 1)..array_upper(slots, 1) LOOP
	RAISE NOTICE 'the th % part = %', I , slots[I];
END LOOP;
FOR i IN 1..m LOOP
	IF slots[i] = -1 THEN
		CONTINUE;
	END IF;
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
				-- SELECT get_query_cand(query_arr[i],curb,s) INTO query_temp;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of candidate generation = %',age(end_time, start_time);
				-- FOR I IN array_lower(query_temp, 1)..array_upper(query_temp, 1) LOOP
				-- 	RAISE NOTICE 'the th query = %',query_temp[I];
				-- END LOOP;
				-- start_time = clock_timestamp();
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi 
				WHERE hi.code = ANY(get_query_cand($1,$2,$3)::uint4[])',i) INTO res_temp using query_arr[i],curb,s;
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
-- start_time = clock_timestamp();
-- SELECT ARRAY(SELECT DISTINCT UNNEST(res)) INTO res;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of only remove duplicate = %',age(end_time, start_time);
end_t = clock_timestamp(); 
RAISE NOTICE 'the time of candidate generation = %',age(end_t, start_t);
RAISE NOTICE 'count = %', array_upper(res, 1);
RETURN QUERY EXECUTE format('SELECT distinct on (id) *
FROM %s as t
WHERE t.id = ANY($2) AND hamming_distance(%s, $1)<=$3',pg_typeof(_tbl_type),clomnname)
using query,res,t;
END
$$
LANGUAGE plpgsql;

 




CREATE OR REPLACE FUNCTION test_array() RETURNS void as $$  
DECLARE
temp int[];
m int;
BEGIN
	m=3;
	RAISE NOTICE '%', array_length(temp,1);
	FOR I IN 1 ..m LOOP
		temp[I] = 1;
	END LOOP;
	RAISE NOTICE '%', array_length(temp,1);
	FOR I IN 1 ..m LOOP
		RAISE NOTICE '%', temp[I];
	END LOOP;
END
$$
LANGUAGE plpgsql;