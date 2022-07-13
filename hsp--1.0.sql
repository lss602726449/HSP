-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hsp" to load this file. \quit


-- type

CREATE TYPE hmcode;

CREATE FUNCTION hmcode_in(cstring, oid, integer) RETURNS hmcode
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hmcode_out(hmcode) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hmcode_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hmcode_recv(internal, oid, integer) RETURNS hmcode
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hmcode_send(hmcode) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE hmcode (
	INPUT     = hmcode_in,
	OUTPUT    = hmcode_out,
	TYPMOD_IN = hmcode_typmod_in,
	RECEIVE   = hmcode_recv,
	SEND      = hmcode_send
);
-- function
CREATE FUNCTION hmcode_convert(hmcode, integer, boolean) RETURNS hmcode
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION array_to_hmcode(integer[], integer) RETURNS hmcode
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hmcode_to_array(hmcode, integer) RETURNS int[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hamming_distance(hmcode, hmcode) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT ;
-- cast

CREATE CAST (integer[] AS hmcode)
	WITH FUNCTION array_to_hmcode(integer[], integer) AS IMPLICIT;

CREATE CAST (hmcode AS integer[])
	WITH FUNCTION hmcode_to_array(hmcode, integer) AS IMPLICIT;

CREATE CAST (hmcode AS hmcode)
	WITH FUNCTION hmcode_convert(hmcode, integer, boolean) AS IMPLICIT;

-- uint4

CREATE TYPE uint4;

CREATE FUNCTION uint4_in(cstring) RETURNS uint4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION uint4_out(uint4) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

-- passedbyvalue so we can return uint4 not the pointer
CREATE TYPE uint4 (
	INPUT     = uint4_in,
	OUTPUT    = uint4_out,
	PASSEDBYVALUE, 
	INTERNALLENGTH = 4,
	ALIGNMENT = int4
);

-- -- functions

CREATE FUNCTION uint4_hamming(uint4, uint4) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT ;

-- -- hash

CREATE FUNCTION hashuint4(uint4) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT ;

CREATE OR REPLACE FUNCTION uint4_eq(uint4, uint4) RETURNS boolean 
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
	LEFTARG = uint4,
	RIGHTARG = uint4,
	PROCEDURE = uint4_eq,
	COMMUTATOR = =,
	RESTRICT = eqsel, 
	JOIN = eqjoinsel
);

CREATE OPERATOR CLASS uint4_ops
    DEFAULT FOR TYPE uint4 USING hash AS
		OPERATOR	1	= (uint4, uint4),
		FUNCTION	1	hashuint4(uint4);

-- -- cast
CREATE CAST (int4 AS uint4) WITH INOUT AS IMPLICIT;

CREATE CAST (bigint AS uint4) WITH INOUT AS IMPLICIT;

CREATE CAST (uint4 AS int4) WITH INOUT AS ASSIGNMENT;

CREATE CAST (uint4 AS bigint) WITH INOUT AS ASSIGNMENT;

-- gph

CREATE OR REPLACE FUNCTION hmcode_split(hmcode, integer) RETURNS bigint[] 
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_hmcode_dim(hmcode) RETURNS integer 
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_query_cand(uint4, integer, integer) RETURNS bigint[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_statistics_array(bigint[], integer, integer) RETURNS integer[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_slots(integer[], bigint[], integer, integer, integer) RETURNS integer[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_greedy(integer[], bigint[], integer, integer) RETURNS integer[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION set_m(m integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_m() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', m);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_pv_num(pv_num float) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_pv_num() RETURNS float AS '' SELECT %s::float '' LANGUAGE sql IMMUTABLE', pv_num);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_total(total integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_total() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', total);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_histgramrate(histgramrate integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_histgramrate() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', histgramrate);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test() RETURNS void AS $$
DECLARE
res_temp integer[];
arr1 integer[];
arr2 integer[];
BEGIN
-- FOR i IN 1..4 LOOP
-- 	EXECUTE format ('SELECT array_agg(hm_index%s.id) 
-- 	FROM hm_index%s, (SELECT UNNEST(get_query_cand(0,32,1)) as q) as foo 
-- 	WHERE code = q ',i,i)INTO res_temp;
-- END LOOP;
arr1[1] = 1;
arr2[1] = 2;
arr2[2] = 1;
arr1 = ARRAY(DISTINCT UNNEST(array_cat(arr1,arr2)));
FOR I IN array_lower(arr1, 1)..array_upper(arr1, 1) LOOP
	RAISE NOTICE 'the th query = %',arr1[I];
END LOOP;
END
$$
LANGUAGE plpgsql;


--trigger

CREATE OR REPLACE FUNCTION create_trigger(tablename varchar, clomnname varchar) RETURNS void AS $$
BEGIN
RAISE NoTICE 'tigger add';
EXECUTE format('CREATE TRIGGER insert_trigger AFTER INSERT ON %s FOR EACH ROW 
				EXECUTE PROCEDURE _insert_trigger(%s, %s)',tablename,tablename,clomnname);
EXECUTE format('CREATE TRIGGER delete_trigger BEFORE DELETE ON %s FOR EACH ROW 
				EXECUTE PROCEDURE _delete_trigger()',tablename);	
EXECUTE format('CREATE TRIGGER update_trigger_before BEFORE UPDATE ON %s FOR EACH ROW 
				EXECUTE PROCEDURE _update_trigger_before()',tablename);
EXECUTE format('CREATE TRIGGER update_trigger_after AFTER UPDATE ON %s FOR EACH ROW 
				EXECUTE PROCEDURE _update_trigger_after(%s, %s)',tablename,tablename,clomnname);	
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _update_trigger_before() RETURNS TRIGGER AS $$
DECLARE
m integer;
id integer;
BEGIN

SELECT get_m() INTO m;
id =  OLD.id;
-- RAISE NOTICE '%',id;
FOR i IN 1..m LOOP
	EXECUTE format('DELETE FROM hm_index%s where id = $1',i) using id;
END LOOP;
RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _update_trigger_after() RETURNS TRIGGER AS $$
DECLARE
m integer;
tablename varchar;
clomnname varchar;
id integer;
hc hmcode;
arr bigint[];
dim integer;
BEGIN
-- RAISE NOTICE 'after';
SELECT get_m() INTO m;
tablename = TG_ARGV[0];
clomnname = TG_ARGV[1];
id =  NEW.id;
EXECUTE format('SELECT get_hmcode_dim(%s) from %s where id = 1',clomnname,tablename) INTO dim;
-- RAISE NOTICE '%' ,hmcode_split(hmcode_in((hstore(NEW)->clomnname)::cstring, 0, dim), m);
SELECT hmcode_split(hmcode_in((hstore(NEW)->clomnname)::cstring, 0, dim), m) INTO arr;
FOR i IN 1..m LOOP
	EXECUTE format('INSERT INTO hm_index%s values($1, $2)',i) using id, arr[i]::uint4;
END LOOP;
RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _insert_trigger() RETURNS TRIGGER AS $$
DECLARE
m integer;
tablename varchar;
clomnname varchar;
id integer;
hc hmcode;
arr bigint[];
dim integer;
BEGIN

SELECT get_m() INTO m;
tablename = TG_ARGV[0];
clomnname = TG_ARGV[1];
id =  NEW.id;
EXECUTE format('SELECT get_hmcode_dim(%s) from %s where id = 1',clomnname,tablename) INTO dim;
-- RAISE NOTICE '%' ,hmcode_split(hmcode_in((hstore(NEW)->clomnname)::cstring, 0, dim), m);
SELECT hmcode_split(hmcode_in((hstore(NEW)->clomnname)::cstring, 0, dim), m) INTO arr;
FOR i IN 1..m LOOP
	EXECUTE format('INSERT INTO hm_index%s values($1, $2)',i) using id, arr[i]::uint4;
END LOOP;
RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _delete_trigger() RETURNS TRIGGER AS $$
DECLARE
m integer;
id integer;
BEGIN

SELECT get_m() INTO m;
id =  OLD.id;
FOR i IN 1..m LOOP
	EXECUTE format('DELETE FROM hm_index%s where id = $1',i) using id;
END LOOP;
RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_index(tablename varchar, clomnname varchar) RETURNS void AS $$
DECLARE
m integer;
total integer;
b integer;
mplus integer;
dim integer;
histgramrate integer;
samplearray int8[];
hist integer[];
statistics_array integer[];
BEGIN

SELECT get_m() INTO m;
EXECUTE format('SELECT get_hmcode_dim(%s) from %s where id = 1',clomnname,tablename) INTO dim;
SELECT ceil(dim::float4/m) INTO b;
SELECT get_histgramrate() INTO histgramrate;


RAISE NOTICE 'dim = %',dim;
RAISE NOTICE 'm = %',m;
RAISE NOTICE 'b = %',b;

EXECUTE format('CREATE TABLE temp AS SELECT id, hmcode_split(%s,%s) as arr FROM %s', clomnname, m, tablename);
EXECUTE format('CREATE TABLE sampletable AS SELECT * FROM temp WHERE id %% %s = 1',histgramrate);

FOR i IN 1..m LOOP
	EXECUTE format('CREATE TABLE hm_index%s (id int4 REFERENCES %s(id), code uint4)',i,tablename);
	EXECUTE format('INSERT INTO hm_index%s SELECT id,arr[%s]::uint4 from temp', i, i);
	EXECUTE format('CREATE INDEX ON hm_index%s using hash(code)',i);
	SELECT array_agg(arr[i]::int8) FROM sampletable INTO samplearray;
	-- FOR I IN array_lower(samplearray, 1)..array_upper(samplearray, 1) LOOP
	-- 	RAISE NOTICE '%',samplearray[I];
	-- END LOOP;
	SELECT get_statistics_array(samplearray, dim, m) INTO statistics_array;
	hist = array_cat(hist,statistics_array);
END LOOP;
-- RAISE NOTICE '%',array_upper(hist,1);
-- FOR I IN array_lower(hist, 1)..array_upper(hist, 1) LOOP
-- 	RAISE NOTICE '%',hist[I];
-- END LOOP;
CREATE TABLE hist AS SELECT hist;
EXECUTE format('SELECT count(*) FROM %s',tablename) INTO total;
EXECUTE format('SELECT set_total(%s)',total);
-- $1 can make it still string
EXECUTE format('SELECT create_trigger($1,$2)')using tablename,clomnname;
DROP TABLE temp;
DROP TABLE sampletable;
END
$$
LANGUAGE plpgsql;


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
start_t2 timestamp;
end_t2 timestamp;
cand_size bigint;
total integer;
greedy integer[];
slots integer[];
i integer;
BEGIN

start_t = clock_timestamp();
SELECT get_m() INTO m;
SELECT get_hmcode_dim(query) INTO dim; --dimension of the query
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

select get_greedy((SELECT * FROM hist), query_arr, dim, m) INTO greedy;
FOR I IN 1 ..m LOOP
	slots[I] = 0;
END LOOP;

-- RAISE NOTICE 'thres = %',thres;
-- RAISE NOTICE 'm = %',m;
-- RAISE NOTICE 'b = %',b;
	-- RAISE NOTICE 's = %',s;
	-- RAISE NOTICE 'count= %',count;
	-- start_t2 = clock_timestamp(); 
   	FOR j IN 1..array_upper(greedy,1) LOOP
		i = greedy[j];
		s = slots[i];
		slots[i] = slots[i]+1;
		-- RAISE NOTICE '%',i;
		-- RAISE NOTICE '%',s;
		-- RAISE NOTICE '%',array_upper(greedy,1);
		IF i <= mplus THEN
			curb = b;
		ELSE
			curb = b-1;
		END IF; 
		IF s <= curb THEN
			-- start_time = clock_timestamp();
			-- SELECT get_query_cand(query_arr[i],curb,s) INTO query_temp;
			-- end_time = clock_timestamp();
			-- RAISE NOTICE 'time of candidate generation = %',age(end_time, start_time);
			EXECUTE format('SELECT array_agg(hi.id) 
			FROM hm_index%s as hi 
			WHERE hi.code = ANY(get_query_cand($1,$2,$3)::uint4[])',i) INTO res_temp using query_arr[i],curb,s;
			-- end_time = clock_timestamp();
			-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
			--RAISE EXCEPTION 'The enumerate number exceed the number of total number of the databse, please use the brutefoce algorithm'; 
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
		ELSE
			RAISE EXCEPTION 'search radis equal partition length'; 

		END IF;
		EXIT WHEN count >= thres;
   	END LOOP;
	-- end_t2 = clock_timestamp(); 
	-- RAISE NOTICE 'time of search = %',age(end_t2, start_t2);
-- SELECT ARRAY(SELECT DISTINCT UNNEST(res)) INTO res;
end_t = clock_timestamp(); 
-- RAISE NOTICE 'time of search = %',age(end_t, start_t);
-- RAISE NOTICE 'count = %', array_upper(res, 1);
RETURN QUERY EXECUTE format('SELECT *
FROM %s as t
WHERE t.id in (SELECT DISTINCT UNNEST($2)) 
ORDER BY hamming_distance(%s, $1) LIMIT $3 ',pg_typeof(_tbl_type),clomnname)
using query,res,k;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION search_knn_mih(query hmcode, _tbl_type anyelement, clomnname varchar, k int4) RETURNS SETOF anyelement AS $$
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

-- start_t = clock_timestamp(); 
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
SELECT ARRAY(SELECT DISTINCT UNNEST(res)) INTO res;
-- end_t = clock_timestamp(); 
-- RAISE NOTICE 'time of search = %',age(end_t, start_t);
RETURN QUERY EXECUTE format('SELECT *
FROM %s as t
WHERE t.id = ANY($2) 
ORDER BY hamming_distance(%s, $1),id LIMIT $3 ',pg_typeof(_tbl_type),clomnname)
using query,res,k;
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
select get_slots((SELECT * FROM hist), query_arr, dim, m, t) INTO slots;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of get dis = %',age(end_time, start_time);
-- FOR I IN array_lower(slots, 1)..array_upper(slots, 1) LOOP
	-- RAISE NOTICE 'the th % part = %', I , slots[I];
-- END LOOP;
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
				-- start_time = clock_timestamp();
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi 
				WHERE hi.code = ANY(get_query_cand($1,$2,$3)::uint4[])',i) INTO res_temp using query_arr[i],curb,s;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
			ELSE
				RAISE EXCEPTION 'The enumerate number exceed the number of total number of the databse, please use the brutefoce algorithm'; 
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
-- RAISE NOTICE 'the time of candidate generation = %',age(end_t, start_t);
-- RAISE NOTICE 'count = %', array_upper(res, 1);
RETURN QUERY EXECUTE format('SELECT distinct on (id) *
FROM %s as t
WHERE t.id = ANY($2) AND hamming_distance(%s, $1)<=$3',pg_typeof(_tbl_type),clomnname)
using query,res,t;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION search_thres_mih(query hmcode, _tbl_type anyelement, clomnname varchar, t integer) RETURNS SETOF anyelement AS $$
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
-- select get_slots((SELECT * FROM hist), query_arr, dim, m, t) INTO slots;
-- end_time = clock_timestamp();
-- RAISE NOTICE 'time of get dis = %',age(end_time, start_time);
FOR I IN 1 ..m LOOP
	IF I <= t%m  THEN
		slots[I] = t/m+1;
	ELSE
		slots[I] = t/m;
	END IF;
	-- RAISE NOTICE '%', slots[I];
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
				-- start_time = clock_timestamp();
				EXECUTE format('SELECT array_agg(hi.id) 
				FROM hm_index%s as hi 
				WHERE hi.code = ANY(get_query_cand($1,$2,$3)::uint4[])',i) INTO res_temp using query_arr[i],curb,s;
				-- end_time = clock_timestamp();
				-- RAISE NOTICE 'time of get res = %',age(end_time, start_time);
			ELSE
				RAISE EXCEPTION 'The enumerate number exceed the number of total number of the databse, please use the brutefoce algorithm'; 
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
-- RAISE NOTICE 'the time of candidate generation = %',age(end_t, start_t);
-- RAISE NOTICE 'count = %', array_upper(res, 1);
RETURN QUERY EXECUTE format('SELECT distinct on (id) *
FROM %s as t
WHERE t.id = ANY($2) AND hamming_distance(%s, $1)<=$3',pg_typeof(_tbl_type),clomnname)
using query,res,t;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_index(tablename varchar, clomnname varchar ) RETURNS void AS $$
DECLARE
m integer;
BEGIN
EXECUTE 'SELECT get_m()' INTO m;	
FOR I IN 1..m LOOP
	EXECUTE format('DROP TABLE hm_index%s',I);
END LOOP;
EXECUTE format('DROP TRIGGER insert_trigger ON %s;',tablename);
EXECUTE format('DROP TRIGGER delete_trigger ON %s;',tablename);
EXECUTE format('DROP TRIGGER update_trigger_before ON %s;',tablename);
EXECUTE format('DROP TRIGGER update_trigger_after ON %s;',tablename);
DROP TABLE hist;
RAISE NOTICE 'index dropped';
END
$$
LANGUAGE plpgsql;

-- default parameter 
SELECT set_m(3);
SELECT set_pv_num(1000);
SELECT set_histgramrate(100);