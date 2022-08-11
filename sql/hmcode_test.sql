SET client_min_messages TO WARNING;

CREATE EXTENSION IF NOT EXISTS hsp;
CREATE EXTENSION IF NOT EXISTS hstore;

select '{0,0,0,0,0,0,0,1}'::hmcode;
select get_hmcode_dim('{0,0,0,0,0,0,0,1}'::hmcode);
select ARRAY[1,2,3,4,5,6,7,8]::hmcode;
select get_hmcode_dim(ARRAY[1,2,3,4,5,6,7,8]::hmcode);
select hmcode_split('{0,0,0,0,0,0,0,1}',3);
select hmcode_split('{120,51,156,84,54,111,69,192}',3);
select get_query_cand(0,10,1);
select get_query_cand(2,10,2);