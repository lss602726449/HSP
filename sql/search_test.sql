SET client_min_messages TO WARNING;

CREATE EXTENSION IF NOT EXISTS hsp;
CREATE EXTENSION IF NOT EXISTS hstore;


CREATE TABLE test_hsp(id SERIAL PRIMARY KEY, val hmcode(64));

INSERT INTO test_hsp(val) VALUES ('{0,0,0,0,0,0,0,0}'),('{0,0,0,0,1,0,0,0}'),('{0,0,0,0,1,1,0,0}'),('{0,1,0,2,0,3,0,0}');

SELECT set_m(3);

SELECT create_index('test_hsp', 'val');

SELECT get_total();

SELECT * from test_hsp where hamming_distance('{0,0,0,0,0,0,0,0}', val) <= 1;

SELECT * from test_hsp order by hamming_distance('{0,0,0,0,0,0,0,0}', val) limit 2;