create table test_btree(id integer);
create index on test_btree using btree(id);
create table test_hash(id integer);
create index on test_hash using hash(id);

insert into test_hash(id) select (random()*21473647)::int from generate_series(1,10000000);
insert into test_hash(id) select (random()*21473647)::int from generate_series(1,10000000);

select * from test_hash , (select (random()*2147483647)::int as id  from generate_series(1,10000)) as q where q.id = test_hash.id;
select * from test_btree , (select (random()*2147483647)::int as id  from generate_series(1,10000)) as q where q.id = test_btree.id;

--         10      100     1000    10000                100000
-- hash    0.7     0.952   3.42    24.601(index scan)   2255(hash join)
-- btree   0.712   0.987   3.998   27.153(index scan)   2261(hash join)