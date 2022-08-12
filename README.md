# HSP

HSP(Hamming simalirity Search in Postgres) is a extension which can support r-neighbor search  and knn(k-nearest neighbor) search in postgres for hamming vecotor efficiently.

## Installation

Compile ans install the extension (tested on Postgres 12.5)

```shell
git clone https://github.com/lss602726449/HSP.git
cd HSP 
make && make install 
cd dependecy/hstore
make && make install
cd ../..
```

Load the extension in postgres

```sql
CREATE EXTENSION IF NOT EXISTS hsp;
CREATE EXTENSION IF NOT EXISTS hstore;
```

## Getting Started

Create a table with 64 dimensions hamming vector (keep the primary key id for index and simalrity search)

```
CREATE TABLE test(id SERIAL PRIMARY KEY, val hmcode(64)));
```

Insert values (hmcode use multiple uint8 separated by comma to represent high-dimensional hamming vectors)

```sql
INSERT INTO test(val) VALUES ('{0,0,0,0,0,0,0,0}'), ('{1,0,0,0,0,0,0,0}'), ('{1,1,0,0,0,0,0,0}');
```

Brute-force search

```sql
SELECT * FROM test WHERE hamming_distance('{0,0,0,0,0,0,0,0}', val)<=1; -- r-neighbor search 
SELECT * FROM test ORDER BY hamming_distance('{0,0,0,0,0,0,0,0}', val) limit 2; -- knn search
```

## Similarity Search

For dataset size larger than 100 thousand, brute-force algorithm may suffer a high time overhead. Serveral algorithm have been proposed to solve the r-neighbor search problem and knn search problem. In this extension, We use the GPH algorithm to better solve the r-neighbor search in postgres, due to its efficiency. GPH algorithm obtains a more tight filting condition and enable flexible threshold allocation. It further uses dynamic programming algorithm to reduce the number of candidate sets. For knn search, multi-index (MIH) algorithm is modified to accelerate the search. For small datasets, it is not recommended to use index and similarity search algorithms.

### Indexing

Multiple hash tables are build as index shared by GPH and MIH. In order to make the dimension of each partition about 23, we set m = 3 for 64 dimension vector, and m=5 for  128. Trigger is also added in the UDF of *create_index* to keep the consistency of vector and index. (There must be at least one vector in the table)

```sql
SELECT set_m(3);
SELECT create_index('test', 'val');
```

### R-neighbor Search

*search_thres* takes query vector, tablename, clomnname, r as input, which searchs in table for all vectors hamming distance not greater than r. The result is exact.

```sql
SELECT * FROM search_thres_('{0,0,0,0,0,0,0,0}', NULL::test, 'val', 2);
```

### KNN Search

*search_knn_mih*  takes query vector, tablename, clomnname, k as input, which searchs in table for k vectors which close to query vector. *set_pv_num* set the num of candidates to be post-verificated, which determines the search quality and search time. The result is not exact, we evaluate the search quality by recall. In our experiment, for sift1M_64 dataset, k= 10,   pv_num = 1000, recall rate of  0.936 can be achieved.

```sql
SELECT set_pv_num(1000);
SELECT *, hamming_distance(val, '{0,0,0,0,0,0,0,0}') AS dis  FROM search_knn_mih('{0,0,0,0,0,0,0,0}', NULL::test, 'val', 10);
```

## Example With SIFT1M_64

load the hamming vector dataset with a table named test_hsp in *data/sift1m.sql*

```shell
createdb test
psql -f data/sift1m.sql -d test
```

Create index and execute search

```sql
SELECT set_m(3);
SELECT create_index('test_hsp', 'val');

SELECT * FROM search_thres_('{0,0,0,0,0,0,0,0}', NULL::test_hsp, 'val', 2);
SELECT set_pv_num(1000);
SELECT *, hamming_distance(val, '{0,0,0,0,0,0,0,0}') AS dis  FROM search_knn_mih('{0,0,0,0,0,0,0,0}', NULL::test_hsp, 'val', 10);
```

## Thanks

Thanks to:

* [GPH: Similarity Search in Hamming Space](https://ieeexplore.ieee.org/document/8509234)
* [Pigeonring: A Principle for FasterThresholded Similarity Search](http://www.vldb.org/pvldb/vol12/p28-qin.pdf)
* [Fast Exact Search in Hamming Spacewith Multi-Index Hashing](https://arxiv.org/pdf/1307.2982.pdf)
* [pgvector](https://github.com/pgvector/pgvector)
