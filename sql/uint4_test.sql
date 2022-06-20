CREATE EXTENSION hsp;
CREATE EXTENSION hstore;

select '4294967295'::uint4;
select 4294967295::uint4;
select uint4_hamming(1,4294967295);
select hashuint4(1);
select '4294967295'::uint4 = '4294967295'::uint4;
