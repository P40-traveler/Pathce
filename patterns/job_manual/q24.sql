-- attach 'catalogs/imdb/pathce/imdb_1_0_200_bucket/data.db' as bucket;
-- attach 'graphs/imdb/duckdb/imdb.duckdb' as graph;

CREATE temp VIEW v0v2 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_keywordEdge_keyword
    GROUP BY
        src);

CREATE temp VIEW v0v1 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_movieCompanies_companyName
    GROUP BY
        src);

CREATE temp VIEW v0v3 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_infoEdge_infoVertex
    GROUP BY
        src);

CREATE temp VIEW v0v1v2v3 AS (
    SELECT
        e1.id id,
        e1.cnt * e2.cnt * e3.cnt cnt
    FROM
        v0v1 e1,
        v0v2 e2,
        v0v3 e3
    WHERE
        e1.id = e2.id
        AND e1.id = e3.id);

CREATE temp VIEW v0v1v2v3v4 AS (
    SELECT
        e1.src id,
        sum(e2.cnt) cnt
    FROM
        graph.castInfoVertex_castInfoEdge_title e1,
        v0v1v2v3 e2
    WHERE
        e1.dst = e2.id
    GROUP BY
        e1.src);

CREATE TABLE star_10002 AS (
    SELECT
        b.bucket_id id,
        max(e.cnt) _mode,
        sum(e.cnt) _count
    FROM
        v0v1v2v3v4 e,
        bucket.bucket_2 b
    WHERE
        e.id = b.id
    GROUP BY
        b.bucket_id
);

