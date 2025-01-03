-- attach 'catalogs/imdb/pathce/imdb_1_0_200_bucket/data.db' as bucket;
-- attach 'graphs/imdb/duckdb/imdb.duckdb' as graph;

CREATE temp VIEW v0v2 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_infoEdge_infoIdxVertex
    GROUP BY
        src);

CREATE temp VIEW v0v3 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_movieCompanies_companyName
    GROUP BY
        src);

CREATE temp VIEW v0v2v3 AS (
    SELECT
        e1.id id,
        e1.cnt * e2.cnt cnt
    FROM
        v0v2 e1,
        v0v3 e2
    WHERE
        e1.id = e2.id);

CREATE temp VIEW v0v1v2v3 AS (
    SELECT
        e1.dst id,
        sum(e2.cnt) cnt
    FROM
        graph.title_linkTypeEdge_title e1,
        v0v2v3 e2
    WHERE
        e1.src = e2.id
    GROUP BY
        e1.dst);

CREATE TABLE star_10006 AS (
    SELECT
        b.bucket_id id,
        max(e.cnt) _mode,
        sum(e.cnt) _count
    FROM
        v0v1v2v3 e,
        bucket.bucket_11 b
    WHERE
        e.id = b.id
    GROUP BY
        b.bucket_id
);

