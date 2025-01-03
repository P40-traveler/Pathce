-- attach "../data.db" as bucket;
-- attach "../imdb.duckdb" as graph;

CREATE temp VIEW v0v2 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_infoedge_infovertex
    GROUP BY
        src);

CREATE temp VIEW v0v1 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_moviecompanies_companyname
    GROUP BY
        src);

CREATE temp VIEW v0v1v2 AS (
    SELECT
        e1.id id,
        e1.cnt * e2.cnt cnt
    FROM
        v0v1 e1,
        v0v2 e2
    WHERE
        e1.id = e2.id);

CREATE temp VIEW v0v1v2v3 AS (
    SELECT
        e1.src id,
        sum(e2.cnt) cnt
    FROM
        graph.castinfovertex_castinfoedge_title e1,
        v0v1v2 e2
    WHERE
        e1.dst = e2.id
    GROUP BY
        e1.src);

CREATE TABLE star_10000 AS (
    SELECT
        b.bucket_id id,
        max(e.cnt) _mode,
        sum(e.cnt) _count
    FROM
        v0v1v2v3 e,
        bucket.bucket_2 b
    WHERE
        e.id = b.id
    GROUP BY
        b.bucket_id
);

