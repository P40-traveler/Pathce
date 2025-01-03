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
        dst id,
        count(*) cnt
    FROM
        graph.complCastInfoVertex_complCastInfoEdge_title
    GROUP BY
        dst);

CREATE temp VIEW v0v4 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.title_infoEdge_infoVertex
    GROUP BY
        src);

CREATE temp VIEW v0v1v2v3v4 AS (
    SELECT
        e1.id id,
        e1.cnt * e2.cnt * e3.cnt * e4.cnt cnt
    FROM
        v0v1 e1,
        v0v2 e2,
        v0v3 e3,
        v0v4 e4
    WHERE
        e1.id = e2.id
        AND e1.id = e3.id
        AND e1.id = e4.id);

CREATE temp VIEW v0v1v2v3v4v5 AS (
    SELECT
        e1.src id,
        sum(e2.cnt) cnt
    FROM
        graph.castInfoVertex_castInfoEdge_title e1,
        v0v1v2v3v4 e2
    WHERE
        e1.dst = e2.id
    GROUP BY
        e1.src);

CREATE TABLE star_10004 AS (
    SELECT
        b.bucket_id id,
        max(e.cnt) _mode,
        sum(e.cnt) _count
    FROM
        v0v1v2v3v4v5 e,
        bucket.bucket_2 b
    WHERE
        e.id = b.id
    GROUP BY
        b.bucket_id
);

CREATE temp VIEW v6v7 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.person_akaNameEdge_akaName
    GROUP BY
        src);

CREATE temp VIEW v6v8 AS (
    SELECT
        src id,
        count(*) cnt
    FROM
        graph.person_personInfoEdge_personInfoVertex
    GROUP BY
        src);

CREATE temp VIEW v6v7v8 AS (
    SELECT
        e1.id id,
        e1.cnt * e2.cnt cnt
    FROM
        v6v7 e1,
        v6v8 e2
    WHERE
        e1.id = e2.id);

CREATE temp VIEW v5v6v7v8 AS (
    SELECT
        e1.src id,
        sum(e2.cnt) cnt
    FROM
        graph.castInfoVertex_castInfoEdge_person e1,
        v6v7v8 e2
    WHERE
        e1.dst = e2.id
    GROUP BY
        e1.src);

CREATE TABLE star_10005 AS (
    SELECT
        b.bucket_id id,
        max(e.cnt) _mode,
        sum(e.cnt) _count
    FROM
        v5v6v7v8 e,
        bucket.bucket_2 b
    WHERE
        e.id = b.id
    GROUP BY
        b.bucket_id
);

