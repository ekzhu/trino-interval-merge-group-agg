# `merge_group`

User-Defined Aggregate plugin for merging overlapping intervals in Trino.

Usage Examples:

Merge integer intervals without `PARTITION BY`:

```sql
WITH input AS (
    VALUES
    (cast(1 AS bigint), cast(4 AS bigint)),
    (2, 3),
    (3, 5),
    (2, 4),
    (4, 6),
    (8, 19)
)
SELECT min(x) AS x, max(y) AS y
FROM (
    SELECT x, y, merge_group(x, y) OVER (ORDER BY x) AS group_id
    FROM input AS i(x, y)
) AS t(x, y, group_id)
GROUP BY group_id;

-- Results:
-- x, y
-- 1, 6
-- 8, 19
```

Merge date intervals with `PARTITION BY`:

```sql
WITH input AS (
    VALUES
    ('a', date '2012-01-01', date '2012-01-02'),
    ('a', date '2012-01-02', date '2012-01-03'),
    ('b', date '2012-01-03', date '2012-01-05'),
    ('c', date '2012-01-02', date '2012-01-04'),
    ('b', date '2012-01-04', date '2012-01-06'),
    ('a', date '2012-01-05', date '2012-01-07')
)
SELECT pid, min(x) as x, min(y) as y
FROM (
    SELECT pid, x, y, merge_group(x, y) OVER (PARTITION BY pid ORDER BY x) AS group_id
    FROM input AS (pid, x, y)
) AS t(pid, x, y, group_id)
GROUP BY pid, group_id;

-- Results:
-- pid, x, y
-- 'a', date '2012-01-01', date '2012-01-03'
-- 'a', date '2012-01-05', date '2012-01-07'
-- 'b', date '2012-01-03', date '2012-01-06'
-- 'c', date '2012-01-02', date '2012-01-04'
```
