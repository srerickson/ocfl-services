WITH versions AS (
    SELECT 
        v.object_id as object_id,
        MAX(v.vnum) as head,
        MIN(v.created_at) as created_at,
        MAX(v.created_at) as updated_at
    FROM ocfl_object_versions v
    GROUP BY v.object_id
)
SELECT
    o.object_id,
    o.storage_path,
    o.padding,
    o.alg,
    o.inventory_digest,
    o.indexed_at,
    COALESCE(v.head, 0) as head,
    COALESCE(v.created_at, 0) as created_at,
    COALESCE(v.updated_at, 0) as updated_at
FROM ocfl_objects o
JOIN ocfl_roots r ON o.root_id = r.id
LEFT JOIN versions v ON v.object_id = o.id
WHERE r.name = ?1
ORDER BY o.object_id
LIMIT ?2 OFFSET ?3