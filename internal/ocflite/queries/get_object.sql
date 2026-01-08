SELECT
    o.storage_path,
    o.padding,
    o.alg,
    o.inventory_digest,
    o.indexed_at,
    COALESCE((SELECT MAX(v.vnum) FROM ocfl_object_versions v WHERE v.object_id = o.id), 0) as head,
    COALESCE((SELECT MIN(v.created_at) FROM ocfl_object_versions v WHERE v.object_id = o.id), 0) as created_at,
    COALESCE((SELECT MAX(v.created_at) FROM ocfl_object_versions v WHERE v.object_id = o.id), 0) as updated_at
FROM ocfl_objects o
JOIN ocfl_roots r ON o.root_id = r.id
WHERE r.name = ?1 AND o.object_id = ?2