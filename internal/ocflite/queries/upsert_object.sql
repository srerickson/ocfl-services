INSERT INTO ocfl_objects (
    root_id,
    object_id,
    storage_path,
    padding,
    alg,
    inventory_digest,
    indexed_at
) VALUES (
    (SELECT id FROM ocfl_roots WHERE name = ?1), 
    ?2, ?3, ?4, ?5, ?6, ?7
) ON CONFLICT (root_id, object_id) DO UPDATE SET
    storage_path = excluded.storage_path,
    padding = excluded.padding,
    alg = excluded.alg,
    inventory_digest = excluded.inventory_digest,
    indexed_at = excluded.indexed_at;