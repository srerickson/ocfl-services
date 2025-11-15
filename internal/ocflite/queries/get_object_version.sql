SELECT 
    v.message,
    v.created_at,
    v.user_name,
    v.user_address,
    v.state_digest
FROM ocfl_object_versions v
JOIN ocfl_objects o ON v.object_id = o.id
JOIN ocfl_roots r ON o.root_id = r.id
WHERE r.name = ?1 AND o.object_id = ?2 AND v.vnum = ?3;