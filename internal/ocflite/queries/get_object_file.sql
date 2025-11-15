SELECT
    f.digest as digest,
    f.path as path,
    f.size as size
FROM ocfl_object_files f
JOIN ocfl_objects o ON f.object_id = o.id
JOIN ocfl_roots r ON o.root_id = r.id
WHERE r.name = ?1 
    AND o.object_id = ?2
    AND f.path = ?3;