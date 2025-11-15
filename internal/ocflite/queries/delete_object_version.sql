DELETE FROM ocfl_object_versions
WHERE id IN (
    SELECT ov.id
    FROM ocfl_object_versions ov
    JOIN ocfl_objects o ON ov.object_id = o.id
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2 AND ov.vnum = ?3
);