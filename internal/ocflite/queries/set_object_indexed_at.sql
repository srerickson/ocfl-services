UPDATE ocfl_objects
SET indexed_at = ?3
WHERE id = (
    SELECT o.id
    FROM ocfl_objects o
    JOIN ocfl_roots r ON r.id = o.root_id
    WHERE r.name = ?1 AND o.object_id = ?2
);
