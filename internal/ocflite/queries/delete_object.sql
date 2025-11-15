-- delete all content files
DELETE FROM ocfl_object_files 
WHERE object_id IN (
    SELECT o.id FROM ocfl_objects o
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2
);

-- delete all version files
DELETE FROM ocfl_object_version_files
WHERE version_id IN (
    SELECT ov.id
    FROM ocfl_object_versions ov
    JOIN ocfl_objects o ON ov.object_id = o.id
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2
);

-- delete all versions
DELETE FROM ocfl_object_versions
WHERE object_id IN (
    SELECT o.id FROM ocfl_objects o
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2
);

-- delete the object
DELETE FROM ocfl_objects
WHERE id IN (
    SELECT o.id FROM ocfl_objects o
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2
);