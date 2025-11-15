INSERT INTO ocfl_object_version_files (
    version_id,
    content_id,
    path,
    is_deleted
) VALUES (
    (
        SELECT v.id AS id
        FROM ocfl_object_versions v
        JOIN ocfl_objects o on v.object_id = o.id 
        JOIN ocfl_roots r ON o.root_id = r.id
        WHERE r.name = ?1 AND o.object_id = ?2 and v.vnum = ?3
    ),
    (
        SELECT f.id as id
        FROM ocfl_object_files f
        JOIN ocfl_objects o on f.object_id = o.id
        JOIN ocfl_roots r ON o.root_id = r.id
        WHERE r.name = ?1 AND o.object_id = ?2 AND f.digest = ?4
    ),
    ?5,
    ?6
);