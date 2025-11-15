-- Insert an object file, or update an existing one
-- use size -1 if size is not known.
INSERT INTO ocfl_object_files (
    object_id,
    path,
    digest,
    size
) VALUES (
    (
        SELECT o.id AS id
        FROM ocfl_objects o
        JOIN ocfl_roots r ON r.id = o.root_id 
        WHERE r.name = ?1 AND o.object_id = ?2
    ),
    ?3, -- path
    ?4, -- digest
    ?5 -- size
) ON CONFLICT(object_id, path) DO UPDATE SET
    digest = excluded.digest,
    size = CASE 
        -- set new size if previous size < 0
        WHEN excluded.size < 0 THEN size
        ELSE excluded.size
    END
;