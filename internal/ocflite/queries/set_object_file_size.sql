UPDATE ocfl_object_files 
SET size = ?4
WHERE object_id = (
    SELECT o.id 
    FROM ocfl_objects o
    JOIN ocfl_roots r ON r.id = o.root_id 
    WHERE r.name = ?1 AND o.object_id = ?2
) AND digest = ?3;