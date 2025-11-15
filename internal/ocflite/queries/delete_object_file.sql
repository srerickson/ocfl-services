-- delete an object's content file by path
DELETE FROM ocfl_object_files 
WHERE 
object_id IN (
    SELECT o.id FROM ocfl_objects o
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2
) AND path = ?3;
