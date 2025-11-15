SELECT
    ofs.path as content_path,
    ofs.digest as digest,
    ofs.size as size,
    vfs.is_deleted as is_deleted,
    v.created_at as mod_time,
    v.vnum as mod_vnum
FROM ocfl_object_version_files vfs
JOIN ocfl_object_files ofs ON ofs.id = vfs.content_id
JOIN ocfl_object_versions v ON vfs.version_id = v.id
JOIN ocfl_objects o ON v.object_id = o.id
JOIN ocfl_roots r ON o.root_id = r.id
WHERE r.name = ?1 
    AND o.object_id = ?2
    AND v.vnum <= ?3
    AND vfs.path = ?4
ORDER BY mod_vnum DESC LIMIT 1