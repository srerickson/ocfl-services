-- This query returns all unique file paths across all versions of an
-- object up to and including a given version number. For each file path,
-- returns information from its most recent modification:
--
-- Returns:
-- - path: logical file path in the version state
-- - digest: content digest (sha256 or sha512)
-- - content_path: physical content path relative to object root
-- - size: file size in bytes (-1 if unknown)
-- - mod_vnum: version number where file was last modified
-- - mod_time: timestamp when that version was created
-- - is_deleted: true if the most recent modification was a deletion
--
-- Rows are ordered by path, then by deletion state (existing files
-- first).
--
-- Arguments:
-- 1: root name
-- 2: object id
-- 3: max version number
-- 4: path or path prefix constraint (use "" or "." for all files)
WITH target_object AS (
    -- Resolve root name + object_id to internal database ID once
    SELECT o.id
    FROM ocfl_objects o
    JOIN ocfl_roots r ON o.root_id = r.id
    WHERE r.name = ?1 AND o.object_id = ?2
),
latest_file AS (
    -- Find the most recent version number for each file path
    SELECT f.path, MAX(v.vnum) as max_vnum
    FROM ocfl_object_version_files f
    JOIN ocfl_object_versions v ON f.version_id = v.id
    JOIN target_object t ON v.object_id = t.id
    WHERE v.vnum <= ?3
        -- Apply path filter early to reduce rows processed
        AND ((?4 = '' OR ?4 = '.') OR f.path LIKE ?4 || '/%')
    GROUP BY path
)
SELECT
    vf.path,
    ofs.digest,
    ofs.path as content_path,
    ofs.size,
    v.vnum as mod_vnum,
    v.created_at as mod_time,
    vf.is_deleted
FROM ocfl_object_version_files vf
JOIN ocfl_object_versions v ON vf.version_id = v.id
JOIN target_object t ON v.object_id = t.id
JOIN latest_file lf ON vf.path = lf.path AND v.vnum = lf.max_vnum
JOIN ocfl_object_files ofs ON vf.content_id = ofs.id
ORDER BY vf.path, vf.is_deleted;
