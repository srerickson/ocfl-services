-- Insert an object version, or update an existing one
INSERT INTO ocfl_object_versions (
    object_id,
    vnum,
    state_digest,
    created_at,
    user_name,
    user_address,
    message
) VALUES (
    (   
        SELECT o.id AS id
        FROM ocfl_objects o
        JOIN ocfl_roots r ON r.id = o.root_id 
        WHERE r.name = ?1 AND o.object_id = ?2
    ),
    ?3, -- vnum (just num)
    ?4, -- state digest
    ?5, -- created_at
    ?6, -- user name
    ?7, -- user address
    ?8  -- message
) ON CONFLICT(object_id, vnum) DO UPDATE SET
    state_digest = excluded.state_digest,
    created_at = excluded.created_at,
    user_name = excluded.user_name,
    user_address = excluded.user_address,
    message = excluded.message;
