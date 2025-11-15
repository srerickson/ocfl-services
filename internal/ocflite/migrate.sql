-- OCFL roots table: list of roots the server knows about.
CREATE TABLE IF NOT EXISTS ocfl_roots (
    id INTEGER PRIMARY KEY, -- internal database id.
    name TEXT NOT NULL UNIQUE, -- short name for URL routing 
    UNIQUE(name)
);


-- Main object index table
CREATE TABLE IF NOT EXISTS ocfl_objects (
    id INTEGER PRIMARY KEY, -- internal database ID
    root_id INTEGER NOT NULL REFERENCES ocfl_roots(id),
    object_id TEXT NOT NULL, -- ocfl object id
    storage_path TEXT NOT NULL, -- 
    padding INTEGER NOT NULL DEFAULT 0, -- version number padding
    alg TEXT NOT NULL DEFAULT '',
    inventory_digest TEXT NOT NULL DEFAULT '',
    indexed_at INTEGER NOT NULL,
    UNIQUE(root_id, object_id),
    UNIQUE(root_id, storage_path)
);

CREATE INDEX IF NOT EXISTS idx_object_root_id ON ocfl_objects (root_id, object_id);
CREATE INDEX IF NOT EXISTS idx_object_storage_path ON ocfl_objects (root_id, storage_path);

CREATE INDEX IF NOT EXISTS idx_object_indexed_at ON ocfl_objects (indexed_at);
CREATE INDEX IF NOT EXISTS idx_object_inventory_digest ON ocfl_objects (inventory_digest);


-- object versions: rows correspond to object version entries
CREATE TABLE IF NOT EXISTS ocfl_object_versions (
    id INTEGER PRIMARY KEY, -- internal database ID
    object_id INTEGER NOT NULL REFERENCES ocfl_objects(id), -- NOT OCFL ID!
    vnum INTEGER NOT NULL, -- just the version's number (without padding)
    state_digest TEXT NOT NULL, --- sha512 hash of the version state
    created_at INTEGER NOT NULL, -- version created at
    user_name TEXT NOT NULL, -- may be empty
    user_address TEXT NOT NULL, -- may be empty
    message TEXT NOT NULL, -- version's message
    UNIQUE(object_id, vnum)
);

-- OCFL object's content files: rows correspond to manifest entries
CREATE TABLE IF NOT EXISTS ocfl_object_files (
    id INTEGER PRIMARY KEY,
    object_id INTEGER NOT NULL REFERENCES ocfl_objects(id),
    path TEXT NOT NULL, -- content path (relative to object root)
    digest TEXT NOT NULL, -- content digest
    size INTEGER NOT NULL DEFAULT -1, -- size in bytes, -1 if not known
    UNIQUE(object_id, path)
);

-- OCFL object version state
CREATE TABLE IF NOT EXISTS ocfl_object_version_files (
    id INTEGER PRIMARY KEY,
    version_id INTEGER NOT NULL REFERENCES ocfl_object_versions(id),
    path TEXT NOT NULL,
    content_id INTEGER NOT NULL REFERENCES ocfl_object_files(id),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE, -- true if file was deleted in this version
    UNIQUE(version_id, path)
);

-- Indexes for ocfl_object_version_files (critical for query performance)
CREATE INDEX IF NOT EXISTS idx_version_files_version_id
    ON ocfl_object_version_files(version_id);

CREATE INDEX IF NOT EXISTS idx_version_files_path
    ON ocfl_object_version_files(path);

CREATE INDEX IF NOT EXISTS idx_version_files_path_version
    ON ocfl_object_version_files(path, version_id);

CREATE INDEX IF NOT EXISTS idx_version_files_content_id
    ON ocfl_object_version_files(content_id);