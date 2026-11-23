-- Real-Time Collaborative Editor Cassandra Schema
-- Keyspace and tables for document revisions, content snapshots, and metadata

CREATE KEYSPACE IF NOT EXISTS collab_docs
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE collab_docs;

-- Document metadata (title, ownership, timestamps)
CREATE TABLE IF NOT EXISTS document_metadata (
  document_id UUID PRIMARY KEY,
  title TEXT,
  created_by TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  current_revision INT,
  collaborator_count INT
);

-- Ordered revision history of ProseMirror step operations
CREATE TABLE IF NOT EXISTS document_revisions (
  document_id UUID,
  revision INT,
  step_json TEXT,
  client_id TEXT,
  user_id TEXT,
  created_at TIMESTAMP,
  PRIMARY KEY (document_id, revision)
) WITH CLUSTERING ORDER BY (revision ASC);

-- Periodic full-document content snapshots for fast load
CREATE TABLE IF NOT EXISTS document_snapshots (
  document_id UUID,
  revision INT,
  content_json TEXT,
  created_at TIMESTAMP,
  PRIMARY KEY (document_id, revision)
) WITH CLUSTERING ORDER BY (revision DESC);
