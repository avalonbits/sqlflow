-- +goose Up
CREATE TABLE kv (
    key TEXT PRIMARY KEY,
    val TEXT NOT NULL
);

-- +goose Down
DROP TABLE kv;
