-- Synthetic data for the isolated local Compose demonstration.
CREATE DATABASE catalog;
CREATE DATABASE analytics;

\connect catalog

CREATE TABLE authors (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE books (
    id bigint PRIMARY KEY,
    author_id bigint NOT NULL REFERENCES authors(id),
    title text NOT NULL,
    deleted boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX books_author_id_idx ON books (author_id);
CREATE INDEX books_updated_at_idx ON books (updated_at, id);
CREATE INDEX authors_updated_at_idx ON authors (updated_at, id);

INSERT INTO authors (id, name) VALUES (1, 'Demo Author');
INSERT INTO books (id, author_id, title) VALUES
    (1, 1, 'A Guide to Local Synchronization'),
    (2, 1, 'Building Search Documents');

\connect analytics

CREATE TABLE book_metrics (
    book_id bigint PRIMARY KEY,
    views bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX book_metrics_updated_at_idx ON book_metrics (updated_at, book_id);
INSERT INTO book_metrics (book_id, views) VALUES (1, 10), (2, 20);
