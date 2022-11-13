CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
); 
CREATE UNIQUE INDEX film_work_id_idx ON content.film_work (id);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);
CREATE UNIQUE INDEX genre_id_idx ON content.genre (id);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid,
    film_work_id uuid,
    created timestamp with time zone,
    FOREIGN KEY (genre_id) REFERENCES content.genre(id) ON DELETE CASCADE,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX genre_film_work_id_idx ON content.genre_film_work (genre_id, film_work_id);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name varchar(250),
    created timestamp with time zone,
    modified timestamp with time zone
);
CREATE UNIQUE INDEX content_id_idx ON content.person (id);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid references content.person(id) ON DELETE CASCADE,
    film_work_id uuid references content.film_work(id) ON DELETE CASCADE,
    role varchar(250),
    created timestamp with time zone
);
CREATE UNIQUE INDEX person_film_work_id_idx ON content.person_film_work (person_id,film_work_id,role);