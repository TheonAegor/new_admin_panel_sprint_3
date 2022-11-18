full_filmwork_data_sql_template = """
    SELECT
        fw.id,
        fw.title,
        fw.description,
        fw.rating,
        fw.type,
        fw.created,
        fw.modified,
        pfw.role as person_role,
        p.id as person_id,
        p.full_name as person_name,
        g.name as genre_name
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    %(where)s
    %(lim)s;
"""
