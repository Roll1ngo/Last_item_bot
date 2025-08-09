-- Створення таблиці (якщо ще не створено через Docker Compose)
CREATE TABLE IF NOT EXISTS offers_parameters (
    id SERIAL PRIMARY KEY,
    offer_id VARCHAR(255) NOT NULL,
    seo_term VARCHAR(255),
    region_id VARCHAR(255), -- Змінено з INT на VARCHAR, оскільки у CSV UUID
    filter_attribute VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE
);

-- Копіювання даних з CSV
COPY offers_parameters(offer_id, seo_term, region_id, filter_attribute, created_at)
FROM '/docker-entrypoint-initdb.d/offers_parameters.csv'
DELIMITER ',' CSV HEADER;