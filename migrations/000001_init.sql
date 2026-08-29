CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT UNIQUE NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0)
);

INSERT INTO accounts (user_id, balance) 
VALUES (1, 10000), (2, 500)
ON CONFLICT (user_id) DO NOTHING;
