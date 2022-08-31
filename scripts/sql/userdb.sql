CREATE DATABASE ghtorrent;
CREATE USER ghtorrent WITH PASSWORD 'ghtorrent';
GRANT ALL PRIVILEGES ON DATABASE "ghtorrent" to ghtorrent;
ALTER USER ghtorrent WITH SUPERUSER;
