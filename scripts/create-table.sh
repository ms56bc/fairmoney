-- init_script.cql
CREATE KEYSPACE IF NOT EXISTS my_keyspace
WITH replication = {
   'class': 'SimpleStrategy',
   'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS my_keyspace.transaction_count (
   user_id int PRIMARY KEY,
   count bigint,
   created_at bigint
);
