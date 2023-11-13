ECONDEXPANSION:

all: sql

sql: crates/minerva/src/schema.sql

crates/minerva/src/schema.sql: schema.yml
	db-schema compile sql -o crates/minerva/src/schema.sql schema.yml
