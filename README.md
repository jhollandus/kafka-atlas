# Kafka Integration with Apache Atlas

## Demo Run

Follow these instructions to run demo of the integration.

### Install postgres

If on OSX then use brew to install postgres

`brew install postgres`

Initialize the db, needs to be done just once

`initdb /usr/local/var/db/postgres`

Then start it up by issuing

`postgres -D /usr/local/var/db/postgres`

This will start in the foreground otherwise look into the `pg_ctl` command to start it as a daemon.

Create a new database called `kafka-atlas` by issuing:

`createdb kakfa-atlas`

Create the schema by issuing these commands from the `psql` prompt:

```
CREATE TABLE accounts(id SERIAL PRIMARY KEY, name VARCHAR(255));
INSERT INTO accounts(name) VALUES('bob');
INSERT INTO accounts(name) VALUES('alice');
```

### Connector

`confluent config kafka-atlas-postgres -d ./conf/kafka-connect.properties`
