# Minerva Admin Tools

This project provides the command line Minerva administration tools.

# Development

To work on the code in this project, you will most of the time need a running Minerva database. You can start an empty Minerva database using Docker:

```
$ docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 hendrikxitc/minerva
```

Then in another terminal, you can initialize the database with a tiny Minerva instance:

```
$ PGUSER=postgres PGHOST=127.0.0.1 PGDATABASE=minerva MINERVA_INSTANCE_ROOT=examples/tiny_instance_v1 cargo run -- initialize
    Finished dev [unoptimized + debuginfo] target(s) in 0.08s
     Running `target/debug/minerva-admin initialize`
Initializing Minerva instance from examples/tiny_instance_v1
Skipping non-executable file 'examples/tiny_instance_v1/custom/pre-init/README.md'
Added trend store TrendStore(hub-kpi, node, 15m)
Added trend store TrendStore(hub, node, 15m)
Created attribute store 'NotificationStore(trigger-notification)'
Added trend materialization TrendViewMaterialization('hub-kpi_node_main_15m')
Executed sql examples/tiny_instance_v1/custom/post-init/01_report_view.sql
```

To run the web API using the previously started database:

```
$ cargo run --bin minerva-service
```
