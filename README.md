# Minerva Admin Tools

This project provides the command line Minerva administration tools.

# Development

To work on the code in this project, you will most of the time need a running Minerva database. You can start an empty Minerva database using Docker:

```
$ docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 hendrikxitc/minerva
```