# pg_mcopy
parallel copy for postgresql

A program that returns a table data file created with the psql \copy command to a table using the copy command with multiple threads.

## build
```shell
cc -Wall -I`pg_config --includedir` -L`pg_config --libdir` -o pg_mcopy pg_mcopy.c -lpq
```

## usage
```shell
./pg_mcopy copy_filename thread_number connection_string table_name
```
