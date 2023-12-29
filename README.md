# pg_mcopy
2023년 연말 휴가 때 심심해서 만든 C 프로그램

psql \copy 명령으로 만들어진 테이블 자료 파일을 다중 쓰레드로 copy 명령을 이용해서 다시 테이블로 넣는 프로그램

## build
```shell
cc -Wall -I`pg_config --includedir` -L`pg_config --libdir` -o pg_mcopy pg_mcopy.c -lpq
```

## usage
```shell
./pg_mcopy copy_filename thread_number connection_string table_name
```
