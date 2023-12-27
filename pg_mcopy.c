#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <libpq-fe.h>
#include <time.h>

#define COPY_QUERY_FORMAT "COPY %s FROM STDIN"

#define CHUNK_SIZE 1024 * 1024 // 1MB

#define BILLION 1000000000L

typedef struct {
  const char * filename;
  const char * db_connection;
  const char * table_name;
  int thread_number;
  off_t start_offset;
  off_t read_size;
  unsigned long long inserted_rows;
}
ThreadResult;

// 줄의 시작 위치로 조정하는 함수
off_t adjustToLineStart(int fd, off_t start_offset) {
  off_t current_offset = start_offset;
  char ch;

  // 현재 위치에서 앞으로 이동하여 줄의 시작 찾기
  while (pread(fd, & ch, 1, current_offset) > 0 && ch != '\n') {
    current_offset--;
  }

  // 줄의 시작 위치 반환
  return current_offset;
}

void * readFile(void * arg) {
  ThreadResult * args = (ThreadResult * ) arg;
  ssize_t read_bytes;

  // 파일 열기
  int fd = open(args -> filename, O_RDONLY);
  if (fd == -1) {
    perror("Error opening file");
    pthread_exit(NULL);
  }

  // PostgreSQL C API 초기화
  PGconn * conn = PQconnectdb(args -> db_connection);
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
    close(fd);
    PQfinish(conn);
    pthread_exit(NULL);
  }

  // COPY 쿼리 생성
  char copy_query[256];
  snprintf(copy_query, sizeof(copy_query), COPY_QUERY_FORMAT, args -> table_name);

  // COPY 쿼리 실행
  PGresult * result = PQexec(conn, copy_query);
  if (PQresultStatus(result) != PGRES_COPY_IN) {
    fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
    PQclear(result);
    close(fd);
    PQfinish(conn);
    pthread_exit(NULL);
  }

  // 메모리 할당
  char * buffer = malloc(CHUNK_SIZE);
  if (buffer == NULL) {
    perror("Error allocating buffer");
    close(fd);
    PQclear(result);
    PQfinish(conn);
    pthread_exit(NULL);
  }

  off_t current_offset = args -> start_offset;

  while (args -> start_offset + args -> read_size > current_offset) {
    if ((args -> start_offset + args -> read_size) > (current_offset + CHUNK_SIZE))
      read_bytes = pread(fd, buffer, CHUNK_SIZE, current_offset);
    else
      read_bytes = pread(fd, buffer, 1 + (args -> start_offset + args -> read_size) - current_offset, current_offset);

    if (PQputCopyData(conn, buffer, read_bytes) != 1) {
      fprintf(stderr, "Error sending data to PostgreSQL: %s", PQerrorMessage(conn));
      PQclear(result);
      free(buffer);
      close(fd);
      PQfinish(conn);
      pthread_exit(NULL);
    }

    current_offset += read_bytes;
  }

  PQputCopyEnd(conn, NULL);

  char * endptr;

  // waiting finish
  while ((result = PQgetResult(conn)) != NULL) {
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
      printf("thread: %d, inserted rows: %s\n", args -> thread_number, PQcmdTuples(result));
      args -> inserted_rows = strtoull(PQcmdTuples(result), & endptr, 10);
    } else {
      fprintf(stderr, "thread: %d, COPY fail: %s", args -> thread_number, PQerrorMessage(conn));
    }
    PQclear(result);
  }

  free(buffer);
  close(fd);

  pthread_exit(NULL);
}

int main(int argc, char * argv[]) {
  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC, & start);
  unsigned long long total_inserted_rows;
  total_inserted_rows = 0;

  if (argc != 5) {
    fprintf(stderr, "Usage: %s <filename> <sections> <db_connection> <table_name>\n", argv[0]);
    return 1;
  }

  const char * filename = argv[1];
  int sections = atoi(argv[2]);
  const char * db_connection = argv[3];
  const char * table_name = argv[4];

  if (sections <= 0) {
    fprintf(stderr, "Sections must be a positive integer\n");
    return 1;
  }

  struct stat st;

  // 파일 정보 가져오기
  if (stat(filename, & st) == 0) {
    // st.st_size에 파일 크기가 저장되어 있음
    off_t fileSize = st.st_size;

    // 등분
    off_t sectionSize = fileSize / sections;

    // 스레드 변수
    pthread_t threads[sections];
    ThreadResult args[sections];

    int fd = open(filename, O_RDONLY);
    for (int i = 0; i < sections; ++i) {
      args[i].filename = filename;
      args[i].db_connection = db_connection;
      args[i].table_name = table_name;
      args[i].thread_number = i;
      args[i].inserted_rows = 0;

      if (i == 0) args[i].start_offset = 0;
      else args[i].start_offset = args[i - 1].start_offset + args[i - 1].read_size + 1;
      if (i == sections - 1) {
        args[i].read_size = fileSize - args[i].start_offset;
      } else {
        off_t current_offset = adjustToLineStart(fd, args[i].start_offset + sectionSize);
        args[i].read_size = current_offset - args[i].start_offset;
      }
    }
    close(fd);

    // 각 부분의 시작 위치와 크기를 스레드에 전달하면서 스레드 생성
    for (int i = 0; i < sections; ++i) {
      if (pthread_create( & threads[i], NULL, readFile, (void * ) & args[i]) != 0) {
        fprintf(stderr, "Error creating thread %d\n", i);
        return 1;
      }
    }

    for (int i = 0; i < sections; ++i) {
      if (pthread_join(threads[i], NULL) != 0) {
        fprintf(stderr, "Error joining thread %d\n", i);
        return 1;
      }
      total_inserted_rows += args[i].inserted_rows;
    }

    printf("total inserted rows: %llu\n", total_inserted_rows);

  } else {
    // stat 함수 실패
    perror("Error getting file size");
    return 1;
  }

  clock_gettime(CLOCK_MONOTONIC, & end);
  long elapsed_time = BILLION * (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec);
  double elapsed_milliseconds = (double) elapsed_time / 1000000.0;
  printf("total execution time: %f ms\n", elapsed_milliseconds);

  return 0;
}
