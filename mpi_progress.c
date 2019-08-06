#define _POSIX_C_SOURCE 199309L

#include <mpi.h>
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <math.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>

// 100k repetitions
#define NUM_REPS 100000UL

// Large transfer: 16GB
#define LARGE_SIZE (16*1024*1024)
#define NUM_REPS_LARGE 1000

#define SLEEPTIME 10

static char *large_buf = NULL;

int comm_size;

uint64_t work_iter = 20000;

static void flush(int rank, MPI_Win win)
{
  MPI_Win_flush(rank, win);
}

static void flush_local(int rank, MPI_Win win)
{
  MPI_Win_flush_local(rank, win);
}


double
work(double val)
{
  sleep(SLEEPTIME); return 1.0;
}

uint64_t
bench_flush(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == 0) {
    // do nothing
  } else {
    uint64_t res;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Win_flush_local(rank, win);
    }
  }
  return NUM_REPS;
}

uint64_t
sendrecv(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  uint64_t res;
  if (target == rank) 
  {
    int source = (rank + 1) % comm_size;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Recv(&res, 1, type, source, 1000, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Send(&res, 1, type, target, 1000, MPI_COMM_WORLD);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return NUM_REPS;
}

uint64_t
sendrecv_twoway(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  uint64_t res;
  if (target == rank) 
  {
    int source = (rank + 1) % comm_size;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Recv(&res, 1, type, source, 1000, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Send(&res, 1, type, source, 1000, MPI_COMM_WORLD);
    }
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Send(&res, 1, type, target, 1000, MPI_COMM_WORLD);
      MPI_Recv(&res, 1, type, target, 1000, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return NUM_REPS;
}

uint64_t
ssendrecv(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  uint64_t res;
  if (target == rank) 
  {
    int source = (rank + 1) % comm_size;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Recv(&res, 1, type, source, 1000, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Ssend(&res, 1, type, target, 1000, MPI_COMM_WORLD);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return NUM_REPS;
}

uint64_t
fetch_op(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    uint64_t res;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 1;
      MPI_Fetch_and_op(&val, &res, type, target, 0, MPI_SUM, win);
      flush(target, win);
    }
  }
  return NUM_REPS;
}

uint64_t
fetch_op_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    uint64_t res;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 1;
      MPI_Fetch_and_op(&val, &res, type, target, 0, MPI_SUM, win);
      flush(target, win);
    }
  }
  return NUM_REPS;
}

uint64_t
fetch_noop(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    uint64_t res;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 1;
      MPI_Fetch_and_op(&val, &res, type, target, 0, MPI_NO_OP, win);
      flush(target, win);
    }
  }
  return NUM_REPS;
}

uint64_t
fetch_noop_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    uint64_t res;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 1;
      MPI_Fetch_and_op(&val, &res, type, target, 0, MPI_NO_OP, win);
      flush(target, win);
    }
  }
  return NUM_REPS;
}


uint64_t
compare_exchange(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    uint64_t res = 0;
    uint64_t val = 0;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t cmp = val;
      val = res + 1;
      MPI_Compare_and_swap(&val, &cmp, &res, type, target, 0, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}

uint64_t
compare_exchange_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    uint64_t res = 0;
    uint64_t val = 0;
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t cmp = val;
      val = cmp + 1;
      MPI_Compare_and_swap(&val, &cmp, &res, type, target, 0, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}


uint64_t
accumulate(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0xDEADBEEF;
      MPI_Accumulate(&val, 1, type, target, 0, 1, type, MPI_SUM, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}

uint64_t
accumulate_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0xDEADBEEF;
      MPI_Accumulate(&val, 1, type, target, 0, 1, type, MPI_SUM, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}

uint64_t
accumulate_large(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS_LARGE; ++i) {
      uint64_t val = 0xDEADBEEF;
      MPI_Accumulate(large_buf, LARGE_SIZE/sizeof(uint64_t), MPI_UINT64_T, target, 0, LARGE_SIZE/sizeof(uint64_t), MPI_UINT64_T, MPI_REPLACE, win);
      //flush(target, win);
    }
    flush(target, win);
  }

  return NUM_REPS;
}

uint64_t
accumulate_flushlocal(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0xDEADBEEF;
      MPI_Accumulate(&val, 1, type, target, 0, 1, type, MPI_SUM, win);
      flush_local(target, win);
    }
  }

  return NUM_REPS;
}


uint64_t
get_accumulate(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0xDEADBEEF;
      uint64_t result = 0;
      MPI_Get_accumulate(&val, 1, type, &result, 1, type, target, 0, 1, type, MPI_SUM, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}

uint64_t
get_accumulate_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0xDEADBEEF;
      uint64_t result = 0;
      MPI_Get_accumulate(&val, 1, type, &result, 1, type, target, 0, 1, type, MPI_SUM, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}


uint64_t
rget_wait(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val;
      MPI_Request req;
      MPI_Rget(&val, 1, type, target, 0, 1, type, win, &req);
      MPI_Wait(&req, MPI_STATUS_IGNORE);
    }
  }

  return NUM_REPS;
}

uint64_t
rget_wait_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = -1;
      MPI_Request req;
      MPI_Rget(&val, 1, type, target, 0, 1, type, win, &req);
      MPI_Wait(&req, MPI_STATUS_IGNORE);
    }
  }
  return NUM_REPS;
}

uint64_t
get_flushlocal(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val;
      MPI_Request req;
      MPI_Rget(&val, 1, type, target, 0, 1, type, win, &req);
      MPI_Wait(&req, MPI_STATUS_IGNORE);
    }
  }

  return NUM_REPS;
}

uint64_t
get_flushlocal_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = -1;
      MPI_Request req;
      MPI_Rget(&val, 1, type, target, 0, 1, type, win, &req);
      MPI_Wait(&req, MPI_STATUS_IGNORE);
    }
  }
  return NUM_REPS;
}


uint64_t
put_flush(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0;
      MPI_Put(&val, 1, type, target, 0, 1, type, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}

uint64_t
put_flush_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0;
      MPI_Put(&val, 1, type, target, 0, 1, type, win);
      flush(target, win);
    }
  }
  return NUM_REPS;
}


uint64_t
put_flushlocal(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0;
      MPI_Put(&val, 1, type, target, 0, 1, type, win);
      flush_local(target, win);
    }
  }
  return NUM_REPS;
}

uint64_t
put_flushlocal_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      uint64_t val = 0;
      MPI_Put(&val, 1, type, target, 0, 1, type, win);
      flush_local(target, win);
    }
  }
  return NUM_REPS;
}

uint64_t
put_flush_large(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS_LARGE; ++i) {
      uint64_t val = 0;
      MPI_Put(large_buf, LARGE_SIZE/sizeof(uint64_t), MPI_UINT64_T, target, 0, LARGE_SIZE/sizeof(uint64_t), MPI_UINT64_T, win);
      flush(target, win);
    }
  }

  return NUM_REPS;
}



uint64_t
lock(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  // unused
  (void)type;
  if (rank == target) {
    // do nothing
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, target, 0, win);
      MPI_Win_unlock(target, win);
    }
  }
  return NUM_REPS;
}

uint64_t
lock_sleep(MPI_Win win, MPI_Datatype type, int rank, int target)
{
  // unused
  (void)type;
  if (rank == target) {
    //sleep(10);
    double res = work(10);
    if (res < 0) printf("%f", res);
  } else {
    for (size_t i = 0; i < NUM_REPS; ++i) {
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, target, 0, win);
      MPI_Win_unlock(target, win);
    }
  }
  return NUM_REPS;
}

static char *lock_type;

//#define STR(s)  XSTR(s)
#define STR(s)  #s
#define XSTR(s) #s

#define BENCHMARK_BARRIER(_op, _type, _typestr, _win, _rank, _target) \
  do { \
    struct timespec start, end; \
    uint64_t reps;              \
    MPI_Barrier(MPI_COMM_WORLD); \
    reps = _op(_win, _type, _rank, _target); \
    clock_gettime(CLOCK_REALTIME, &start); \
    reps = _op(_win, _type, _rank, _target); \
    clock_gettime(CLOCK_REALTIME, &end); \
    MPI_Barrier(MPI_COMM_WORLD); \
    if (rank != 0) {           \
      double time = ((end.tv_sec - start.tv_sec) * 1E6 + (end.tv_nsec - start.tv_nsec) / 1E3); \
      printf("Time for %lu times %s on %s with %s lock: %fus (%fus avg)\n", NUM_REPS, STR(_op), _typestr, \
             lock_type, time, time / NUM_REPS); \
    } \
  } while (0)

#define BENCHMARK_(_op, _type, _typestr, _win, _rank, _target) BENCHMARK_BARRIER(_op, _type, _typestr, _win, _rank, _target)
#define BENCHMARK(_op, _type, _win, _rank, _target) BENCHMARK_BARRIER(_op, _type, #_type, _win, _rank, _target)

#define BENCHMARK_LOCAL_(_op, _type, _typestr, _win, _rank) \
  do { \
    struct timespec start, end; \
    uint64_t reps;              \
    reps = _op(_win, _type, !_rank, _rank); \
    clock_gettime(CLOCK_REALTIME, &start); \
    reps = _op(_win, _type, !_rank, _rank); \
    clock_gettime(CLOCK_REALTIME, &end); \
    double time = ((end.tv_sec - start.tv_sec) * 1E6 + (end.tv_nsec - start.tv_nsec) / 1E3); \
    printf("Time for %lu times %s on %s with %s lock local: %fus (%fus avg)\n", NUM_REPS, STR(_op), _typestr, \
           lock_type, time, time / NUM_REPS); \
  } while (0)

#define BENCHMARK_LOCAL(_op, _type, _win, _rank) BENCHMARK_LOCAL_(_op, _type, #_type, _win, _rank)


static void benchmark_target(int target, int rank, int size, MPI_Win win)
{

#define BENCHMARK_FOR_TYPE(_type)                                      \
  do {                                                                 \
      BENCHMARK_(fetch_op, _type, #_type, win, rank, target);                   \
      BENCHMARK_(fetch_op_sleep, _type, #_type, win, rank, target);             \
      BENCHMARK_(fetch_noop, _type, #_type, win, rank, target);                   \
      BENCHMARK_(fetch_noop_sleep, _type, #_type, win, rank, target);             \
      BENCHMARK_(compare_exchange, _type, #_type, win, rank, target);           \
      BENCHMARK_(compare_exchange_sleep, _type, #_type, win, rank, target);     \
      BENCHMARK_(accumulate, _type,  #_type, win, rank, target);                 \
      BENCHMARK_(accumulate_sleep, _type, #_type, win, rank, target);           \
      BENCHMARK_(accumulate_flushlocal, _type, #_type, win, rank, target);           \
      BENCHMARK_(get_accumulate, _type,  #_type, win, rank, target);             \
      BENCHMARK_(get_accumulate_sleep, _type,  #_type, win, rank, target);       \
      BENCHMARK_(rget_wait, _type,  #_type, win, rank, target);                   \
      BENCHMARK_(rget_wait_sleep, _type, #_type, win, rank, target);             \
      BENCHMARK_(get_flushlocal, _type,  #_type, win, rank, target);                   \
      BENCHMARK_(get_flushlocal_sleep, _type, #_type, win, rank, target);             \
      BENCHMARK_(put_flush, _type, #_type, win, rank, target);                  \
      BENCHMARK_(put_flush_sleep, _type, #_type, win, rank, target);            \
      BENCHMARK_(put_flushlocal, _type, #_type, win, rank, target);             \
      BENCHMARK_(put_flushlocal_sleep, _type, #_type, win, rank, target);       \
  } while (0)



    if (size == 2) {
      lock_type = "exclusive"; 
      if (rank > 0)
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, 0, 0, win);

      printf("Running with exclusive lock...\n");
      BENCHMARK_FOR_TYPE(MPI_UINT32_T);
      BENCHMARK_FOR_TYPE(MPI_UINT64_T);
      if (rank > 0)
        MPI_Win_unlock(0, win);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    printf("Running with shared lock...\n");
    MPI_Win_lock_all(0, win);
    lock_type = "shared"; 
    BENCHMARK(put_flush_large, MPI_UINT64_T, win, rank, target);
    BENCHMARK(accumulate_large, MPI_UINT64_T, win, rank, target);
    BENCHMARK_FOR_TYPE(MPI_UINT32_T);
    BENCHMARK_FOR_TYPE(MPI_UINT64_T);
    MPI_Win_unlock_all(win);

    MPI_Barrier(MPI_COMM_WORLD);
    BENCHMARK(lock, MPI_UINT64_T, win, rank, target);
    BENCHMARK(lock_sleep, MPI_UINT64_T, win, rank, target);
    BENCHMARK(sendrecv, MPI_UINT64_T, win, rank, target);
    BENCHMARK(ssendrecv, MPI_UINT64_T, win, rank, target);
    BENCHMARK(sendrecv_twoway, MPI_UINT64_T, win, rank, target);
    MPI_Barrier(MPI_COMM_WORLD);

#undef BENCHMARK_FOR_TYPE
}

static void benchmark_local(int target, int rank, int size, MPI_Win win)
{

#define BENCHMARK_FOR_TYPE(_type)                                      \
  do {                                                                 \
      BENCHMARK_LOCAL_(fetch_op, _type, #_type, win, rank);                   \
      BENCHMARK_LOCAL_(fetch_noop, _type, #_type, win, rank);                   \
      BENCHMARK_LOCAL_(compare_exchange, _type, #_type, win, rank);           \
      BENCHMARK_LOCAL_(accumulate, _type,  #_type, win, rank);                 \
      BENCHMARK_LOCAL_(accumulate_flushlocal, _type, #_type, win, rank);           \
      BENCHMARK_LOCAL_(get_accumulate, _type,  #_type, win, rank);             \
      BENCHMARK_LOCAL_(rget_wait, _type,  #_type, win, rank);                   \
      BENCHMARK_LOCAL_(get_flushlocal, _type,  #_type, win, rank);                   \
      BENCHMARK_LOCAL_(put_flush, _type, #_type, win, rank);                  \
      BENCHMARK_LOCAL_(put_flushlocal, _type, #_type, win, rank);             \
  } while (0)

    if (rank == target) {
      lock_type = "exclusive"; 
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, 0, 0, win);

      printf("Running with exclusive lock...\n");
      BENCHMARK_FOR_TYPE(MPI_UINT32_T);
      BENCHMARK_FOR_TYPE(MPI_UINT64_T);
      MPI_Win_unlock(0, win);
      printf("Running with shared lock...\n");
      MPI_Win_lock_all(0, win);
      lock_type = "shared"; 
      BENCHMARK_LOCAL(put_flush_large, MPI_UINT64_T, win, rank);
      BENCHMARK_LOCAL(accumulate_large, MPI_UINT64_T, win, rank);
      BENCHMARK_FOR_TYPE(MPI_UINT32_T);
      BENCHMARK_FOR_TYPE(MPI_UINT64_T);
      MPI_Win_unlock_all(win);

      BENCHMARK_LOCAL(lock, MPI_UINT64_T, win, rank);
      BENCHMARK_LOCAL(lock_sleep, MPI_UINT64_T, win, rank);
    }
#undef BENCHMARK_FOR_TYPE
}


int main(int argc, char **argv)
{
    int rank;
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Win win;
    char *base;

    large_buf = malloc(LARGE_SIZE);

    MPI_Info info;
    MPI_Info_create(&info);
    MPI_Info_set(info, "accumulate_ordering", "none");
    MPI_Info_set(info, "same_size"          , "true");
    MPI_Info_set(info, "same_disp_unit"     , "true");
    MPI_Info_set(info, "accumulate_ops"     , "same_op");

    MPI_Win_allocate(
        LARGE_SIZE,
        1,
        info,
        MPI_COMM_WORLD,
        &base,
        &win);

    MPI_Info_free(&info);

    benchmark_local(0, rank, comm_size, win);
    benchmark_target(0, rank, comm_size, win);

    free(large_buf);

    MPI_Win_free(&win);
    MPI_Finalize();

    return 0;
}

