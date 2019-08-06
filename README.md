# mpi-progress
A simple benchmark to determine some progress characteristics of MPI RMA implementations.

In particular, this benchmark is designed to determine whether an MPI implementation provides progress dependending on whether the remote side is active in MPI or not.
The remote side either waits in a barrier or sleeps for a certain time. 
The origin attempts to perform RMA operations and will block if no progress is provided. 
Hence, the resulting time reported will be the sum of the time the origin was blocked waiting for the remote side to become active in MPI (i.e., enter the barrier) and the time needed to perform all N operations divided by N.
If the origin does not block waiting for the target it will simply report the average time per operation.

All test cases exist in a regular version (the target immediately enters an `MPI_Barrier`) and a version suffixed `_sleep` in which the target sleeps for 10s before entering the barrier.
For the `_sleep` version, a latency >100us strongly indicates that no progress was available while the target was sleeping (with `N=100k` the average `10s / N` yields 100us).

The benchmark also provides measurements for local RMA operations to help determine the difference in latency between local and non-local MPI RMA operations.

## Building

If `mpicc` is not in the `$PATH` please adjust the variable `MPICC` in the `Makefile`.

```
$ make
```

## Running

To run using `N` ranks use:
```
mpirun -n <N> -N 1 ./mpi_progress
```

The benchmark can run on more than 2 ranks but will refuse to run the tests using an exclusive lock if more than 2 ranks are provided. 
In that case, only the tests using shared lock will be performed.
