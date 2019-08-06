MPICC=mpicc
CFLAGS=-O2 -std=c11
LDFLAGS=-O2

BINARY=mpi_progress

all: $(BINARY)

mpi_progress: mpi_progress.o
	$(MPICC) $(LDFLAGS) $< -o $@

%.o: %.c
	$(MPICC) $(CFLAGS) $< -c -o $@

clean:
	rm -f *.o $(BINARY)
