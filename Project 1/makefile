GCC=mpic++

all: Final_PageRank.cpp
	$(GCC) ./Final_PageRank.cpp -o main  

 
run: main 
	mpiexec -np 4 main fl_compact.tab fl_compact_part.4 5 4