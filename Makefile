

MKDIR_P = mkdir -p
all: analysis workloadgenerator
	$(MKDIR_P) tree_dat
	
simple_analysis: betree.h dual_tree.h analysis.cpp -DBPLUS
	g++ -g -std=c++11 betree.h dual_tree.h analysis.cpp -o analysis.o 

analysis: betree.h dual_tree.h analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h analysis.cpp -o analysis.o -DTIMER -DBPLUS

workloadgenerator: workload_generator.cpp
	g++ -g -std=c++11 workload_generator.cpp -o workload_generator.o 

clean: 
	$(RM) *.o
	$(RM) tree_dat/*
	rm -r tree_dat/
