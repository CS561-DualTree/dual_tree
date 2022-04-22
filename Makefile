

MKDIR_P = mkdir -p
all: analysis workloadgenerator test_query
	$(MKDIR_P) tree_dat
	
simple_analysis: betree.h dual_tree.h analysis.cpp -DBPLUS
	g++ -g -std=c++11 betree.h dual_tree.h analysis.cpp -o analysis.o 

analysis: betree.h dual_tree.h analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h analysis.cpp -o analysis.o -DTIMER -DBPLUS -lpthread

test_query: betree.h dual_tree.h test_query.cpp
	g++ -g -std=c++11 betree.h dual_tree.h test_query.cpp -o test_query.o -DTIMER -DBPLUS -lpthread

workloadgenerator: workload_generator.cpp
	g++ -g -std=c++11 workload_generator.cpp -o workload_generator.o 

clean: 
	$(RM) *.o
	$(RM) tree_dat/*
	rm -r tree_dat/
