# cs561_btree

## Compilation
Run the "make" command in the terminal to compile using the Makefile provided. 

Note, the "-DBPLUS" compilation flag converts the B-epsilon tree implementation to a B+ tree. Essentially, A B-epsilon tree with a single spot in the buffer of every internal node (including the root) will function as a B+ tree, since all these internal nodes will immediately flush the inserted entry down to the lower levels (cascading until the inserted entry reaches the leaf level). 

## Generate Test data:
 Two parameters are required by the generator: noise (%) is the percentage (int) of out of order elements, and windowThreshold(%) is the window (as percentage of total elements) within which an out of order element can be placed from its original location. So, a 5% noise and 5% window threshold means 5% of the total domain size of elements will be out-of-order and each of these out-of-order elements will be placed within a 5% window from its original (sorted) location. Compile the workload generator using the "make" or "make workloadgenerator" command, and execute using

`./workloadgenerator <domain_size> <noise_percentage> <windowThreshold>` 

For example, to generate a workload of 1 Million integers with 0% noise and 5% windowThreshold, use:

`./workloadgenerator 1000000 0 5`

## Run analysis
To run the analysis, first use command "make" or "make analysis". The "analysis.o" receives one input data file created by workload_generator.

`./analysis.o <data_file_path>`

Currently, it only displays the insertion time cost of the dual_tree comparing with a single b-plus tree.