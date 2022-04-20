# cs561_btree

## Compilation
Run the "make" command in the terminal to compile using the Makefile provided. 

Note, the "-DBPLUS" compilation flag converts the B-epsilon tree implementation to a B+ tree. Essentially, A B-epsilon tree with a single spot in the buffer of every internal node (including the root) will function as a B+ tree, since all these internal nodes will immediately flush the inserted entry down to the lower levels (cascading until the inserted entry reaches the leaf level). 

## Generate Test data:
 Two parameters are required by the generator: noise (%) is the percentage (int) of out of order elements, and windowThreshold(%) is the window (as percentage of total elements) within which an out of order element can be placed from its original location. So, a 5% noise and 5% window threshold means 5% of the total domain size of elements will be out-of-order and each of these out-of-order elements will be placed within a 5% window from its original (sorted) location. Compile the workload generator using the "make" or "make workloadgenerator" command, and execute using

`./workloadgenerator <domain_size> <noise_percentage> <windowThreshold>` 

For example, to generate a workload of 1 Million integers with 0% noise and 5% windowThreshold, use:

`./workloadgenerator 1000000 0 5`

Currently, the script workload.sh can be used to generate specified test dataset. It now supports creating test set of 100K, 1M, 10M and 50M in size by passing the parameter to the script. For each data size, it will generate two types of test sets: "k" test and "l" test. In "k" test, "l" is set to 50, while "k" is varied from 10 to 50 in increment of 10. In "l" test, "k" is set to 35, while "l" is varied from 10 to 50 in increment of 10. Run the script in the root directory of the program.
For example, after running the command
`./workload.sh 100K`
A new directory will be created, and the structure of it should be like:
test_set/100K_test
├── k_test
│   ├── k10
│   │   ├── test case 1
│   │   ├── test case 2
│   │   ├── test case 3
│   │   ├── test case 4
│   │   └── test case 5
│   ├── k20
│   │   └── ...(same as above)
│   ├── k30
│   │   └── ...
│   ├── k40
│   │   └── ...
│   └── k50
│       └── ...


## Run analysis
To run the analysis, first use command "make" or "make analysis". The "analysis.o" receives one input data file created by workload_generator.

`./analysis.o <data_file_path>`

Currently, it only displays the insertion time cost of the dual_tree comparing with a single b-plus tree.