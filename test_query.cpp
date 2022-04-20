#include <iostream>
#include <thread>
#include "betree.h"
#include "dual_tree.h"

std::vector<int> generatePointQueries(std::vector<int> data, int n)
{
    std::vector<int> queries(data.begin(), data.end());

    // add a few elements out of range
    int non_existing_counter = (data.size() * 0.1);
    std::uniform_int_distribution<int> dist(n, (int)(1.8 * n));
    // Initialize the random_device
    std::random_device rd;
    // Seed the engine
    std::mt19937_64 generator(rd());
    std::set<int> non_existing;
    while (non_existing.size() != non_existing_counter)
    {
    non_existing.insert(dist(generator));
    }

    queries.insert(queries.end(), non_existing.begin(), non_existing.end());

    // shuffle indexes
    std::random_shuffle(queries.begin(), queries.end());

    return queries;
}

std::vector<int> generatePeriodicQuery(std::vector<int> data)
{
    std::vector<int> queries;

    for (auto i: data)
    {
        for (int i = 0; i < 5; i++)
        {
            queries.insert(queries.end(),i);
        }
    }
    
    return queries;
}

void dual_tree_test_query(const std::vector<int>& data_set)
{
    auto start = std::chrono::high_resolution_clock::now();
    dual_tree<int, int> dt;
    int idx = 0;
    for(int i: data_set)
    {
        dt.insert(i, idx++); 
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    std::cout << "--------------------------------------------------------------------------" << std::endl;

    std::cout << "Data Load time For dual tree(us):" << duration.count() << std::endl;
    std::cout << "Sorted tree size: " << dt.sorted_tree_size() << std::endl;
    std::cout << "Unsorted tree size: " << dt.unsorted_tree_size() << std::endl;
    std::cout << "dual b+ tree size " << dt.unsorted_tree_size() + dt.sorted_tree_size() + 16 << std::endl;
    dt.fanout();

    std::cout << "--------------------------------------------------------------------------" << std::endl;

    // simple query the dual tree
    std::vector<int> queries = generatePointQueries(data_set, data_set.size());
    std::vector<int> p_queries = generatePeriodicQuery(data_set);
    int counter = 0;

    start = std::chrono::high_resolution_clock::now();
    for (int i : queries) 
    {
        counter += dt.query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Naive query with Random Workload Performance for dual tree(us):" << duration.count() << std::endl;
    std::cout << "Dual B+ Tree found " << counter << " out of " << queries.size() << std::endl;

    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : data_set) 
    {
        counter += dt.query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Naive query with Sequential Workload Performance for dual tree(us):" << duration.count() << std::endl;
    std::cout << "Dual B+ Tree found " << counter << " out of " << data_set.size() << std::endl;

    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : p_queries) 
    {
        counter += dt.query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Naive query with Periodic Workload Performance for dual tree(us):" << duration.count() << std::endl;
    std::cout << "Dual B+ Tree found " << counter << " out of " << p_queries.size() << std::endl;

    // query the dual tree in parallel
    // counter = 0;
    // start = std::chrono::high_resolution_clock::now();
    // for (int i : queries) 
    // {
    //     counter += dt.parallelQuery(i);
    // }
    // stop = std::chrono::high_resolution_clock::now();
    // duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    // std::cout << "Prallel query time for dual tree(us):" << duration.count() << std::endl;
    // std::cout << "Dual B+ Tree with Parallel read found " << counter << " out of " << data_set.size() << std::endl;


    // query the dual tree using MRU
    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : queries) 
    {
        counter += dt.MRU_query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "MRU query with Random Workload Performance for dual tree(us):" << duration.count() << std::endl;
    std::cout << "Dual B+ Tree with MRU read found " << counter << " out of " << queries.size() << std::endl;

    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : data_set) 
    {
        counter += dt.MRU_query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "MRU query with Sequential Workload Performance for dual tree(us):" << duration.count() << std::endl;
    std::cout << "Dual B+ Tree with MRU read found " << counter << " out of " << data_set.size() << std::endl;

    
    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : p_queries) 
    {
        counter += dt.MRU_query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "MRU query with Periodic Workload Performance for dual tree(us):" << duration.count() << std::endl;
    std::cout << "Dual B+ Tree with MRU read found " << counter << " out of " << p_queries.size() << std::endl;
    std::cout << "--------------------------------------------------------------------------" << std::endl;
    
}

void b_plus_tree_test_query(const std::vector<int>& data_set)
{

    auto start = std::chrono::high_resolution_clock::now();
    BeTree<int,int> tree("manager", "./tree_dat", BeTree_Default_Knobs<int, int>::BLOCK_SIZE,
        BeTree_Default_Knobs<int, int>::BLOCKS_IN_MEMORY);

    int idx = 0;
    for(int i: data_set)
    {
        tree.insert(i, idx++);
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    std::cout << "--------------------------------------------------------------------------" << std::endl;
    std::cout << "Data Load time For b plus tree(us):" << duration.count() << std::endl;
    std::cout << "--------------------------------------------------------------------------" << std::endl;

    // query the b+ tree as baseline
    int counter = 0;
    std::vector<int> queries = generatePointQueries(data_set, 1000000);
    std::vector<int> p_queries = generatePeriodicQuery(data_set);

    start = std::chrono::high_resolution_clock::now();
    for (int i : queries) 
    {
        counter += tree.query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "B+ Tree query with Random Workload Performance:" << duration.count() << std::endl;
    std::cout << "B+ Tree found " << counter << " out of " << queries.size() << std::endl;

    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : data_set) 
    {
        counter += tree.query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "B+ Tree query with Sequential Workload Performance:" << duration.count() << std::endl;
    std::cout << "B+ Tree found " << counter << " out of " << data_set.size() << std::endl;

    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i : p_queries) 
    {
        counter += tree.query(i);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "B+ Tree query with Periodic Workload Performance:" << duration.count() << std::endl;
    std::cout << "B+ Tree found " << counter << " out of " << p_queries.size() << std::endl;
    std::cout << "--------------------------------------------------------------------------" << std::endl;

}

void simple_test_query()
{
    dual_tree<int, int> dt;
    dt.insert(10, 10);
    dt.insert(20, 20);

    std::cout << dt.query(10) << std::endl;
    // std::cout << dt.parallelQuery(10) << std::endl;
    std::cout << dt.MRU_query(10) << std::endl;
    std::cout << dt.MRU_query(12) << std::endl;
}

int main(int argc, char **argv)
{
    if(argc < 2)
    {
        std::cout<< "Usage: ./main <input_file>" << std::endl;
    }

    // Read the input file
    std::string input_file = argv[1];
    std::ifstream ifs;
    std::vector<int> data;

    ifs.open(input_file);
    ifs.seekg(0, std::ios::end);
    size_t filesize = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    data.resize(filesize / sizeof(int));
    ifs.read((char*)data.data(), filesize);

    dual_tree<int, int>::show_tree_knobs();
    
    dual_tree_test_query(data);
    b_plus_tree_test_query(data);

    // simple_test_query();

    return 0;
}