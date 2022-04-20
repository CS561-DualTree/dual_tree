#include <iostream>
#include <thread>
#include "betree.h"
#include "dual_tree.h"

void dual_tree_test(const std::vector<int>& data_set)
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

    std::cout << "Data Load time For dual tree(us):" << duration.count() << std::endl;
    std::cout << "Sorted tree size: " << dt.sorted_tree_size() << std::endl;
    std::cout << "Unsorted tree size: " << dt.unsorted_tree_size() << std::endl;
    std::cout << "Sorted Tree domain size: " << dt.sorted_tree_min() << " - " << dt.sorted_tree_max() << std::endl;
    std::cout << "Unsorted Tree domain size: " << dt.unsorted_tree_min() << " - " << dt.unsorted_tree_max() << std::endl;
    dt.fanout();

    int cnt1 = 0;
    int cnt2 = 0;
    int cnt = 0;
    for(int i :data_set)
    {
        if(dt.query(i))
            cnt1 += 1;
        else
        {
            cnt2 += 1;
            if(cnt2 <= 10)
                std::cout << "Missing " << i << "in position " << cnt << std::endl;
        }
        cnt += 1;
    }
    std::cout<<"Query hit:" << cnt1 << std::endl;
    std::cout<<"Query miss:" << cnt2 << std::endl;
}

void b_plus_tree_test(const std::vector<int>& data_set)
{
    // init a tree that takes integers for both key and value
    // the first argument is the name of the block manager for the cache (can be anything)
    // second argument is the directory where the block manager will operate (if changing this, make sure to update makefile)
    // 3rd argument is the size of every block in bytes. Suggest to keep it as is since 4096B = 4KB = 1 page
    // 4th argument is the number of blocks to store in memory. Right now, it is at 10000 blocks = 10000*4096 bytes = 40.96 MB
    // potentially, the only argument to change would be the last one (4th arg) to increase the memory capacity

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

    std::cout << "Data Load time For b plus tree(us):" << duration.count() << std::endl;
    int cnt1 = 0;
    int cnt2 = 0;
    for(int i: data_set)
    {
        if(tree.query(i))
            cnt1 += 1;
        else
        {
            cnt2 += 1;
            //std::cout << "Missing " << i << std::endl;
        }
    }
    std::cout<<"Query hit:" << cnt1 << std::endl;
    std::cout<<"Query miss:" << cnt2 << std::endl;

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

    dual_tree_test(data);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    b_plus_tree_test(data);
    return 0;
}