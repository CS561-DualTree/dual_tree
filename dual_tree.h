#ifndef DUALTREE_H
#define DUEALTREE_H
#include "betree.h"
#include <stdlib.h>

template<typename _key, typename _value>
class DUAL_TREE_KNOBS
{
public:
    // Sorted tree split fraction, it will affect the space utilization of the tree. It means how
    // many elements will stay in the original node.
    static constexpr float SORTED_TREE_SPLIT_FRAC = 0.9;

    // Unsorted tree splitting fraction.
    static constexpr float UNSORTED_TREE_SPLIT_FRAC = 0.5;

    // Heap buffer size(in number of tuples), default is 0. When it is non-zero, newly added tuples will be put into 
    // the heap, when the heap size reaches the threshold, pop the root tuple of the heap and add it to one of the trees.
    // Note that a big heap will cost huge overhead.
    static const uint HEAP_SIZE = 16;
};

template <typename _key, typename _value, typename _compare=std::less<_key>>
class key_comparator
{
public:
    bool operator() (const std::pair<_key, _value>& p1, const std::pair<_key, _value>& p2)
    {
        _compare cmp{};
        return !cmp(p1.first, p2.first);
    }
};

template <typename _key, typename _value, typename _dual_tree_knobs=DUAL_TREE_KNOBS<_key, _value>,
            typename _betree_knobs = BeTree_Default_Knobs<_key, _value>, 
            typename _compare=std::less<_key>>
class dual_tree
{
    // Left tree to accept unsorted input data.
    BeTree<_key, _value, _betree_knobs, _compare> *unsorted_tree;
    // Right tree to accept sorted input data.
    BeTree<_key, _value, _betree_knobs, _compare> *sorted_tree;

    uint sorted_size;

    uint unsorted_size;

    std::priority_queue<std::pair<_key, _value>, std::vector<std::pair<_key, _value>>, 
        key_comparator<_key, _value>> *heap_buf;


public:

    // Default constructor, disable the buffer.
    dual_tree()
    {   
        unsorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>("manager", "./tree_dat", 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY, DUAL_TREE_KNOBS<_key, _value>::UNSORTED_TREE_SPLIT_FRAC);
        sorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>("manager", "./tree_dat", 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY, DUAL_TREE_KNOBS<_key, _value>::SORTED_TREE_SPLIT_FRAC);
        sorted_size = 0;
        unsorted_size = 0;
        if(_dual_tree_knobs::HEAP_SIZE != 0) 
            heap_buf = new std::priority_queue<std::pair<_key, _value>, std::vector<std::pair<_key, _value>>,
                key_comparator<_key, _value>>();
    }

    // Deconstructor
    ~dual_tree()
    {
        delete sorted_tree;
        delete unsorted_tree;
        if(_dual_tree_knobs::HEAP_SIZE != 0)
            delete heap_buf;
    }

    uint sorted_tree_size() { return sorted_size;}

    uint unsorted_tree_size() { return unsorted_size;}

    bool insert(_key key, _value value)
    {
        _key inserted_key = key;
        _value inserted_value = value;
        if(_dual_tree_knobs::HEAP_SIZE != 0)
        {
            assert(heap_buf->size() <= _dual_tree_knobs::HEAP_SIZE);
            if(heap_buf->size() == _dual_tree_knobs::HEAP_SIZE)
            {
                heap_buf->push(std::pair<_key, _value>(inserted_key, inserted_value));
                std::pair<_key, _value> tmp = heap_buf->top();
                heap_buf->pop();
                inserted_key = tmp.first;
                inserted_value = tmp.second;
            }
            else
            {
                // heap is not full, add new tuple to the heap and return
                heap_buf->push(std::pair<_key, _value>(inserted_key, inserted_value));
                return true;
            }
        }
        if(sorted_size == 0)
        {
            // The first tuple is always inserted to the 
            sorted_tree->insert_to_tail_leaf(inserted_key, inserted_value);
            sorted_size += 1;
        }
        else 
        {
            if(inserted_key < sorted_tree->getMaximumKey())
            {
                unsorted_tree->insert(inserted_key, inserted_value);
                unsorted_size += 1;
            }
            else
            {
                sorted_tree->insert_to_tail_leaf(inserted_key, inserted_value);
                sorted_size += 1;
            }
        }
        return true;
    }

    bool query(_key key)
    {
        // First search the one with less tuples.
        if(sorted_size < unsorted_size)
        {
            return sorted_tree->query(key) || unsorted_tree->query(key);
        }
        else
        {
            return unsorted_tree->query(key) || sorted_tree->query(key);
        }
        
    }

    std::vector<std::pair<_key, _value>> rangeQuery(_key low, _key high) 
    {
        // Range query in both tree.
        std::vector<std::pair<_key, _value>> unsorted_res = unsorted_tree.rangeQuery(low, high);
        std::vector<std::pair<_key, _value>> sorted_res = sorted_tree.rangeQuery(low, high);
        unsorted_res.insert(unsorted_res.end(), sorted_res.begin(), sorted_res.end());
        return unsorted_res;
    }

    void fanout()
    {
        sorted_tree->fanout();
        std::cout << "Sorted Tree: number of splitting leaves = " << sorted_tree->traits.leaf_splits
            << std::endl;
        std::cout << "Sorted Tree: number of splitting internal nodes = " << 
            sorted_tree->traits.internal_splits << std::endl;
        std::cout << "Sorted Tree: number of leaves = " << sorted_tree->traits.num_leaf_nodes 
            << std::endl;
        std::cout << "Sorted Tree: number of internal nodes = " << 
            sorted_tree->traits.num_internal_nodes << std::endl;

        unsorted_tree->fanout();
        std::cout << "Unsorted Tree: number of splitting leaves = " << unsorted_tree->traits.leaf_splits
            << std::endl;
        std::cout << "Unsorted Tree: number of splitting internal nodes = " << 
            unsorted_tree->traits.internal_splits << std::endl;
        std::cout << "Unsorted Tree: number of leaves = " << unsorted_tree->traits.num_leaf_nodes 
            << std::endl;
        std::cout << "Unsorted Tree: number of internal nodes = " << 
            unsorted_tree->traits.num_internal_nodes << std::endl;
    }

    static void show_tree_knobs()
    {
        std::cout << "B Epsilon Tree Knobs:" << std::endl;
        std::cout << "Number of Upserts = " << _betree_knobs::NUM_UPSERTS << std::endl;
        std::cout << "Number of Pivots = " << _betree_knobs::NUM_PIVOTS << std::endl;
        std::cout << "Number of Children = " << _betree_knobs::NUM_CHILDREN << std::endl;
        std::cout << "Number of Data pairs = " << _betree_knobs::NUM_DATA_PAIRS << std::endl;
#ifdef UNITTEST

#else
        std::cout << "Block Size = " << _betree_knobs::BLOCK_SIZE << std::endl;
        std::cout << "Data Size = " << _betree_knobs::DATA_SIZE << std::endl;
        std::cout << "Block Size = " << _betree_knobs::BLOCK_SIZE << std::endl;
        std::cout << "Metadata Size = " << _betree_knobs::METADATA_SIZE << std::endl;
        std::cout << "Unit Size = " << _betree_knobs::UNIT_SIZE << std::endl;
        std::cout << "Pivots Size = " << _betree_knobs::PIVOT_SIZE << std::endl;
        std::cout << "Buffer Size = " << _betree_knobs::BUFFER_SIZE << std::endl;
#endif
        std::cout << "--------------------------------------------------------------------------" << std::endl;

        std::cout << "Dual Tree Knobs:" << std::endl;
        std::cout << "Sorted tree split fraction = " << _dual_tree_knobs::SORTED_TREE_SPLIT_FRAC << std::endl;
        std::cout << "Unsorted tree split fraction = " << _dual_tree_knobs::UNSORTED_TREE_SPLIT_FRAC << std::endl;
        std::cout << "Heap buffer size = " << _dual_tree_knobs::HEAP_SIZE << std::endl;

        std::cout << "--------------------------------------------------------------------------" << std::endl;
    }
};

#endif
