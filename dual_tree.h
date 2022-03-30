#ifndef DUALTREE_H
#define DUEALTREE_H
#include "betree.h"
#include <stdlib.h>

template<typename _key, typename _value>
class DUAL_TREE_KNOBS
{
public:
    // The maximum number of tuples that the buffer can hold
    static const uint BUFFER_SIZE = 128;
};

template <typename _key, typename _value, typename _dual_tree_knobs=DUAL_TREE_KNOBS<_key, _value>,
            typename _betree_knobs=BeTree_Default_Knobs<_key, _value>, typename _compare=std::less<_key>>
class dual_tree
{
    // Left tree to accept unsorted input data.
    BeTree<_key, _value, _betree_knobs, _compare> *unsorted_tree;
    // Right tree to accept sorted input data.
    BeTree<_key, _value, _betree_knobs, _compare> *sorted_tree;

    uint sorted_size{0};

    uint unsorted_size{0};

public:

    // Default constructor, disable the buffer.
    dual_tree()
    {   
        unsorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>("manager", "./tree_dat", 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY);
        sorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>("manager", "./tree_dat", 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY);
    }

    // Deconstructor
    ~dual_tree()
    {
        delete sorted_tree;
        delete unsorted_tree;
    }

    uint sorted_tree_size() { return sorted_size;}

    uint unsorted_tree_size() { return unsorted_size;}

    bool insert(_key key, _value value)
    {
        if(sorted_size == 0)
        {
            // The first tuple is always inserted to the 
            sorted_tree->insert_to_tail_leaf(key, value);
            sorted_size += 1;
        }
        else 
        {
            if(key < sorted_tree->getMaximumKey())
            {
                unsorted_tree->insert(key, value);
                unsorted_size += 1;
            }
            else
            {
                sorted_tree->insert_to_tail_leaf(key, value);
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
};

#endif
