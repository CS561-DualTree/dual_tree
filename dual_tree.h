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

/**
 * The buffer here not only holds tuples temporarily, but also detects whether
 * the tuples in it are sorted(descending or ascending).
 * When a new tuple comes, the dual tree system will first check whether the 
 * buffer is full. If it is, then the buffer will be flushed in to the real tree,
 * if it is not, then the new coming one will be offered to the buffer.
*/
template<typename _key, typename _value, typename _knobs = DUAL_TREE_KNOBS<_key, _value>,
  typename _compare = std::less<_key>>
class buffer 
{
    // Number of tuples in the buffer currently
    uint size;
    // The inner buffer that hold tuples
    std::pair<_key,_value> buf[_knobs::BUFFER_SIZE];
    
public:
    buffer() { size = 0;}

    std::pair<_key,_value> getTuple(uint index){return buf[index];}

    bool add_tuple(std::pair<_key, _value> new_tuple)
    {
        if(this->size == DUAL_TREE_KNOBS::BUFFER_SIZE) 
        {
            return false;    
        }
        else
        {
            buf[size++] = new_tuple;
            return true;
        }
    }

    /**
     * This method will flush tuples into corresponding tree, the flow of the method is:
     * 1. Scan the buffer once, if a tuple in the buffer falls in to the range of the @sorted_tree,
     *  insert it to the @unsorted_tree.
     * 2. If we find enough consecutive sorted tuples and non of them falls into the range of @sorted_tree,
     *  then we get a "sub-array". We may find several sub-arrays during scanning.
     * 3. Use a heap to store all subarrays, sort them by their lengths. 
     * 4. After the scanning, try to append subarrays to the @sorted_tree one by one from the heap.
     */
    void flush_tuples(BeTree<_key, _value> &unsorted_tree, BeTree<_key, _value> &sorted_tree) 
    {
        bool comp=[](std::pair<uint, uint> a, std::pair<uint, uint> b)
        {
            if(abs(a.first - a.second) >= abs(b.first - b.second)) 
            {
                return false;
            }
            else 
            {
                return true;
            }
        }
        std::priority_queue<std::pair<uint, uint>, std::vector<std::pair<uint, uint>>, 
            decltype(comp)> pq(comp);
        uint i = 0;
        while(buf[i].first <= sorted_tree.max_key && i < this->size)
        {
            unsorted_tree.insert(buf[i].first, buf[i].second);
            i += 1;
        }
        if(i < this->size)
        {
            uint begin = i;
            i += 1;
            while(i < this->size)
            {
                if(buf[i].first <= sorted_tree.max_key)
                {
                    pq.push(std::pair<uint, uint>(begin, i-1));
                    while(buf[i].first <= sorted_tree.max_key && i < this->size)
                    {
                        unsorted_tree.insert(buf[i].first, buf[i].second);
                        i += 1;
                    }
                    begin = i;
                    i += 1;
                } 
                else 
                {
                    if(buf[i-1].first > buf[i].first) 
                    {
                        pq.push(std::pair<uint, uint>(begin, i-1));
                        begin = i;
                    }
                    i += 1;
                }
            }
        }
        while(!pq.empty()) 
        {
            auto t = pq.pop();
            uint begin = t.first;
            uint end = t.second;
            // check the begin position of the subarray, during the insertion, newly added 
            //tuples may overlap with other subarrays
            if(buf[begin].first <= sorted_tree.max_key) 
            {
                uint j = t.first;
                while(buf[j].first <= sorted_tree.max_key && j <= end) 
                    unsorted_tree.insert(buf[j].first, buf[j].second);
                if(j > end) 
                    continue;
                begin = j;
            }
            // check the end position of the subarray for the same reason as the begin position.
            if(buf[end].first <= sorted_tree.max_key) 
            {
                uint j = end;
                while(buf[j].first <= sorted_tree.max_key && j >= begin) 
                    unsorted_tree.insert(buf[j].first, buf[j].second);
                if(j < begin)
                    continue;
                end = j;
            }
            sorted_tree.sorted_load(buf, begin, end);
        }
    }


};

template <typename _key, typename _value, typename _dual_tree_knobs=DUAL_TREE_KNOBS<_key, _value>,
            typename _betree_knobs=BeTree_Default_Knobs<_key, _value>, typename _compare=std::less<_key>>
class dual_tree
{
    // Left tree to accept unsorted input data.
    BeTree<_key, _value, _knobs, _compare> left_tree("manager", "./left_tree_dat", 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY);

    // Right tree to accept sorted input data.
    BeTree<_key, _value, _knobs, _compare> right_tree("manager", "./right_tree_dat",
        _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY);
    
    // For now, do not use the buffer.
    buffer<_key, _value, _dual_tree_knobs, _compare> *buf;

    // indicator of whether to use buffer to temporarily store incoming tuples.
    bool buffer_mode{ false };

    // The maximum value of the right tree
    _key right_tree_max;

    // The minimum value of the right tree
    _key right_tree_min;

    // The number of the tuples in the dual-tree
    ulong size;

public:

    // Default constructor, disable the buffer.
    dual_tree(){
        buf=nullptr;
        size = 0;
    }

    // Parameterized constructor 1
    dual_tree(bool enable_buffer): buffer_mode(enable_buffer)
    {
        if(buffer_mode)
            buf = new buffer<_key, _value, _dual_tree_knobs, _compare>();
        size = 0;
    }

    // Deconstructor
    ~dual_tree()
    {
        if(buffer_mode)
            delete buf;
    }


    bool insert_tuple(_key key, _value value)
    {
        if(buffer_mode)
        {
            if(!buf->add_tuple(std::pair<_key, _value>(key, value)))
            {
                buf->flush_tuples(this->left_tree, this->right_tree);
                right_tree_max = right_tree.getMaximumKey();
                right_tree_min = right_tree.getMinimumKey();
            }
            buf->add_tuple(std::pair<_key, _value>(key, value));
        }
        else
        {
            if(size == 0)
                right_tree.insert_to_tail_leaf(key, value);
            else 
            {
                if(key < right_tree_max)
                    left_tree.insert(key, value);
                else
                    right_tree.insert_to_tail_leaf(key, value);
            }
            right_tree_max = right_tree.getMaximumKey();
            right_tree_min = right_tree.getMinimumKey();
        }
        size += 1; 
    }

    bool query(_key key)
    {
        // 1. If enable buffer: fist search the buffer for the key.
        if(this->buffer_mode)
            for(int i = 0; i < buf->size; i++)
                if(buf->getTuple(i).first == key)
                    return true;
        // 2. Search both trees for the key.
        return left_tree.query(key) && right_tree.query(key);
    }

    std::vector<std::pair<_key, _value>> rangeQuery(_key low, _key high) 
    {
        // 1. TODO: range query when using buffer

        // 2. range query in both tree.
        std::vector<std::pair<_key, _value>> left_res = left_tree.rangeQuery(low, high);
        std::vector<std::pair<_key, _value>> right_res = right_tree.rangeQuery(low, high);
        left_res.insert(left_res.end(), right_res.begin(), right_res.end());
        return left_res;
    }
};

#endif
