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

    // The initial tolerance threshold, determine whether the key of the newly added tuple is too far from the previous
    //tuple in the sorted tree. If set it to 0, the dual tree will disable the outlier detector.
    static const uint INIT_TOLERANCE_FACTOR = 200;

    // The minimum value of the TOLERANCE_FACTOR, when the value of tolerance factor is too small, 
    //most tuples will be inserted to the unsorted tree, thus we need to keep the value from too small.
    //This value should be less than @INIT_TOLERANCE_FACTOR
    static constexpr float MIN_TOLERANCE_FACTOR = 50;

    // The expected average distance between any two consecutive tuples in the sorted tree. This
    //tuning knob helps to modify the tolerance factor in the outlier detector. If it is less or equal to 
    //1, then the tolerance factor becomes a constant.
    static constexpr float EXPECTED_AVG_DISTANCE = 2.5;

  
    // When it is true, tuples that are less than maximum key of the sorted tree and are greater than minimum key of
    //the tail leaf of the sorted tree will be inserted to the tail leaf. If it is false, then only tuples that are 
    //greater than maximum key of the sorted tree is allowed to inserted into the sorted tree.
    static const bool ALLOW_SORTED_TREE_INSERTION = true;

    // Query buffer size for determine which tree to query first statistically. When it is set to zero, we simply make the decision
    // based on the size of sorted and unsorted trees. When it is set to a non-zero value, we compare the number of top QUERY_BUFFER_SIZE
    // queries and query the most queried tree first.
    static const uint QUERY_BUFFER_SIZE = 10;
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


// This class is used to detector outlier in the newly inserted tuples with respect to the sorted tree
template<typename _key>
class outlier_detector
{
private:
    // The default value of @average_distance.
    static constexpr float INIT_AVG = -1;

    // The decrease ratio of @tolerance_factor
    static constexpr float DECREASE_STEP = 0.90;

    // The large decrease step of @tolerance(in ratio);
    static constexpr float LARGE_DECREASE_STEP = 0.5;

    // The decrease ratio of @tolerance_factor
    static constexpr float INCREASE_STEP = 1.05;

    // The maximum multiple of the difference between @avg_distance and @expected_avg_distance.
    //If the distance exceed the multiple, then @LARGE_DECREASE_STEP instead of @DECREASE_STEP is used.
    static const short MAX_MULTIPLE_DIFF = 20;

    // The maximum difference between @avg_distance and @expected_avg_distance. If the difference 
    //between those two variables exceeds this value, the @tolerance_factor need to decrease. This
    //variable should be greater than 0;
    static constexpr float MAX_DISTANCE_DIFF = 0.5;

    // The minimum value of the INIT_TOLERANCE_FACTOR, when the value of tolerance factor is too small, 
    //most tuples will be inserted to the unsorted tree, thus we need to keep the value from too small
    const float min_tolerance_factor;

    // The average distance between any two consecutive keys of tuples in the sorted tree.
    float avg_distance;

    // The expected average distance.
    const float expected_avg_distance;

    // The tolerance threshold, determine whether the key of the newly added tuple is too far from the previous
    //tuple in the sorted tree. When the distance is greater than @avg_distance * @tolerance_factor,
    //the newly added tuple should be added to the unsorted tree. Should be greater than 0.
    float tolerance_factor;

    // The most recently added key of the sorted tree;
    _key previous_key;

public:

    outlier_detector(float tolerance_factor, float min_tolerance_factor, float expected_avg_distance=1):
        tolerance_factor(tolerance_factor), expected_avg_distance(expected_avg_distance), 
        min_tolerance_factor(min_tolerance_factor), avg_distance(-1){}

    double get_avg_distance() {return avg_distance;}

    double get_tolerance_factor() { return tolerance_factor; }

    /**
     *  Check whether a key is an outlier with repsect to the sorted tree.
     * @param new_key The pending new key
     * @param num_tuples Size of the sorted tree.
    */
    bool is_outlier(const _key& new_key, const uint& num_tuples)
    {
        if(tolerance_factor <= 0)
        {
            // If the tolerance_factor is less or equal to 0, then stop use it.
            return false;
        }
        if(avg_distance == INIT_AVG)
        {
            if(num_tuples == 0){
                // no tuple has been added to the sorted tree.
                previous_key = new_key;
            }
            else
            {
                assert(num_tuples == 1);
                avg_distance = new_key - previous_key;
                previous_key = new_key;
            }
            return false;
        }
        else
        {
            double new_distance = new_key - previous_key;
            if(new_key - previous_key >= avg_distance * tolerance_factor)
            {
                return true;
            }
            else
            {
                // update the average;
                avg_distance = ((double)(avg_distance * (num_tuples-1) + new_key - previous_key)) /
                    (num_tuples);
                previous_key = new_key;

                // adjust the tolerance factor
                if(expected_avg_distance > 1)
                {
                    if(expected_avg_distance * MAX_MULTIPLE_DIFF < avg_distance)
                        tolerance_factor *= LARGE_DECREASE_STEP;
                    else
                    {
                        if(avg_distance - MAX_DISTANCE_DIFF > expected_avg_distance)
                            tolerance_factor *= DECREASE_STEP;
                        else if(expected_avg_distance - MAX_DISTANCE_DIFF > avg_distance)
                            tolerance_factor *= INCREASE_STEP;
                    }
                }    
                if(tolerance_factor < min_tolerance_factor)
                {
                    tolerance_factor = min_tolerance_factor;
                }

                return false;
            }
        }
    }

    /**
     * This function is only called after inserting(not appending) a tuple to the tail leaf of the
     * sorted tree. 
    */
    void update_avg_distance(const int& num_tuples)
    {
        if(tolerance_factor > 0)
        {
            avg_distance = ((double)(avg_distance * (num_tuples-1) + 1) / num_tuples);
        }
    }

};

template<typename T>
class MRU_query_buffer
{

private:

    // put 1 into the buffer if current query can be answered in the sorted tree
    int sorted = 0;

    // put 0 into the buffer if current query can be answered in the unsorted tree
    int unsorted = 1;

    // buffer that holds the latest buffer_size number of queried tree in chronological order
    int* buffer;

    // size of buffer we used for predicting next query
    uint buffer_size;

    // index pointer that points to the next position in the buffer array that need to be poped
    int buffer_ptr;

    // number of sorted tree query in the latest buffer_size number of queries
    int sorted_counter;
    
    // number of unsorted tree query in the latest buffer_size number of queries
    int unsorted_counter;

public:

    // constructor
    MRU_query_buffer(uint size)
    {
        buffer_size = size;
        buffer = new int[buffer_size] { -1 };
        buffer_ptr = 0;
        sorted_counter = 0;
        unsorted_counter = 0;
    }

    void update_ptr()
    {
        buffer_ptr = (buffer_ptr + 1) % buffer_size;
    }

    void update_buffer(uint next)
    {
        // std::cout << "before update buffer: " << buffer << " | sorted counter: " << sorted_counter << " | unsorted counter: " << unsorted_counter << " | buffer pointer: " << buffer_ptr << std::endl;
        uint poped = buffer[buffer_ptr];

        sorted_counter -= poped == sorted;
        unsorted_counter -= poped == unsorted;

        buffer[buffer_ptr] = next;

        sorted_counter += next == sorted;
        unsorted_counter += next == unsorted;

        update_ptr();
        // std::cout << "before update buffer: " << buffer << " | sorted counter: " << sorted_counter << " | unsorted counter: " << unsorted_counter << " | buffer pointer: " << buffer_ptr << std::endl;
    }

    int predict()
    {
        return unsorted_counter > sorted_counter;
    }

    bool buffer_full()
    {
        return unsorted_counter + sorted_counter == buffer_size && buffer_size != 0;
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

    _key sorted_min;

    _key sorted_max;

    _key unsorted_min;

    _key unsorted_max;

    std::priority_queue<std::pair<_key, _value>, std::vector<std::pair<_key, _value>>, 
        key_comparator<_key, _value>> *heap_buf;

    outlier_detector<_key> *od;

    MRU_query_buffer<_key> *query_buf;

    template <class T, class S, class C>
    S& container(std::priority_queue<T, S, C>& q)
    {
        struct HackedQueue : private std::priority_queue<T, S, C>
        {
            static S& Container(std::priority_queue<T, S, C>& q)
            {
                return q.*&HackedQueue::c;
            }
        };
        return HackedQueue::Container(q);
    }


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

        sorted_min, unsorted_min = INT_MAX;
        sorted_max, unsorted_max = INT_MIN;

        if(_dual_tree_knobs::HEAP_SIZE != 0) 
            heap_buf = new std::priority_queue<std::pair<_key, _value>, std::vector<std::pair<_key, _value>>,
                key_comparator<_key, _value>>();
        od = new outlier_detector<_key>(_dual_tree_knobs::INIT_TOLERANCE_FACTOR, _dual_tree_knobs::MIN_TOLERANCE_FACTOR, 
             _dual_tree_knobs::EXPECTED_AVG_DISTANCE);
        query_buf = new MRU_query_buffer<_key>(_dual_tree_knobs::QUERY_BUFFER_SIZE);
    }

    // Deconstructor
    ~dual_tree()
    {
        delete sorted_tree;
        delete unsorted_tree;
        if(_dual_tree_knobs::HEAP_SIZE != 0)
            delete heap_buf;
        delete od;
        delete query_buf;
    }

    uint sorted_tree_size() { return sorted_size;}

    uint unsorted_tree_size() { return unsorted_size;}

    _key sorted_tree_min() { return sorted_min; }

    _key sorted_tree_max() { return sorted_max; }

    _key unsorted_tree_min() { return unsorted_min; }

    _key unsorted_tree_max() { return unsorted_max; }

    void update_domain_size(bool in_sorted, _key value)
    {
        if (in_sorted)
        {
            sorted_max = std::max(value, sorted_max);
            sorted_min = std::min(value, sorted_min);
        } else
        {
            unsorted_max = std::max(value, unsorted_max);
            unsorted_min = std::min(value, unsorted_min);
        }
    }

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
            sorted_tree->insert_to_tail_leaf(inserted_key, inserted_value, true);
            update_domain_size(true, key);
            sorted_size += 1;
        }
        else 
        {
            _key lower_bound = _dual_tree_knobs::ALLOW_SORTED_TREE_INSERTION ? sorted_tree->get_tail_leaf_minimum_key():
                sorted_tree->getMaximumKey();
            if(inserted_key < lower_bound ||
                (inserted_key > sorted_tree->getMaximumKey() && od->is_outlier(inserted_key, sorted_size)))
            {
                unsorted_tree->insert(inserted_key, inserted_value);
                update_domain_size(false, key);
                unsorted_size += 1;
            }
            else
            {
                // When _dual_tree_knobs::ALLOW_SORTED_TREE_INSERTION is false, @append is always true.
                bool append = inserted_key >= sorted_tree->getMaximumKey();
                sorted_tree->insert_to_tail_leaf(inserted_key, inserted_value, append);
                update_domain_size(true, key);
                sorted_size += 1;
                if(!append)
                    od->update_avg_distance(sorted_size);
            }
        }
        return true;
    }

    bool query(_key key)
    {
        bool found;
        // Search the one with more tuples at first.
        if(sorted_size > unsorted_size)
        {
            found = sorted_tree->query(key) || unsorted_tree->query(key);
        }
        else
        {
            found = unsorted_tree->query(key) || sorted_tree->query(key);
        }

        if (found) {
            return true;
        }

        // Search the buffer
        std::vector<std::pair<_key, _value>> &tmp = container(*(this->heap_buf));
        for(typename std::vector<std::pair<_key, _value>>::iterator it = tmp.begin(); it !=tmp.end(); it++)
        {
            if((*it).first == key) 
            {
                return true;
            }
        }

        return found;
        
    }

    // helper function used for parallel query, query a single tree
    // query sorted tree if tree == 0, query unsorted tree otherwise
    void querySingleTree(int tree, _key key, std::promise<bool> &promiseObj) {
        if (tree == 0) {
            promiseObj.set_value(sorted_tree->query(key));
        } else {
            promiseObj.set_value(unsorted_tree->query(key));
        }
    }

    // query both trees in parallel
    bool parallelQuery(_key key) {

        // fire up a thread to query the sorted tree
        std::promise<bool> sortedPromise;
        std::future<bool> sortedFuture = sortedPromise.get_future();
        std::thread sortedQuery(&dual_tree::querySingleTree, this, 0, key, std::ref(sortedPromise));

        // fire up another thread to query the unsorted tree
        std::promise<bool> unsortedPromise;
        std::future<bool> unsortedFuture = unsortedPromise.get_future();
        std::thread unsortedQuery(&dual_tree::querySingleTree, this, 1, key, std::ref(unsortedPromise));

        // wait for the thread to finish
        sortedQuery.join();
        unsortedQuery.join();

        return sortedFuture.get() || unsortedFuture.get();
    }

    bool MRU_query(_key key)
    {
        
        bool found_in_tree;

        if (query_buf->buffer_full())
        {

            if (query_buf->predict())
            {
                bool found = unsorted_tree->query(key);
                query_buf->update_buffer(found);
                found_in_tree = found || sorted_tree->query(key);
            } else 
            {
                bool found = sorted_tree->query(key);
                query_buf->update_buffer(!found);
                found_in_tree = found || unsorted_tree->query(key);
            }

            if (found_in_tree)
            {
                return true;
            }

        } else 
        {
            if (sorted_size > unsorted_size)
            {
                bool found = sorted_tree->query(key);
                query_buf->update_buffer(!found);
                found_in_tree = found || unsorted_tree->query(key);
            } else
            {
                bool found = unsorted_tree->query(key);
                query_buf->update_buffer(found);
                found_in_tree = found || sorted_tree->query(key);
            }
            
            if (found_in_tree)
            {
                return true;
            }

        }

        std::vector<std::pair<_key, _value>> &tmp = container(*(this->heap_buf));
        for(typename std::vector<std::pair<_key, _value>>::iterator it = tmp.begin(); it !=tmp.end(); it++)
        {
            if((*it).first == key) 
            {
                return true;
            }
        }

        return found_in_tree;
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
        std::cout << "Sorted Tree: Maximum value = " << sorted_tree->getMaximumKey() << std::endl;
        std::cout << "Sorted Tree: Minimum value = " << sorted_tree->getMinimumKey() << std::endl;
        std::cout << "Sorted Tree: Average Distance between tuples = " << this->od->get_avg_distance() << std::endl;
        std::cout << "Sorted Tree: Tolerance factor = " << this->od->get_tolerance_factor() << std::endl;

        unsorted_tree->fanout();
        std::cout << "Unsorted Tree: number of splitting leaves = " << unsorted_tree->traits.leaf_splits
            << std::endl;
        std::cout << "Unsorted Tree: number of splitting internal nodes = " << 
            unsorted_tree->traits.internal_splits << std::endl;
        std::cout << "Unsorted Tree: number of leaves = " << unsorted_tree->traits.num_leaf_nodes 
            << std::endl;
        std::cout << "Unsorted Tree: number of internal nodes = " << 
            unsorted_tree->traits.num_internal_nodes << std::endl;
        std::cout << "Unsorted Tree: Maximum value = " << unsorted_tree->getMaximumKey() << std::endl;
        std::cout << "Unsorted Tree: Minimum value = " << unsorted_tree->getMinimumKey() << std::endl;
        
        std::cout << "Heap buf size = " << this->heap_buf->size() << std::endl;
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
        std::cout << "Initial outlier tolerance factor = " << _dual_tree_knobs::INIT_TOLERANCE_FACTOR << std::endl;
        std::cout << "Minimum outlier tolerance factor = " << _dual_tree_knobs::MIN_TOLERANCE_FACTOR << std::endl;
        std::cout << "Expected average distance = " << _dual_tree_knobs::EXPECTED_AVG_DISTANCE << std::endl;
        std::cout << "Allow sorted tree insertion = " << _dual_tree_knobs::ALLOW_SORTED_TREE_INSERTION << std::endl;
        std::cout << "Query Buffer Size = " << _dual_tree_knobs::QUERY_BUFFER_SIZE << std::endl;

        std::cout << "--------------------------------------------------------------------------" << std::endl;
    }

    unsigned long long get_sorted_tree_true_size() {return sorted_tree->getNumKeys();}

    unsigned long long get_unsorted_tree_true_size() {return unsorted_tree->getNumKeys();}};

#endif
