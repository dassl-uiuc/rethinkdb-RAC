// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "btree/depth_first_traversal.hpp"

#include "btree/internal_node.hpp"
#include "btree/leaf_node.hpp"
#include "btree/operations.hpp"
#include "concurrency/interruptor.hpp"
#include "rdb_protocol/profile.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <atomic>
#include <mutex>
#include <thread>

// Shared atomic counter across threads
std::atomic<int> access_counter(0);
// Mutex to ensure only one thread performs the operation
std::mutex mtx;
std::atomic<bool> tree_print(false);

void print_btree_structure(superblock_t *sb, const std::string &output_file);

scoped_key_value_t::scoped_key_value_t(const btree_key_t *_key,
                                       const void *_value,
                                       movable_t<counted_buf_lock_and_read_t> &&buf)
    : key_(_key), value_(_value), buf_(std::move(buf))
{
    guarantee(buf_.has());
}

scoped_key_value_t::scoped_key_value_t(scoped_key_value_t &&movee)
    : key_(movee.key_),
      value_(movee.value_),
      buf_(std::move(movee.buf_))
{
    movee.key_ = nullptr;
    movee.value_ = nullptr;
}

scoped_key_value_t::~scoped_key_value_t() {}

buf_parent_t scoped_key_value_t::expose_buf()
{
    guarantee(buf_.has());
    return buf_parent_t(&buf_->lock);
}

// Releases the hold on the buf_lock_t, after which key(), value(), and expose_buf()
// may not be used.
void scoped_key_value_t::reset()
{
    buf_.reset();
}

/* Returns `true` if we reached the end of the subtree or range, and `false` if
`cb->handle_value()` returned `false`. */
continue_bool_t btree_depth_first_traversal(
    counted_t<counted_buf_lock_and_read_t> block,
    const key_range_t &range,
    depth_first_traversal_callback_t *cb,
    access_t access,
    direction_t direction,
    const btree_key_t *left_excl_or_null,
    const btree_key_t *right_incl,
    signal_t *interruptor);

continue_bool_t btree_depth_first_traversal(
    superblock_t *superblock,
    const key_range_t &range,
    depth_first_traversal_callback_t *cb,
    access_t access,
    direction_t direction,
    release_superblock_t release_superblock,
    signal_t *interruptor)
{
    if (range.is_empty())
    {
        return continue_bool_t::CONTINUE;
    }

    // std::cout << "btree_depth_first_traversal_1\n";
    print_btree_structure(superblock, "/mydata/btree_structure.txt");

    const btree_key_t *left_excl_or_null;
    store_key_t left_excl_buf(range.left);
    if (left_excl_buf.decrement())
    {
        left_excl_or_null = left_excl_buf.btree_key();
    }
    else
    {
        left_excl_or_null = nullptr;
    }
    store_key_t right_incl_buf;
    if (range.right.unbounded)
    {
        right_incl_buf = store_key_t::max();
    }
    else
    {
        right_incl_buf = range.right.key();
        bool ok = right_incl_buf.decrement();
        guarantee(ok, "this is impossible because we checked range is not empty");
    }

    block_id_t root_block_id = superblock->get_root_block_id();
    if (root_block_id == NULL_BLOCK_ID)
    {
        if (release_superblock == release_superblock_t::RELEASE)
        {
            superblock->release();
        }
        return cb->handle_empty(left_excl_or_null, right_incl_buf.btree_key(),
                                interruptor);
    }
    else
    {
        counted_t<counted_buf_lock_and_read_t> root_block;
        {
            // We know that `superblock` is already read-acquired because we call
            // get_block_id() above -- so `starter` won't measure time waiting for
            // the parent to become acquired.
            PROFILE_STARTER_IF_ENABLED(
                cb->get_trace() != nullptr,
                "Acquire block for read.",
                cb->get_trace());
            root_block = make_counted<counted_buf_lock_and_read_t>(
                superblock->expose_buf(), root_block_id, access);
            if (release_superblock == release_superblock_t::RELEASE)
            {
                // Release the superblock ASAP because that's good.
                superblock->release();
            }
            // Wait for read acquisition of the root block, so that `starter`'s
            // profiling information is correct.
            wait_interruptible(root_block->lock.read_acq_signal(), interruptor);
        }

        return btree_depth_first_traversal(
            std::move(root_block), range, cb, access, direction,
            left_excl_or_null, right_incl_buf.btree_key(), interruptor);
    }
}

void get_child_key_range(
    const internal_node_t *inode,
    int child_index,
    /* `parent_*` describe the intersection of `inode`'s range and the range of keys
    that the traversal is interested in */
    const btree_key_t *parent_left_excl_or_null,
    const btree_key_t *parent_right_incl,
    const btree_key_t **left_excl_or_null_out,
    const btree_key_t **right_incl_out)
{
    const btree_internal_pair *pair =
        internal_node::get_pair_by_index(inode, child_index);
    if (child_index != inode->npairs - 1)
    {
        rassert(child_index < inode->npairs - 1);
        if (btree_key_cmp(&pair->key, parent_right_incl) < 0)
        {
            *right_incl_out = &pair->key;
        }
        else
        {
            *right_incl_out = parent_right_incl;
        }
    }
    else
    {
        *right_incl_out = parent_right_incl;
    }

    if (child_index > 0)
    {
        const btree_internal_pair *left_neighbor =
            internal_node::get_pair_by_index(inode, child_index - 1);
        if (parent_left_excl_or_null == nullptr ||
            btree_key_cmp(&left_neighbor->key, parent_left_excl_or_null) > 0)
        {
            *left_excl_or_null_out = &left_neighbor->key;
        }
        else
        {
            *left_excl_or_null_out = parent_left_excl_or_null;
        }
    }
    else
    {
        *left_excl_or_null_out = parent_left_excl_or_null;
    }
}

continue_bool_t btree_depth_first_traversal(
    counted_t<counted_buf_lock_and_read_t> block,
    const key_range_t &range,
    depth_first_traversal_callback_t *cb,
    access_t access,
    direction_t direction,
    const btree_key_t *left_excl_or_null,
    const btree_key_t *right_incl,
    signal_t *interruptor)
{
    bool skip;

    // std::cout << "btree_depth_first_traversal_2\n";
    if (continue_bool_t::ABORT == cb->filter_range_ts(
                                      left_excl_or_null, right_incl, block->lock.get_recency(), interruptor,
                                      &skip))
    {
        return continue_bool_t::ABORT;
    }
    if (skip)
    {
        return continue_bool_t::CONTINUE;
    }
    block->read.init(new buf_read_t(&block->lock));
    const node_t *node = static_cast<const node_t *>(block->read->get_data_read());
    if (node::is_internal(node))
    {
        if (continue_bool_t::ABORT == cb->handle_pre_internal(
                                          block, left_excl_or_null, right_incl, interruptor))
        {
            return continue_bool_t::ABORT;
        }
        const internal_node_t *inode = reinterpret_cast<const internal_node_t *>(node);
        int start_index = internal_node::get_offset_index(inode, range.left.btree_key());
        int end_index;
        if (range.right.unbounded)
        {
            end_index = inode->npairs;
        }
        else
        {
            store_key_t r = range.right.key();
            r.decrement();
            end_index = internal_node::get_offset_index(inode, r.btree_key()) + 1;
        }
        for (int i = 0; i < end_index - start_index; ++i)
        {
            int true_index = (direction == FORWARD ? start_index + i : (end_index - 1) - i);
            const btree_internal_pair *pair = internal_node::get_pair_by_index(inode, true_index);

            // Get the child key range
            const btree_key_t *child_left_excl_or_null;
            const btree_key_t *child_right_incl;
            get_child_key_range(inode, true_index,
                                left_excl_or_null, right_incl,
                                &child_left_excl_or_null, &child_right_incl);

            if (continue_bool_t::ABORT == cb->filter_range(
                                              child_left_excl_or_null, child_right_incl, interruptor, &skip))
            {
                return continue_bool_t::ABORT;
            }
            if (!skip)
            {
                counted_t<counted_buf_lock_and_read_t> lock;
                {
                    PROFILE_STARTER_IF_ENABLED(
                        cb->get_trace() != nullptr,
                        "Acquire block for read.",
                        cb->get_trace());
                    lock = make_counted<counted_buf_lock_and_read_t>(
                        &block->lock, pair->lnode, access);
                    wait_interruptible(lock->lock.read_acq_signal(), interruptor);
                }
                if (continue_bool_t::ABORT == btree_depth_first_traversal(
                                                  std::move(lock), range, cb, access, direction,
                                                  child_left_excl_or_null, child_right_incl, interruptor))
                {
                    return continue_bool_t::ABORT;
                }
            }
        }
        return continue_bool_t::CONTINUE;
    }
    else
    {
        if (continue_bool_t::ABORT == cb->handle_pre_leaf(
                                          block, left_excl_or_null, right_incl, interruptor, &skip))
        {
            return continue_bool_t::ABORT;
        }
        if (skip)
        {
            return continue_bool_t::CONTINUE;
        }

        const leaf_node_t *lnode = reinterpret_cast<const leaf_node_t *>(node);
        const btree_key_t *key;

        if (direction == FORWARD)
        {
            for (auto it = leaf::inclusive_lower_bound(range.left.btree_key(), *lnode);
                 it != leaf::end(*lnode); ++it)
            {
                key = (*it).first;
                // range.right is exclusive
                if (!range.right.unbounded &&
                    btree_key_cmp(key, range.right.key().btree_key()) >= 0)
                {
                    break;
                }
                if (continue_bool_t::ABORT == cb->handle_pair(
                                                  scoped_key_value_t(
                                                      key, (*it).second,
                                                      movable_t<counted_buf_lock_and_read_t>(block)),
                                                  interruptor))
                {
                    return continue_bool_t::ABORT;
                }
            }
        }
        else
        {
            leaf_node_t::reverse_iterator it;
            if (range.right.unbounded)
            {
                it = leaf::rbegin(*lnode);
            }
            else
            {
                it = leaf::exclusive_upper_bound(range.right.key().btree_key(), *lnode);
            }
            for (/* assignment above */; it != leaf::rend(*lnode); ++it)
            {
                key = (*it).first;

                // range.left is inclusive
                if (btree_key_cmp(key, range.left.btree_key()) < 0)
                {
                    break;
                }

                if (continue_bool_t::ABORT == cb->handle_pair(
                                                  scoped_key_value_t(
                                                      key, (*it).second,
                                                      movable_t<counted_buf_lock_and_read_t>(block)),
                                                  interruptor))
                {
                    return continue_bool_t::ABORT;
                }
            }
        }
        return continue_bool_t::CONTINUE;
    }
}

// Function to print details of a node to the output file.
void print_node_info(std::ofstream &out, block_id_t block_id, bool is_internal, int depth, const node_t *node)
{
    // Add indentation based on depth level for better visualization.
    for (int i = 0; i < depth; ++i)
    {
        out << "    "; // Indent with 4 spaces per level.
    }

    // Print whether the node is internal or leaf.
    if (is_internal)
    {
        out << "|-- Internal Node (ID: " << block_id << ")" << std::endl;
    }
    else
    {
        out << "|-- Leaf Node (ID: " << block_id << ")" << std::endl;

        // Write leaf node details to a separate file.
        FILE *fp = fopen("leaf_node_details.txt", "a");
        if (fp)
        {
            fprintf(fp, "Leaf Node ID: %llu\n", static_cast<unsigned long long>(block_id));
            // If more details are available from the node, print them here.
            fclose(fp);
        }
        else
        {
            std::cerr << "Error: Could not open leaf_node_details.txt" << std::endl;
        }
    }
}

// Recursive function to traverse the B-tree and print all internal and leaf nodes.
void traverse_btree_and_print(std::ofstream &out, buf_lock_t *buf, int depth)
{
    // Read the current node from the buffer.
    buf_read_t buf_read(buf);
    const node_t *node = static_cast<const node_t *>(buf_read.get_data_read());

    // Check if the node is an internal node or a leaf node.
    bool is_internal = node::is_internal(node);

    // Print the node information.
    print_node_info(out, buf->block_id(), is_internal, depth, node);

    // If the node is an internal node, traverse its children recursively.
    if (is_internal)
    {
        const internal_node_t *internal_node = reinterpret_cast<const internal_node_t *>(node);

        // Iterate through all child nodes of the internal node.
        for (int pair_idx = 0; pair_idx < internal_node->npairs; ++pair_idx)
        {
            block_id_t child_id = internal_node::get_pair_by_index(internal_node, pair_idx)->lnode;

            // Acquire the child node buffer.
            buf_lock_t child_buf(buf, child_id, access_t::read);

            // Recursively traverse the child node.
            traverse_btree_and_print(out, &child_buf, depth + 1); // Increase depth for child nodes.
        }
    }
}

// Entry function to print the entire B-tree structure including all internal and leaf nodes.
void print_btree_structure(superblock_t *sb, const std::string &output_file)
{
    // Open the output file for appending the B-tree structure.
    std::ofstream out(output_file, std::ios::app);
    if (!out.is_open())
    {
        std::cerr << "Error: Unable to open file: " << output_file << std::endl;
        return;
    }
    int current_count = ++access_counter;
    if (current_count % 1000 == 0 && !(tree_print.load()))
    {
        std::cout << "Access counter: " << current_count << std::endl;
    }

    if (current_count >= 10000 && !(tree_print.load()))
    {
        tree_print.store(true);
        std::lock_guard<std::mutex> lock(mtx);
        std::cout << "Access counter: " << std::endl;

        // Acquire the root of the B-tree.
        std::cout << "Acquiring root of the B-tree..." << std::endl;
        buf_lock_t root_buf = get_root_read_no_sizer(sb); // Removed the sizer

        // Traverse the tree starting from the root and print all nodes in a tree-like structure.
        std::cout << "Printing B-tree structure..." << std::endl;
        out << "B-tree Structure:" << std::endl;
        traverse_btree_and_print(out, &root_buf, 0); // Start at depth 0 (root level).

        // Close the output file.
        out.close();
        std::cout << "B-tree structure written to file: " << output_file << std::endl;
    }
}
