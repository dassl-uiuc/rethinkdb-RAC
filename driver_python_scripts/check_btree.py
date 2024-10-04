import re
from collections import defaultdict

list_of_files = ['btree_structure_0.txt', 'btree_structure_1.txt', 'btree_structure_2.txt']
# list_of_files = ['btree_structure_0.txt', 'btree_structure_1.txt']

def parse_btree_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    tree = {}
    current_parent = None
    node_stack = []
    level_count = defaultdict(int)
    
    for line in lines:
        depth = (len(line) - len(line.lstrip())) // 4
        level_count[depth] += 1
        
        match = re.search(r'(Internal Node|Leaf Node) \(ID: (\d+)\)', line)
        if match:
            node_type, node_id = match.groups()
            node_id = int(node_id)

            if depth > len(node_stack):
                if current_parent is not None:
                    node_stack.append(current_parent)

            while depth < len(node_stack):
                current_parent = node_stack.pop()

            if current_parent not in tree:
                tree[current_parent] = []

            tree[current_parent].append((node_type, node_id))

            if node_type == "Internal Node":
                current_parent = node_id
            else:
                current_parent = node_stack[-1] if node_stack else None

    return tree, level_count

def compare_trees(tree1, tree2):
    ids_different = 0
    number_of_children = 0
    leaf_ids_different = 0
    
    for parent in tree1:
        if parent not in tree2:
            ids_different += 1
            continue

        children1 = tree1[parent]
        children2 = tree2[parent]
        
        if len(children1) != len(children2):
            number_of_children += 1
            continue
        for i in range(len(children1)):
            if children1[i] != children2[i]:
                leaf_ids_different += 1

    print(f"ids_different: {ids_different}")
    print(f"number_of_children: {number_of_children}")
    print(f"leaf_ids_different: {leaf_ids_different}")
    return []

trees = []
level_counts = []
for tmp_file in list_of_files:
    file_path = f'/proj/rasl-PG0/kiran/rethink-rac/results/load3/{tmp_file}'
    tree, level_count = parse_btree_file(file_path)
    trees.append(tree)
    level_counts.append(level_count)

for i, tmp_file in enumerate(list_of_files):
    print(f"Node levels in {tmp_file}:")
    for level, count in sorted(level_counts[i].items()):
        print(f"Level {level}: {count} nodes")
    print('-' * 40)

for i in range(len(trees)):
    print(f"Comparing {list_of_files[i]} and {list_of_files[((i+1)%3)]}")
    compare_trees(trees[i], trees[((i+1)%3)])

    # print(f"Comparing {list_of_files[0]} and {list_of_files[1]}")
    # compare_trees(trees[0], trees[1])
    print('-' * 40)
