import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ShortestPathDF').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


# dataset schema
DATASET_SCHEMA = types.StructType([
    types.StructField('source', types.StringType()),
    types.StructField('dest', types.StringType()),
])

# output schema
OUTPUT_SCHEMA = types.StructType([
    types.StructField('node', types.StringType()),
    types.StructField('source', types.StringType()),
    types.StructField('distance', types.IntegerType())
])


def main(inputs, outputs, source_node, dest_node):
    # read graph
    graph = spark.read.csv(inputs, sep=':', schema=DATASET_SCHEMA)

    # convert graph edges to dataframe rows
    all_edges = graph.select(graph['source'], functions.explode(functions.split(graph['dest'],' +')).alias('dest'))
    valid_edges = all_edges.where(all_edges['dest'] != '').cache()

    # setup temp_paths and know_paths variables
    temp_paths =  spark.createDataFrame([[source_node, '-', 0]], OUTPUT_SCHEMA)
    known_paths = spark.createDataFrame([[source_node, '-', 0]], OUTPUT_SCHEMA)

    # search for a path of maximum 6 edges
    for i in range(6):
        # join current iteration nodes with their corresponding edges to get next iteration nodes
        temp_paths = temp_paths.join(valid_edges, temp_paths['node'] == valid_edges['source'], 'inner') \
                                .select(valid_edges['dest'].alias('node'), temp_paths['node'].alias('source'), (temp_paths['distance'] + 1).alias('distance')) \
                                .cache()

        # find duplicate paths in each iteration
        duplicate_paths = temp_paths.join(known_paths, temp_paths['node'] == known_paths['node'], 'inner') \
                                    .select(temp_paths['node'], temp_paths['source'], temp_paths['distance'])

        # remove duplicate paths
        if (duplicate_paths.count() > 0):
            new_paths = temp_paths.subtract(duplicate_paths)
        else:
            new_paths = temp_paths.select('*')

        # add new paths to known_paths
        known_paths = known_paths.unionAll(new_paths).cache()

        # to write generated paths in files for debug
        known_paths.write.json(outputs + '/iter-' + str(i))

        # break out of loop when first path found
        if (known_paths.where(known_paths['node'] == dest_node).count() > 0):
            break

    # generating output path
    parent_node = dest_node
    output_path = [parent_node]

    # loop until we reach source node from dest node
    while (int(parent_node) != int(source_node)):
        # find node in paths
        edge = known_paths.where(known_paths['node'] == parent_node).collect()

        # exit if not found
        if (not edge):
            break

        # get source node
        parent_node = edge[0][1]

        # insert source node to output path
        output_path.insert(0, int(parent_node))

    # if there is only one item in output_path, there is not path available
    if (len(output_path) == 1 and source_node != dest_node):
        output_path = ['no path exists from node ' + str(source_node) + ' to node ' + str(dest_node) + '!']

    # write path to output
    print("\n\n\n------------------------")
    print("Path: %s" % (output_path))
    print("Path: [if any] is also generated in %s" % (outputs + '/path'))
    print("------------------------\n\n")

    # you can add .coalesce(1) to view path in a single file
    final_output = sc.parallelize(output_path)
    final_output.saveAsTextFile(outputs + '/path')


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    source_node = int(sys.argv[3])
    dest_node = int(sys.argv[4])

    main(inputs, outputs, source_node, dest_node)
