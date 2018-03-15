# tpcds-dags

## Each unique DAG is contained in a .txt file. There are 291 DAG's in total.

## The layout of each file is as follows: (explained using a sample DAG)

~~~~
# unique-dag-name
3 0 <-- (number of vertices, constant 0)
vertex-name-1 3000 0.05499302 0.01972656 0.00001600 0.00000002 0.00000000 10 <-- (unique vertex name, avg task time in msec, cpu, mem, network, disk-in, disk-out (each out of 1.0 per task), num tasks)  
vertex-name-2 4000 0.04342291 0.02187500 0.43275925 0.00049198 0.00040674 1 
vertex-name-3 5000 0.05395247 0.01972656 0.00000304 0.00000000 0.00000001 10 
2 <-- (number of edges in DAG)
vertex-name-1 vertex-name-3 scg <-- (source vertex, destination vertex, type of edge - typically scatter gather)
vertex-name-2 vertex-name-3 scg
~~~~


