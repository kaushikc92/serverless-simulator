import org.omg.PortableInterceptor.INACTIVE;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;



public class Main {

    public static void main(String[] args) {
        Cluster cluster = new Cluster(10);
        List<DAG> DAGs = new ArrayList<>();

        //DAGs.add(new DAG(17, 0, 0, 0));
        //DAGs.add(new DAG(17, 0, 0, 1));
        //DAGs.add(new DAG(0)); // Read a random DAG

        for (int i = 0; i < 20; i++)
        {
            DAGs.add(new DAG(i, i));
        }


        //CriticalPathScheduler scheduler = new CriticalPathScheduler(DAGs, cluster);
        SJFScheduler scheduler = new SJFScheduler(DAGs, cluster);
        double SJFexecTime = scheduler.schedule();
        System.out.println("SJF Scheduler ACT: " + SJFexecTime);
        //scheduler.printStatistics();



        SDAGFScheduler sdagfScheduler = new SDAGFScheduler(DAGs, new Cluster(10));
        double SDFexecTime = sdagfScheduler.schedule();
        System.out.println("SDF + SJF Scheduler ACT: " + SDFexecTime);

        System.out.println("Speed up(SDF + SJF -> SJF): " + SJFexecTime / SDFexecTime);


        FIFODAGScheduler fifodagScheduler = new FIFODAGScheduler(DAGs, cluster);
        double FIFOexecTime = fifodagScheduler.schedule();
        System.out.println("FIFO Scheduler ACT: " + FIFOexecTime);

        System.out.println("Speed up(SDF + SJF -> FIFO): " + FIFOexecTime / SDFexecTime);


        SDFSpreadScheduler sdfSpreadScheduler = new SDFSpreadScheduler(DAGs, new Cluster(10));
        double SDFSpreadexecTime = sdfSpreadScheduler.schedule();
        System.out.println("SDF + SJF + Spread Scheduler ACT: " + SDFSpreadexecTime);

        System.out.println("Speed up(SDF + SJF + Spread -> SDF + SJF): " + SDFexecTime / SDFSpreadexecTime);
    }

}






class Cluster {
    Map<Integer, CPUNode> allCPUs = new HashMap<>();
    double totalCapacityFromDB =    16;
    double totalCapacityToDB = 16;
    public Cluster(int numOfCPUs)
    {
        for (int i = 0; i < numOfCPUs; i++)
        {
            allCPUs.put(i, new CPUNode());
        }
    }
}

class CPUNode {

    int NumOfCores = 2;
    int computationPower = 2;
    double bandwidthFromDB = 2;
    double bandwidthToDB = 2;

    List<DAGNodeTime> exec = new LinkedList<>();
    LinkedList<DAGEdgeTime> input = new LinkedList<>();
    LinkedList<DAGEdgeTime> output = new LinkedList<>();
    List<DAGNode> next = new LinkedList<>(); //Waiting nodes

    List<DAGEdgeInterval> inputHistory = new LinkedList<>();
    List<DAGEdgeInterval> outputHistory = new LinkedList<>();
    List<DAGNodeInterval> execHistory = new LinkedList<>();

}

class DAGNodeInterval{
    DAGNode dagNode;
    double startTime;
    double endTime;
    public DAGNodeInterval(DAGNode dagNode, double startTime, double endTime)
    {
        this.dagNode = dagNode;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}

class DAGEdgeInterval{
    DAGEdge dagEdge;
    double startTime;
    double endTime;
    public DAGEdgeInterval(DAGEdge dagEdge, double startTime, double endTime)
    {
        this.dagEdge = dagEdge;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}

class DAGNodeTime{
    DAGNode dagNode;
    double timeStamp;
    public DAGNodeTime(DAGNode dagNode, double timeStamp)
    {
        this.dagNode = dagNode;
        this.timeStamp = timeStamp;
    }
}

class DAGEdgeTime{
    DAGEdge dagEdge;
    double timeStamp;
    public DAGEdgeTime(DAGEdge dagEdge, double timeStamp)
    {
        this.dagEdge = dagEdge;
        this.timeStamp = timeStamp;
    }
}

class DAGNode {
    int id;
    double executingTime;
    double score;
    int DAGid;
    DAG dag;

    List<DAGEdge> children = new ArrayList<>();
    List<DAGEdge> parents = new ArrayList<>();


    public DAGNode(int id, double executingTime, int DAGid, DAG dag)
    {
        this.id = id;
        this.executingTime = executingTime;
        this.DAGid = DAGid;
        this.dag =dag;
    }
}

class DAGEdge{
    double amountOfData;
    double amountOfDataBU;
    DAGNode srcId;
    DAGNode destId;
    double score;
    DAG dag;

    boolean writeToDB = false;
    boolean readFromDB = false;
    public DAGEdge(double amountOfData, DAGNode srcId, DAGNode destId, DAG dag)
    {
        this.amountOfData = amountOfData;
        this.amountOfDataBU = amountOfData;
        this.srcId = srcId;
        this.destId = destId;
        this.dag = dag;
    }
}

class DAG{
    int id;
    double weight = 1;
    double timeStamp;
    Map<Integer, DAGNode> allNodes = new HashMap<>();

    public DAG(int queryNum, int index, double timeStamp, int id)
    {
        this.id = id;
        Random random = new Random();

        List<Node> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();

        int[][] queryList = {{17, 16}, {18, 23}, {25, 13}, {29, 13}, {46, 15}, {49, 7}, {56, 63}, {60, 63}, {80, 49}, {91, 19}};

        if (queryNum == -1)
        {
            queryNum = queryList[random.nextInt(10)][0];
        }

        int queryI;

        for (queryI = 0; queryI < queryList.length; queryI++)
        {
            if (queryList[queryI][0] == queryNum) {
                break;
            }
        }

        if (queryI == queryList.length)
        {
            System.out.println("Wrong query number");
            return;
        }


        if (index == -1)
        {
            index = random.nextInt(queryList[queryI][1] + 1);
        }

        if (queryList[queryI][1] < index)
        {
            System.out.println("Wrong query index number");
            return;
        }


        String fileName = "query" + queryNum + "-" + index + ".txt";
        String path = "./tpcds-dags/";
        String line = null;

        Map<Integer, Integer> numOfTasks = new HashMap<>();

        try {
            FileReader fileReader = new FileReader(path + fileName);
            System.out.println("Read file: " + fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            line = bufferedReader.readLine(); //skip headline

            line = bufferedReader.readLine().split(" ")[0];
            int numOfNodes = Integer.parseInt(line);

            Map<String, Integer> map = new HashMap<>();
            for (int i = 0; i < numOfNodes; i++)
            {
                String[] fields = bufferedReader.readLine().split(" ");
                map.put(fields[0], i);
                nodes.add(new Node(i, Integer.parseInt(fields[1])));
                numOfTasks.put(i, Integer.parseInt(fields[fields.length - 1]));
            }

            line = bufferedReader.readLine();
            int numOfEdges = Integer.parseInt(line);

            for (int i = 0; i < numOfEdges; i++)
            {
                String[] fields = bufferedReader.readLine().split(" ");
                edges.add(new Edge(map.get(fields[0]), map.get(fields[1]), random.nextDouble()));
            }
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
        }

        int nodeIndex = 0;


        Map<Integer, List<DAGNode>> mapOfNodes = new HashMap<>();

        for (Node node: nodes)
        {
            /*
            if(allNodes.containsKey(node.id))
            {
                System.out.println("Duplicate Node");
                return;
            }
            */
            for (int i = 0; i < numOfTasks.get(node.id); i++)
            {
                DAGNode newNode = new DAGNode(nodeIndex, node.executionCost / 1000, id, this);
                allNodes.put(nodeIndex++, newNode);
                if (i == 0)
                {
                    mapOfNodes.put(node.id, new ArrayList<DAGNode>());
                }
                mapOfNodes.get(node.id).add(newNode);
            }

        }
        for (Edge edge: edges)
        {
            /*
            if(!allNodes.containsKey(edge.sourceId) || !allNodes.containsKey(edge.destId))
            {
                System.out.println("Can not find node");
                return;
            }*/
            for (DAGNode srcDagNode: mapOfNodes.get(edge.sourceId))
            {
                for (DAGNode destDagNode: mapOfNodes.get(edge.destId))
                {
                    DAGEdge dagEdge = new DAGEdge(edge.executionCost * 1000, srcDagNode, destDagNode, this);
                    srcDagNode.children.add(dagEdge);
                    destDagNode.parents.add(dagEdge);
                }
            }
        }
        this.timeStamp = timeStamp;


    }

    public DAG (double timeStamp, int id)
    {
        this(-1, -1, timeStamp, id);
    }

    public static void defineDAG1(List<Node> nodes, List<Edge> edges)
    {
        nodes.add(new Node(0, 2));
        nodes.add(new Node(1, 1));
        nodes.add(new Node(2, 4));
        nodes.add(new Node(3, 1));
        nodes.add(new Node(4, 1));

        edges.add(new Edge(0, 2, 3));
        edges.add(new Edge(1, 3, 2));
        edges.add(new Edge(2, 4, 0.25));
        edges.add(new Edge(3, 4, 0.25));
    }
}

class Edge{
    int sourceId;
    int destId;
    double executionCost;
    public Edge(int sourceId, int destId, double executionCost)
    {
        this.sourceId = sourceId;
        this.destId = destId;
        this.executionCost = executionCost;
    }
}

class Node{
    int id;
    double executionCost;
    public Node(int id, double executionCost)
    {
        this.id = id;
        this.executionCost = executionCost;
    }
}
