import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SJFScheduler{
    public static double eps = 0.0000001;
    List<DAG> DAGs;
    Cluster cluster;
    public SJFScheduler(List<DAG> DAGs, Cluster cluster)
    {
        this.DAGs = DAGs;
        this.cluster = cluster;
    }

    public double schedule()
    {
        //List<DAGNode> nodeTasks = new ArrayList<>();
        List<DAGEdge> edgeTasks = new ArrayList<>();

        double currrentTime = 0;

        Map<Integer, CPUNode> allCPUs = cluster.allCPUs;
        Map<DAGNode, Integer> inDegree = new HashMap<>();

        for (DAG dag: DAGs)
        {
            inDegree.putAll(calInDegree(dag));
        }

        double totoalUsedInputBandwidth = 0;
        double totoalUsedOutputBandwidth = 0;

        int cpuIndex = 0;

        for (DAG dag: DAGs)
        {
            for (DAGNode dagNode: dag.allNodes.values())
            {
                if (inDegree.get(dagNode) == 0)
                {
                    allCPUs.get(cpuIndex).exec.add(new DAGNodeTime(dagNode, 0));
                    cpuIndex++;
                    if (cpuIndex == allCPUs.size())
                    {
                        cpuIndex = 0;
                    }
                }
            }
        }

        Map<DAGNode, CPUNode> DAGCPUMap = new HashMap<>();

        int DAGsLeft = DAGs.size();

        Map<Integer, Integer> nodesLeft = new HashMap<>();

        for (DAG dag: DAGs)
        {
            nodesLeft.put(dag.id, dag.allNodes.size());
        }

        double completeTime = 0;

        while (DAGsLeft != 0) {
            //nodeTasks.sort((o1, o2) -> comp(o1.executingTime, o2.executingTime));
            edgeTasks.sort((o1, o2) -> comp(o1.amountOfData, o2.amountOfData));

            int nodeTaskIndex = 0;
            int edgeTaskIndex = 0;

            for (CPUNode cpuNode: allCPUs.values())
            {
                if (cpuNode.input.size() == 0)
                {
                    if (totoalUsedInputBandwidth + cpuNode.bandwidthFromDB > cluster.totalCapacityFromDB)
                        break;
                    if (edgeTaskIndex == edgeTasks.size())
                        break;
                    cpuNode.input.add(new DAGEdgeTime(edgeTasks.get(edgeTaskIndex), currrentTime));
                    edgeTasks.remove(edgeTaskIndex);
                    totoalUsedInputBandwidth += cpuNode.bandwidthFromDB;
                }
            }

            double nextTime = Double.MAX_VALUE;
            for (CPUNode cpuNode: allCPUs.values())
            {
                for (DAGNodeTime dagNodeTime: cpuNode.exec)
                {
                    nextTime = Math.min(nextTime, dagNodeTime.timeStamp + dagNodeTime.dagNode.executingTime);
                }

                if (cpuNode.input.size() != 0 && cpuNode.input.get(0).timeStamp != -1)
                {
                    DAGEdgeTime dagEdgeTime = cpuNode.input.get(0);
                    nextTime = Math.min(nextTime, dagEdgeTime.timeStamp + calNetworkTime(dagEdgeTime.dagEdge.amountOfData, cpuNode.bandwidthFromDB));

                }

                if (cpuNode.output.size() != 0 && cpuNode.output.get(0).timeStamp != -1)
                {
                    DAGEdgeTime dagEdgeTime = cpuNode.output.get(0);
                    nextTime = Math.min(nextTime, dagEdgeTime.timeStamp + calNetworkTime(dagEdgeTime.dagEdge.amountOfData, cpuNode.bandwidthToDB));
                }
            }

            currrentTime = nextTime;

            for (CPUNode cpuNode: allCPUs.values())
            {
                for (int i = 0; i < cpuNode.exec.size(); i++)
                {
                    DAGNodeTime dagNodeTime = cpuNode.exec.get(i);

                    if (dagNodeTime.timeStamp + dagNodeTime.dagNode.executingTime <= nextTime + eps)
                    {
                        for (DAGEdge dagEdge: dagNodeTime.dagNode.children)
                        {
                            cpuNode.output.add(new DAGEdgeTime(dagEdge, -1));
                        }

                        int nodesLeftNum = nodesLeft.get(dagNodeTime.dagNode.DAGid);
                        nodesLeft.put(dagNodeTime.dagNode.DAGid, nodesLeftNum - 1);
                        if (nodesLeftNum == 1)
                        {
                            DAGsLeft--;
                            completeTime += currrentTime;
                        }
                        cpuNode.exec.remove(dagNodeTime);
                        i--;
                    }
                }

                if (cpuNode.input.size() != 0)
                {
                    DAGEdgeTime firstNode = cpuNode.input.getFirst();
                    if (firstNode.timeStamp + calNetworkTime(firstNode.dagEdge.amountOfData, cpuNode.bandwidthFromDB) < nextTime + eps)
                    {

                        cpuNode.exec.add(new DAGNodeTime(firstNode.dagEdge.destId, nextTime));


                        cpuNode.input.removeFirst();
                        totoalUsedInputBandwidth -= cpuNode.bandwidthFromDB;
                    }

                    /*
                    if (cpuNode.input.size() != 0 && cpuNode.input.getFirst().timeStamp == -1) {
                        cpuNode.input.sort((o1, o2) -> comp(o1.dagEdge.amountOfData, o2.dagEdge.amountOfData));
                        cpuNode.input.get(0).timeStamp = nextTime;
                    }*/
                }

                if (cpuNode.output.size() != 0)
                {
                    DAGEdgeTime firstNode = cpuNode.output.getFirst();

                    if (firstNode.timeStamp != -1)
                    {
                        if (firstNode.timeStamp + calNetworkTime(firstNode.dagEdge.amountOfData, cpuNode.bandwidthToDB) < nextTime + eps)
                        {
                            inDegree.put(firstNode.dagEdge.destId, inDegree.get(firstNode.dagEdge.destId) - 1);
                            if (inDegree.get(firstNode.dagEdge.destId) == 0) {
                                DAGEdge newDAGEdge = new DAGEdge(0, firstNode.dagEdge.srcId, firstNode.dagEdge.destId, firstNode.dagEdge.dag);
                                edgeTasks.add(newDAGEdge);
                                for (DAGEdge dagEdge: firstNode.dagEdge.destId.parents)
                                {
                                    newDAGEdge.amountOfData += dagEdge.amountOfData;
                                }
                            }
                            cpuNode.output.removeFirst();
                            totoalUsedOutputBandwidth -= cpuNode.bandwidthToDB;
                        }


                    }

                    if (cpuNode.output.size() != 0 && cpuNode.output.getFirst().timeStamp == -1 && totoalUsedOutputBandwidth + cpuNode.bandwidthToDB <= cluster.totalCapacityToDB + eps) {
                        cpuNode.output.sort((o1, o2) -> comp(o1.dagEdge.amountOfData, o2.dagEdge.amountOfData));
                        cpuNode.output.get(0).timeStamp = nextTime;
                    }
                }
            }



        }

        for (DAG dag: DAGs)
        {
            completeTime -= dag.timeStamp;
        }

        return completeTime / DAGs.size();

    }

    public double calNetworkTime(double amountOfData, double bandwidth)
    {
        return amountOfData/bandwidth;
    }

    public Map<DAGNode, Integer> calInDegree(DAG dag)
    {
        Map<DAGNode, Integer> inDegree = new HashMap<>();
        for (DAGNode dagNode: dag.allNodes.values())
        {
            inDegree.put(dagNode, dagNode.parents.size());
        }
        return inDegree;
    }

    public Map<DAGNode, Integer> calOutDegree(DAG dag)
    {
        Map<DAGNode, Integer> inDegree = new HashMap<>();
        for (DAGNode dagNode: dag.allNodes.values())
        {
            inDegree.put(dagNode, dagNode.children.size());
        }
        return inDegree;
    }

    public int comp(double s1, double s2)
    {
        if (s1 - s2 > 0)
            return 1;
        else if (s1 - s2 < 0)
            return -1;
        else
            return 0;
    }
}