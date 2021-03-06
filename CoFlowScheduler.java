import java.util.*;

class CoFlowScheduler{
    public static double eps = 0.0000001;
    List<DAG> DAGs;
    Cluster cluster;
    public CoFlowScheduler(List<DAG> DAGs, Cluster cluster)
    {
        this.DAGs = DAGs;
        this.cluster = cluster;
    }

    public double schedule()
    {
        score();
        double currrentTime = 0;
        double totoalUsedInputBandwidth = 0;
        double totoalUsedOutputBandwidth = 0;
        double completeTime = 0;
        int DAGsLeft = DAGs.size();
        Map<Integer, Integer> nodesLeft = new HashMap<>();
        List<DAGEdge> edgeTasks = new ArrayList<>();
        List<CoFlow> coFlowTasks = new ArrayList<>();
        Map<DAG, Double> DAGLength = calDAGLength();
        Map<DAG, Double> DAGTotalTime = calDAGTotalTime();
        Map<Integer, CPUNode> allCPUs = cluster.allCPUs;
        Map<DAGNode, Integer> inDegree = new HashMap<>();
        Map<DAGNode, Integer> DAGInDegree = new HashMap<>();
        Map<DAGNode, CPUNode> DAGCPUMap = new HashMap<>();
        Map<CPUNode, Double> remInputBW = new HashMap<>();
        Map<CPUNode, Double> remOutputBW = new HashMap<>();

        for (DAG dag: DAGs)
        {
            dag.weight = DAGLength.get(dag) * DAGTotalTime.get(dag);
        }

        for (DAG dag: DAGs)
        {
            inDegree.putAll(calInDegree(dag));
            DAGInDegree.putAll(calInDegree(dag));
        }

        int cpuIndex = 0;

        for (DAG dag: DAGs)
        {
            for (DAGNode dagNode: dag.allNodes.values())
            {
                if (inDegree.get(dagNode) == 0)
                {
                    allCPUs.get(cpuIndex).exec.add(new DAGNodeTime(dagNode, 0));
                    DAGCPUMap.put(dagNode, allCPUs.get(cpuIndex));
                    cpuIndex++;
                    if (cpuIndex == allCPUs.size())
                    {
                        cpuIndex = 0;
                    }
                }
            }
        }

        for (DAG dag: DAGs)
        {
            nodesLeft.put(dag.id, dag.allNodes.size());
        }

        for (CPUNode cpuNode: cluster.allCPUs.values())
        {
            remInputBW.put(cpuNode, cpuNode.bandwidthFromDB);
            remOutputBW.put(cpuNode, cpuNode.bandwidthToDB);
        }


        while (DAGsLeft != 0) {
            //nodeTasks.sort((o1, o2) -> comp(o1.executingTime, o2.executingTime));
            coFlowTasks.sort((o1, o2) -> comp(o1, o2));
            edgeTasks.sort((o1, o2) -> comp(o1, o2));

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

            int coflowIndex = 0;
            while (coflowIndex < coFlowTasks.size())
            {
                CoFlow coFlow = coFlowTasks.get(coflowIndex);
                if (coFlow.calBW(remOutputBW, cluster.totalCapacityToDB - totoalUsedOutputBandwidth))
                {
                    for (CPUNode cpuNode: coFlow.assignedBW.keySet())
                    {
                        cpuNode.bandwidthToDB -= coFlow.assignedBW.get(cpuNode);
                    }
                    coFlowTasks.remove(coflowIndex);
                }
                else
                    coflowIndex++;
            }

            for (CPUNode cpuNode: allCPUs.values())
            {
                if (cpuNode.output.size() != 0 && cpuNode.bandwidthToDB > eps)
                {
                    cpuNode.output.sort(((o1, o2) -> comp(o1.dagEdge, o2.dagEdge)));
                    cpuNode.output.getFirst().timeStamp = currrentTime;
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


            for (CoFlow coFlow: coFlowTasks)
            {
                nextTime = Math.min(nextTime, coFlow.expectedFinishTime);
            }


            for (CPUNode cpuNode: allCPUs.values())
            {
                for (int i = 0; i < cpuNode.exec.size(); i++)
                {
                    DAGNodeTime dagNodeTime = cpuNode.exec.get(i);

                    if (dagNodeTime.timeStamp + dagNodeTime.dagNode.executingTime <= nextTime + eps)
                    {
                        int nodesLeftNum = nodesLeft.get(dagNodeTime.dagNode.DAGid);
                        nodesLeft.put(dagNodeTime.dagNode.DAGid, nodesLeftNum - 1);
                        if (nodesLeftNum == 1)
                        {
                            DAGsLeft--;
                            completeTime += nextTime;
                        }

                        for (DAGEdge child: dagNodeTime.dagNode.children)
                        {
                            DAGNode dest = child.destId;
                            DAGInDegree.put(dest, DAGInDegree.get(dest) - 1);
                            if (DAGInDegree.get(dest) == 0)
                            {

                                coFlowTasks.add(new CoFlow(dest, DAGCPUMap));
                                removeFromOutputQueue(dest);
                            }
                            else
                            {
                                cpuNode.output.add(new DAGEdgeTime(child, -1));
                            }
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
                        DAGCPUMap.put(firstNode.dagEdge.destId, cpuNode);

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
                        firstNode.dagEdge.amountOfData -= cpuNode.bandwidthToDB * (nextTime - currrentTime);
                        if (firstNode.dagEdge.amountOfData < eps)
                        {
                            cpuNode.output.removeFirst();
                            totoalUsedOutputBandwidth -= cpuNode.bandwidthToDB;
                        }


                    }
                    /*
                    if (cpuNode.output.size() != 0 && cpuNode.output.getFirst().timeStamp == -1 && totoalUsedOutputBandwidth + cpuNode.bandwidthToDB <= cluster.totalCapacityToDB + eps) {
                        cpuNode.output.sort((o1, o2) -> comp(o1.dagEdge, o2.dagEdge));
                        cpuNode.output.get(0).timeStamp = nextTime;
                    }*/
                }


            }

            for (CoFlow coFlow: coFlowTasks)
            {
                if (coFlow.expectedFinishTime < nextTime + eps)
                {
                    DAGEdge newDAGEdge = new DAGEdge(0, coFlow.dagNode, coFlow.dagNode, coFlow.dagNode.dag);
                    edgeTasks.add(newDAGEdge);
                    for (DAGEdge dagEdge: coFlow.dagNode.parents)
                    {
                        newDAGEdge.amountOfData += dagEdge.amountOfDataBU;
                    }

                    for (CPUNode cpuNode: coFlow.assignedBW.keySet())
                    {
                        cpuNode.bandwidthToDB += coFlow.assignedBW.get(cpuNode);
                    }
                }
            }

            currrentTime = nextTime;


        }

        for (DAG dag: DAGs)
        {
            completeTime -= dag.timeStamp;
        }

        return completeTime / DAGs.size();

    }

    public void removeFromOutputQueue(DAGNode dest)
    {
        for (CPUNode cpuNode: cluster.allCPUs.values())
        {
            for (int i = 0; i < cpuNode.output.size(); i++)
            {
                DAGEdgeTime dagEdgeTime = cpuNode.output.get(i);
                if (dagEdgeTime.dagEdge.destId == dest)
                {
                    cpuNode.output.remove(i);
                    i--;
                }
            }
        }
    }

    public Map<DAG, Double> calDAGLength()
    {
        Map<DAG, Double> DAGLength = new HashMap<>();
        for (DAG dag: DAGs)
        {
            double length = 0;
            for (DAGNode dagNode : dag.allNodes.values()) {
                length = Math.max(length, dagNode.score);
            }
            DAGLength.put(dag, length);
        }

        return DAGLength;
    }

    public Map<DAG, Double> calDAGTotalTime()
    {
        Map<DAG, Double> DAGTotalTime = new HashMap<>();

        double computationalPower = cluster.allCPUs.get(0).computationPower;
        double bandwidthToDB = cluster.allCPUs.get(0).bandwidthToDB;
        double bandwidthFromDB = cluster.allCPUs.get(0).bandwidthFromDB;
        for (DAG dag: DAGs)
        {
            double length = 0;
            for (DAGNode dagNode : dag.allNodes.values()) {
                length += calCPUTime(dagNode.executingTime, computationalPower);
                for (DAGEdge dagEdge: dagNode.children)
                {
                    length += calNetworkTime(dagEdge.amountOfData, bandwidthFromDB);
                    length += calNetworkTime(dagEdge.amountOfData, bandwidthToDB);
                }
            }
            DAGTotalTime.put(dag, length);
        }

        return DAGTotalTime;
    }



    public void score()
    {
        double computationalPower = cluster.allCPUs.get(0).computationPower;
        double bandwidthToDB = cluster.allCPUs.get(0).bandwidthToDB;
        double bandwidthFromDB = cluster.allCPUs.get(0).bandwidthFromDB;
        for(DAG dag: DAGs)
        {
            Map<DAGNode, Integer> inDegree = calOutDegree(dag);


            Queue<DAGNode> q = new LinkedList<>();

            for (DAGNode dagNode: inDegree.keySet())
            {
                if(inDegree.get(dagNode) == 0)
                    q.offer(dagNode);
            }

            while (!q.isEmpty())
            {
                DAGNode dagNode = q.poll();
                double maxScore = 0;
                for (DAGEdge child: dagNode.children)
                {
                    maxScore = Math.max(maxScore, child.score);
                }
                dagNode.score = maxScore + calCPUTime(dagNode.executingTime, computationalPower);
                for (DAGEdge parent: dagNode.parents)
                {
                    parent.score = dagNode.score + calNetworkTime(parent.amountOfData, bandwidthFromDB) + calNetworkTime(parent.amountOfData, bandwidthToDB);
                    DAGNode p = parent.srcId;

                    inDegree.put(p, inDegree.get(p) - 1);
                    if(inDegree.get(p) == 0)
                        q.offer(p);
                }
            }
        }
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

    public void printStatistics()
    {

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

    public int comp(DAGEdge s1, DAGEdge s2)
    {
        if (s1.dag.weight - s2.dag.weight > 0)
            return 1;
        else if (s1.dag.weight - s2.dag.weight < 0)
            return -1;
        else
        {

            if (s1.score - s2.score > 0)
                return 1;
            else if (s1.score - s2.score < 0)
                return -1;
            else
                return 0;

            /*
            if (s1.amountOfData - s2.amountOfData > 0)
                return -1;
            else if (s1.amountOfData - s2.amountOfData < 0)
                return 1;
            else
                return 0;
                */
        }
    }

    public int comp(CoFlow s1, CoFlow s2)
    {
        if (s1.dagNode.dag.weight - s2.dagNode.dag.weight > 0)
            return 1;
        else if (s1.dagNode.dag.weight - s2.dagNode.dag.weight < 0)
            return -1;
        else
        {

            if (s1.score - s2.score > 0)
                return 1;
            else if (s1.score - s2.score < 0)
                return -1;
            else
                return 0;

            /*
            if (s1.amountOfData - s2.amountOfData > 0)
                return -1;
            else if (s1.amountOfData - s2.amountOfData < 0)
                return 1;
            else
                return 0;
                */
        }
    }

    public double calNetworkTime(double amountOfData, double bandwidth)
    {
        return amountOfData/bandwidth;
    }

    public double calCPUTime(double executionTime, double computationalPower)
    {
        return executionTime/computationalPower;
    }
}

class CoFlow{
    double esp = 0.001;
    double timeStamp;
    double expectedFinishTime = -1;
    int width;
    double length = 0;
    double score;
    DAGNode dagNode;
    Map<CPUNode, Double> flows = new HashMap<>();
    Map<CPUNode, Double> assignedBW = new HashMap<>();

    public CoFlow(DAGNode dagNode, Map<DAGNode, CPUNode> DAGCPUMap)
    {
        this.dagNode = dagNode;
        this.timeStamp = -1;

        for (DAGEdge parent: dagNode.parents)
        {
            if (parent.amountOfData > esp)
            {
                addFlow(DAGCPUMap.get(parent.srcId), parent.amountOfData);
            }
        }
        this.score = this.width * length;
    }

    public void addFlow(CPUNode cpuNode, double amountOfData)
    {
        double newAmountOfData = amountOfData;
        if (flows.containsKey(cpuNode))
        {
            newAmountOfData += flows.get(cpuNode);
        }
        else
        {
            width++;
        }

        length = Math.max(length, newAmountOfData);
        flows.put(cpuNode, newAmountOfData);
    }

    public boolean calBW(Map<CPUNode, Double> remOutputBW, double remTotalOutputBW)
    {
        if (remTotalOutputBW < esp)
            return false;

        double totalAmountOfData = 0;

        for (Double data: flows.values())
        {
            totalAmountOfData += data;
        }



        for (CPUNode cpuNode: flows.keySet())
        {
            if (remOutputBW.get(cpuNode) < esp)
            {
                expectedFinishTime = -1;
                return false;
            }
            expectedFinishTime = Math.max(expectedFinishTime, flows.get(cpuNode) / remOutputBW.get(cpuNode));
        }

        expectedFinishTime = Math.max(expectedFinishTime, totalAmountOfData / remTotalOutputBW);

        for (CPUNode cpuNode: flows.keySet())
        {
            assignedBW.put(cpuNode, flows.get(cpuNode) / expectedFinishTime);
        }

        expectedFinishTime += timeStamp;

        return true;
    }

    public boolean isRunning(double currentTime)
    {
        return expectedFinishTime != -1;
    }

    public boolean isFinish(double currentTime)
    {
        return currentTime + esp > expectedFinishTime;
    }
}