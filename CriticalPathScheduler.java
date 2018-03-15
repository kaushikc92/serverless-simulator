import java.util.*;

class CriticalPathScheduler{
    public static double eps = 0.001;
    List<DAG> DAGs;
    Cluster cluster;
    public CriticalPathScheduler(List<DAG> DAGs, Cluster cluster)
    {
        this.DAGs = DAGs;
        this.cluster = cluster;
    }

    public double schedule()
    {
        score();

        List<DAGNode> nodeTasks = new ArrayList<>();
        List<DAGEdge> edgeTasks = new ArrayList<>();

        double currrentTime = 0;
        for (DAG dag: DAGs)
        {
            nodeTasks.addAll(dag.allNodes.values());
        }

        Map<Integer, CPUNode> allCPUs = cluster.allCPUs;
        Map<DAGNode, Integer> inDegree = new HashMap<>();
        for (DAG dag: DAGs)
        {
            inDegree.putAll(calInDegree(dag));
        }
        Map<DAGNode, CPUNode> DAGCPUMap = new HashMap<>();



        while (!nodeTasks.isEmpty())
        {
            nodeTasks.sort((o1, o2) -> -comp(o1.score, o2.score));
            edgeTasks.sort((o1, o2) -> -comp(o1.score, o2.score));

            int index = 0;
            while(index < nodeTasks.size()) {

                DAGNode nodeToExec = nodeTasks.get(index);
                if (inDegree.get(nodeToExec) != 0)
                {
                    index++;
                    continue;
                }

                boolean hasIdleCPU = false;
                CPUNode CPUToUse = null;

                if (DAGCPUMap.containsKey(nodeToExec)) {
                    CPUToUse = DAGCPUMap.get(nodeToExec);
                    if (CPUToUse.exec.size() < CPUToUse.computationPower)
                        hasIdleCPU = true;
                    else

                        for (DAGNodeTime dagNodeTime : CPUToUse.exec) {
                            if (dagNodeTime.dagNode.score < nodeToExec.score) {
                                double startTime = dagNodeTime.timeStamp;
                                double endTime = currrentTime;
                                DAGNodeInterval dagNodeInterval = new DAGNodeInterval(dagNodeTime.dagNode, startTime, endTime);
                                CPUToUse.execHistory.add(dagNodeInterval);
                                dagNodeTime.dagNode.score -= (endTime - startTime);
                                dagNodeTime.dagNode.executingTime -= (endTime - startTime);
                                CPUToUse.next.add(dagNodeTime.dagNode);

                                CPUToUse.exec.add(new DAGNodeTime(nodeToExec, currrentTime));
                                nodeTasks.remove(nodeToExec);
                                hasIdleCPU = true;
                            }
                        }

                } else {



                    for (CPUNode cpuNode : allCPUs.values()) {
                        if (cpuNode.exec.size() < cpuNode.computationPower) {
                            CPUToUse = cpuNode;
                            hasIdleCPU = true;
                            break;
                        }
                    }

                    if (!hasIdleCPU) {
                        DAGNodeTime lowestPriorityNode = null;
                        for (CPUNode cpuNode : allCPUs.values()) {
                            for (DAGNodeTime dagNodeTime : cpuNode.exec) {
                                if (dagNodeTime.dagNode.score < nodeToExec.score) {
                                    if (lowestPriorityNode == null) {
                                        lowestPriorityNode = dagNodeTime;
                                        CPUToUse = cpuNode;
                                    } else {
                                        if (lowestPriorityNode.dagNode.score > dagNodeTime.dagNode.score) {
                                            lowestPriorityNode = dagNodeTime;
                                            CPUToUse = cpuNode;
                                        }
                                    }
                                }
                            }
                        }

                        if (lowestPriorityNode != null) {
                            hasIdleCPU = true;
                            double startTime = lowestPriorityNode.timeStamp;
                            double endTime = currrentTime;
                            DAGNodeInterval dagNodeInterval = new DAGNodeInterval(lowestPriorityNode.dagNode, startTime, endTime);
                            CPUToUse.execHistory.add(dagNodeInterval);
                            lowestPriorityNode.dagNode.score -= (endTime - startTime);
                            lowestPriorityNode.dagNode.executingTime -= (endTime - startTime);//??
                            CPUToUse.next.add(lowestPriorityNode.dagNode);
                        }
                    }


                }

                if (hasIdleCPU) {
                    CPUToUse.exec.add(new DAGNodeTime(nodeToExec, currrentTime));
                    nodeTasks.remove(nodeToExec);
                    DAGCPUMap.put(nodeToExec, CPUToUse);
                    index--;
                } else
                    break;

                index++;
            }


            index = 0;

            while (index < edgeTasks.size())
            {
                DAGEdge edgeToExec = edgeTasks.get(index);
                boolean hasIdleCPU = false;
                CPUNode CPUToUse = null;

                for (CPUNode cpuNode: allCPUs.values())
                {
                    if (cpuNode.input.size() == 0)
                    {
                        CPUToUse = cpuNode;
                        hasIdleCPU = true;
                        break;
                    }
                }

                if (!hasIdleCPU)
                {
                    DAGEdgeTime lowestPriorityNode = null;
                    for (CPUNode cpuNode: allCPUs.values())
                    {
                        for (DAGEdgeTime dagEdgeTime: cpuNode.input)
                        {
                            if (dagEdgeTime.dagEdge.score < edgeToExec.score)
                            {
                                if (lowestPriorityNode == null)
                                {
                                    lowestPriorityNode = dagEdgeTime;
                                    CPUToUse = cpuNode;
                                }
                                else
                                {
                                    if (lowestPriorityNode.dagEdge.score > dagEdgeTime.dagEdge.score)
                                    {
                                        lowestPriorityNode = dagEdgeTime;
                                        CPUToUse = cpuNode;
                                    }
                                }
                            }
                        }
                    }

                    if (lowestPriorityNode != null)
                    {
                        hasIdleCPU = true;
                        double startTime = lowestPriorityNode.timeStamp;
                        double endTime = currrentTime;
                        DAGEdgeInterval dagEdgeInterval = new DAGEdgeInterval(lowestPriorityNode.dagEdge, startTime, endTime);
                        CPUToUse.inputHistory.add(dagEdgeInterval);
                        lowestPriorityNode.dagEdge.score -= (endTime -startTime);
                        lowestPriorityNode.timeStamp -= (endTime -startTime);
                        CPUToUse.input.addFirst(new DAGEdgeTime(lowestPriorityNode.dagEdge, currrentTime));

                    }
                }

                if (hasIdleCPU)
                {
                    CPUToUse.input.add(new DAGEdgeTime(edgeToExec, currrentTime));
                    CPUToUse.next.add(edgeToExec.destId);
                    edgeTasks.remove(edgeToExec);
                    DAGCPUMap.put(edgeToExec.destId, CPUToUse);
                    index--;
                }
                else
                    break;
                index++;
            }

            double timeForward = nextTime();
            currrentTime += timeForward;

            //exec


            for (CPUNode cpuNode: cluster.allCPUs.values())
            {

                for (DAGNodeTime dagNodeTime: cpuNode.exec)
                {

                    dagNodeTime.dagNode.executingTime -= timeForward;
                    dagNodeTime.dagNode.score -= timeForward;
                    if (dagNodeTime.dagNode.executingTime < eps)
                    {
                        DAGNode finishedNode = dagNodeTime.dagNode;
                        cpuNode.exec.remove(dagNodeTime);
                        cpuNode.execHistory.add(new DAGNodeInterval(finishedNode, dagNodeTime.timeStamp, currrentTime));
                        for (DAGEdge dagEdge: finishedNode.children)
                        {
                            cpuNode.output.add(new DAGEdgeTime(dagEdge, -1));
                        }
                        DAGCPUMap.remove(finishedNode);
                    }
                }



                if(cpuNode.output.size() > 0)
                {
                    DAGEdgeTime dagEdgeTime = cpuNode.output.get(0);

                    if (dagEdgeTime.timeStamp == -1)
                    {
                        dagEdgeTime.timeStamp = currrentTime;
                    }
                    else
                    {
                        dagEdgeTime.dagEdge.amountOfData -= timeForward;
                        dagEdgeTime.dagEdge.score -= timeForward;
                        if(dagEdgeTime.dagEdge.amountOfData < eps)
                        {
                            DAGEdge finishedEdge = dagEdgeTime.dagEdge;
                            cpuNode.output.remove(dagEdgeTime);
                            if (cpuNode.output.size() > 0)
                                cpuNode.output.get(0).timeStamp = currrentTime;

                            finishedEdge.amountOfData = finishedEdge.amountOfDataBU;
                            finishedEdge.score += finishedEdge.amountOfDataBU;
                            if (DAGCPUMap.containsKey(finishedEdge.destId))
                            {
                                DAGCPUMap.get(finishedEdge.destId).input.add(new DAGEdgeTime(finishedEdge, -1));
                            }
                            else
                            {
                                edgeTasks.add(finishedEdge);
                            }
                        }
                    }

                }

                if(cpuNode.input.size() > 0)
                {
                    DAGEdgeTime dagEdgeTime = cpuNode.input.get(0);

                    if (dagEdgeTime.timeStamp == -1)
                    {
                        dagEdgeTime.timeStamp = currrentTime;
                    }
                    else {
                        dagEdgeTime.dagEdge.amountOfData -= timeForward;
                        dagEdgeTime.dagEdge.score -= timeForward;
                        if (dagEdgeTime.dagEdge.amountOfData < eps) {
                            DAGEdge finishedEdge = dagEdgeTime.dagEdge;
                            inDegree.put(finishedEdge.destId, inDegree.get(finishedEdge.destId) - 1);


                            cpuNode.input.remove(dagEdgeTime);


                            if (cpuNode.input.size() > 0) {
                                //Do we need to rank here?
                                cpuNode.input.get(0).timeStamp = currrentTime;
                            }
                        }
                    }
                }


            }


        }
        return currrentTime;
    }

    public double nextTime()
    {
        double nextTime = Double.MAX_VALUE;
        for (CPUNode cpuNode: cluster.allCPUs.values())
        {
            for (DAGNodeTime dagNodeTime: cpuNode.exec)
            {
                nextTime = Math.min(dagNodeTime.dagNode.executingTime, nextTime);
            }

            if (cpuNode.input.size() > 0)
            {
                DAGEdgeTime dagEdgeTime = cpuNode.input.get(0);
                nextTime = Math.min(dagEdgeTime.dagEdge.amountOfData, nextTime);
            }

            if (cpuNode.output.size() > 0)
            {
                DAGEdgeTime dagEdgeTime = cpuNode.output.get(0);
                nextTime = Math.min(dagEdgeTime.dagEdge.amountOfData, nextTime);
            }
        }

        return nextTime;
    }


    public void score()
    {
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
                dagNode.score = maxScore + dagNode.executingTime;
                for (DAGEdge parent: dagNode.parents)
                {
                    parent.score = dagNode.score + parent.amountOfData * 2;
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
}