#include "fonda_scheduler/SchedulerHeader.hpp"

#include <iterator>

Cluster* imaginedCluster;
Cluster* actualCluster;

double howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(const vertex_t* v, const shared_ptr<Processor>& pj)
{
    assert(pj->getAvailableMemory() >= 0);
    double Res = pj->getAvailableMemory() - peakMemoryRequirementOfVertex(v);
    for (int i = 0; i < v->in_degree; i++) {
        auto inEdge = v->in_edges[i];
        if (pj->getPendingMemories().find(inEdge) != pj->getPendingMemories().end()) {
            // incoming edge occupied memory
            Res += inEdge->weight;
        }
    }
    return Res;
}

double new_heuristic(graph_t* graph, int algoNum, bool isHeft)
{
    algoNum = isHeft ? 1 : algoNum;
    if (isHeft) {
        imaginedCluster->mayBecomeInvalid();
    }
    vector<pair<vertex_t*, double>> ranks = calculateBottomLevels(graph, algoNum);
    removeSourceAndTarget(graph, ranks);
    sort(ranks.begin(), ranks.end(),
        [](pair<vertex_t*, double> a, pair<vertex_t*, double> b) {
            return a.second > b.second;
        });
    double makespan = 0, makespanPerceived = 0;
    int numberWithEvictedCases = 0, numberWithEvictedCases2 = 0;
    for (auto& pair : ranks) {
        auto vertex = pair.first;
        cout << "processing " << vertex->name << endl;
        if ( // vertex->name=="TRIMGALORE_00000540" //|| vertex->name=="TRIMGALORE_00000002" ||
            vertex->name == "MERGED_LIB_BAM_FILTER_00000205") {
            cout << endl;
        }

        SchedulingResult bestSchedulingResult = SchedulingResult(nullptr);
        SchedulingResult bestSchedulingResultCorrectForHeftOnly = SchedulingResult(nullptr);
        bestTentativeAssignment(isHeft, vertex, bestSchedulingResult, bestSchedulingResultCorrectForHeftOnly);

        SchedulingResult bestSchedulingResultOnReal = SchedulingResult(
            actualCluster->getProcessorById(bestSchedulingResult.processorOfAssignment->id));
        SchedulingResult bestCorrectSchedulingResultOnReal = SchedulingResult(
            actualCluster->getProcessorById(bestSchedulingResult.processorOfAssignment->id));
        if (isHeft) {
            tentativeAssignmentHEFT(vertex, true, bestSchedulingResultOnReal, bestCorrectSchedulingResultOnReal);
            bestSchedulingResultOnReal = bestCorrectSchedulingResultOnReal;
        } else {
            tentativeAssignment(vertex, true, bestSchedulingResultOnReal);
        }

        if (bestSchedulingResult.modifiedProcs.empty()) {
            cout << "Invalid assignment of " << vertex->name;
            return -1;
        } else {
            cout << " for " << vertex->name << " best " << bestSchedulingResult.startTime << " "
                 << bestSchedulingResult.finishTime << " on proc "
                 << bestSchedulingResult.processorOfAssignment->id
                 << endl; //<<" with av mem "<<bestProcessorToAssign->availableMemory<<endl;

            cout << " for " << vertex->name << " best real " << bestSchedulingResultOnReal.startTime << " "
                 << bestSchedulingResultOnReal.finishTime << " on proc "
                 << bestSchedulingResultOnReal.processorOfAssignment->id
                 << endl; //<<" with av mem "<<bestProcessorToAssign->availableMemory<<endl;
        }

        if (vertex->name == "TRIMGALORE_00000055") {
            cout << "";
        }
        cout << "imagine" << endl;
        putChangeOnCluster(vertex, bestSchedulingResult, imaginedCluster, numberWithEvictedCases, false, isHeft);
        // try {
        cout << "real" << endl;
        putChangeOnCluster(vertex, bestSchedulingResultOnReal, actualCluster, numberWithEvictedCases2, true, isHeft);
        //}
        // catch(...){
        //    cout<<"some error"<<endl;
        //   }

        if (!isHeft) {
            for (const auto& item : imaginedCluster->getProcessors()) {
                assert(item.second->getPendingMemories().size() == actualCluster->getProcessorById(item.second->id)->getPendingMemories().size());
            }
        }

        vertex->makespanPerceived = bestSchedulingResult.finishTime;
        assert(bestSchedulingResult.startTime < bestSchedulingResult.finishTime);

        vertex->makespan = bestSchedulingResultOnReal.finishTime;

        if (makespan < bestSchedulingResultOnReal.finishTime)
            makespan = bestSchedulingResultOnReal.finishTime;
        if (makespanPerceived < bestSchedulingResult.finishTime)
            makespanPerceived = bestSchedulingResult.finishTime;
    }
    cout <<
        //" #eviction " << numberWithEvictedCases << " " <<
        " ms perceived " << makespanPerceived << " ";
    return makespan;
}

void bestTentativeAssignment(bool isHeft, vertex_t* vertex, SchedulingResult& result, SchedulingResult& correctResultForHeftOnly)
{
    result.finishTime = numeric_limits<double>::max();
    result.startTime = 0;

    double bestActualStartTime, bestActualFinishTime;

    for (auto& [id, processor] : imaginedCluster->getProcessors()) {

        double actualStartTime = 0, actualFinishTime = 0;
        SchedulingResult tentativeResult = SchedulingResult(processor);
        SchedulingResult correctTentativeResultForHeftOnly = SchedulingResult(processor);

        checkIfPendingMemoryCorrect(processor);
        if (isHeft) {
            tentativeAssignmentHEFT(vertex, false, tentativeResult, correctTentativeResultForHeftOnly);
        } else {
            tentativeAssignment(vertex, false, tentativeResult);
        }
        // cout<<"tentative ft on "<<processor->id<<" is "<<tentativeResult.finishTime<<endl;

        if (!isHeft)
            checkIfPendingMemoryCorrect(tentativeResult.processorOfAssignment);
        // cout<<"start "<<startTime<<" end "<<finTime<<endl;
        if (tentativeResult.startTime != actualStartTime) {
            //  cout<<"not actual "<<finTime<<" vs "<<actualFinishTime<<" on "<<processor->id<<endl;
        }
        if (result.finishTime > tentativeResult.finishTime
            || (result.finishTime == tentativeResult.finishTime && result.processorOfAssignment && tentativeResult.processorOfAssignment->getMemorySize() > result.processorOfAssignment->getMemorySize())) {
            assert(!tentativeResult.modifiedProcs.empty());
            result = tentativeResult;
            if (isHeft) {
                correctResultForHeftOnly = correctTentativeResultForHeftOnly;
                if (result.startTime != correctTentativeResultForHeftOnly.startTime) {
                    // resultnumberWithEvictedCases++;
                    // throw runtime_error("numberWithEvictedCases++;");
                    //    cout << "increase numWithEvictged in HEFT" << endl;
                }
                result.resultingVar = 1;
            }
        }
    }
}

void tentativeAssignment(vertex_t* v, bool real, SchedulingResult& result)
{

    result.resultingVar = 1;
    double timeToRun = real ? v->time * v->factorForRealExecution : v->time;

    double sumOut = getSumOut(v);
    if (result.processorOfAssignment->getMemorySize() < sumOut) {
        //  cout<<"too large outs absolutely"<<endl;
        result.finishTime = std::numeric_limits<double>::max();
        return;
    }
    realSurplusOfOutgoingEdges(v, result.processorOfAssignment, sumOut);

    vector<std::shared_ptr<Processor>> modifiedProcs;
    modifiedProcs.emplace_back(result.processorOfAssignment);
    processIncomingEdges(v, real, result.processorOfAssignment, modifiedProcs, result.startTime);

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    result.startTime = result.processorOfAssignment->getReadyTimeCompute() > result.startTime
        ? result.processorOfAssignment->getReadyTimeCompute()
        : result.startTime;

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    checkIfPendingMemoryCorrect(result.processorOfAssignment);

    double Res = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(v, result.processorOfAssignment);
    result.peakMem = (Res < 0) ? 1 : (result.processorOfAssignment->getMemorySize() - Res) / result.processorOfAssignment->getMemorySize();

    if (Res < 0) {
        // try finish times with and without memory overflow
        double amountToOffload = -Res;

        double timeToFinishNoEvicted = result.startTime + timeToRun / result.processorOfAssignment->getProcessorSpeed() + amountToOffload / result.processorOfAssignment->memoryOffloadingPenalty;
        assert(timeToFinishNoEvicted > result.startTime);
        if (sumOut > result.processorOfAssignment->getAvailableMemory()) {
            // cout<<"cant"<<endl;
            timeToFinishNoEvicted = std::numeric_limits<double>::max();
        }

        double timeToFinishBiggestEvicted = std::numeric_limits<double>::max(),
               timeToFinishAllEvicted = std::numeric_limits<double>::max();
        double timeToWriteAllPending = 0;

        double startTimeFor1Evicted, startTimeForAllEvicted;
        startTimeFor1Evicted = startTimeForAllEvicted = result.processorOfAssignment->getReadyTimeWrite() > result.startTime ? result.processorOfAssignment->getReadyTimeWrite() : result.startTime;
        auto biggestPendingEdge = result.processorOfAssignment->getBiggestPendingEdgeThatIsNotIncomingOf(v);
        if (!result.processorOfAssignment->getPendingMemories().empty() && biggestPendingEdge != nullptr) {
            assert((*result.processorOfAssignment->getPendingMemories().begin())->weight >= (*result.processorOfAssignment->getPendingMemories().rbegin())->weight);

            auto biggestFileWeight = biggestPendingEdge->weight;
            double amountToOffloadWithoutBiggestFile = (amountToOffload - biggestFileWeight) > 0 ? (amountToOffload - biggestFileWeight)
                                                                                                 : 0;

            double biggestWeightToWrite = real ? biggestPendingEdge->weight * biggestPendingEdge->factorForRealExecution
                                               : biggestPendingEdge->weight;
            double finishTimeToWrite = result.processorOfAssignment->getReadyTimeWrite() + biggestWeightToWrite / result.processorOfAssignment->writeSpeedDisk;
            startTimeFor1Evicted = max(result.startTime, finishTimeToWrite);
            timeToFinishBiggestEvicted = startTimeFor1Evicted
                + timeToRun / result.processorOfAssignment->getProcessorSpeed() + amountToOffloadWithoutBiggestFile / result.processorOfAssignment->memoryOffloadingPenalty;
            assert(timeToFinishBiggestEvicted > startTimeFor1Evicted);

            double availableMemWithoutBiggest = result.processorOfAssignment->getAvailableMemory() + biggestFileWeight;
            if (sumOut > availableMemWithoutBiggest)
                timeToFinishBiggestEvicted = std::numeric_limits<double>::max();

            double sumWeightsOfAllPending = 0;
            for (const auto& item : result.processorOfAssignment->getPendingMemories()) {
                if (item->head->name != v->name) {
                    double itemWeightToWrite = real ? item->weight * item->factorForRealExecution : item->weight;
                    timeToWriteAllPending += itemWeightToWrite / result.processorOfAssignment->writeSpeedDisk;
                    sumWeightsOfAllPending += item->weight;
                }
            }

            double amountToOffloadWithoutAllFiles = (amountToOffload - sumWeightsOfAllPending > 0) ? amountToOffload - sumWeightsOfAllPending : 0;

            assert(amountToOffloadWithoutAllFiles >= 0);
            finishTimeToWrite = result.processorOfAssignment->getReadyTimeWrite() + timeToWriteAllPending;
            startTimeForAllEvicted = max(startTimeForAllEvicted, finishTimeToWrite);
            timeToFinishAllEvicted = startTimeForAllEvicted + timeToRun / result.processorOfAssignment->getProcessorSpeed() + amountToOffloadWithoutAllFiles / result.processorOfAssignment->memoryOffloadingPenalty;
            assert(timeToFinishAllEvicted > startTimeForAllEvicted);
        }

        double minTTF = min(timeToFinishNoEvicted, min(timeToFinishBiggestEvicted, timeToFinishAllEvicted));
        if (minTTF == std::numeric_limits<double>::max()) {
            cout << "minTTF inf" << endl;
            result.finishTime = std::numeric_limits<double>::max();
            return;
        }
        assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
        result.processorOfAssignment->setReadyTimeCompute(minTTF);
        result.finishTime = result.processorOfAssignment->getReadyTimeCompute();
        assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

        if (timeToFinishBiggestEvicted == minTTF) {
            result.edgeToKick = biggestPendingEdge;
            // cout<<"best tentative with biggest Evicted "; print_edge(toKick);
            result.resultingVar = 2;
            double biggestWeightToWrite = real ? biggestPendingEdge->weight * biggestPendingEdge->factorForRealExecution
                                               : biggestPendingEdge->weight;
            result.processorOfAssignment->setReadyTimeWrite(result.processorOfAssignment->getReadyTimeWrite() + biggestWeightToWrite / result.processorOfAssignment->writeSpeedDisk);
            // ourModifiedProc->pendingMemories.erase()
            // penMemsAsVector.erase(penMemsAsVector.begin());
            assert(result.startTime <= startTimeFor1Evicted);
            result.startTime = startTimeFor1Evicted;
            assert(result.edgeToKick != nullptr);
            assert(!result.edgeToKick->locations.empty());
            assert(isLocatedOnThisProcessor(result.edgeToKick, result.processorOfAssignment->id));
        } else if (timeToFinishAllEvicted == minTTF) {
            result.resultingVar = 3;
            // cout<<"best tentative with all Evicted ";
            result.processorOfAssignment->setReadyTimeWrite(
                result.processorOfAssignment->getReadyTimeWrite() + timeToWriteAllPending);
            assert(result.startTime <= startTimeForAllEvicted);
            result.startTime = startTimeForAllEvicted;
            // penMemsAsVector.resize(0);
        }

    } else {
        // startTime =  ourModifiedProc->readyTimeCompute;
        //  printInlineDebug("should be successful");
        result.processorOfAssignment->setReadyTimeCompute(
            result.startTime + timeToRun / result.processorOfAssignment->getProcessorSpeed());
        result.finishTime = result.processorOfAssignment->getReadyTimeCompute();
    }
    // cout<<endl;
    assert(result.finishTime > result.startTime);
    result.modifiedProcs = modifiedProcs;
}

void tentativeAssignmentPureHEFT(vertex_t* v, SchedulingResult& result)
{

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    double sumOut = getSumOut(v);

    if (result.processorOfAssignment->getMemorySize() < sumOut) {
        result.finishTime = std::numeric_limits<double>::max();
        return;
    }

    vector<std::shared_ptr<Processor>> modifiedProcs;
    modifiedProcs.emplace_back(result.processorOfAssignment);
    processIncomingEdges(v, false, result.processorOfAssignment, modifiedProcs, result.startTime);

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    result.startTime = result.processorOfAssignment->getReadyTimeCompute() > result.startTime ? result.processorOfAssignment->getReadyTimeCompute() : result.startTime;

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    result.finishTime = result.startTime + v->time / result.processorOfAssignment->getProcessorSpeed();
    if (result.finishTime == std::numeric_limits<double>::max()) {
        cout << "perceivedFinishTime inf" << endl;
        return;
    }
    result.processorOfAssignment->setReadyTimeCompute(result.finishTime);
    result.modifiedProcs = modifiedProcs;
}

void tentativeAssignmentHEFT(vertex_t* v, bool real, SchedulingResult& result, SchedulingResult& resultCorrect)
{
    // cout<<"tent on proc "<<ourModifiedProc->id<< " ";
    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    double timeToRun = real ? v->time * v->factorForRealExecution : v->time;

    double sumOut = getSumOut(v);

    if (result.processorOfAssignment->getMemorySize() < sumOut) {
        //  cout<<"too large outs absolutely"<<endl;
        result.finishTime = std::numeric_limits<double>::max();
        return;
    }

    // cout<<"sumOut includes ";
    realSurplusOfOutgoingEdges(v, resultCorrect.processorOfAssignment, sumOut);

    vector<std::shared_ptr<Processor>> modifiedProcs, modifiedProcsCorrect;
    modifiedProcs.emplace_back(result.processorOfAssignment);
    modifiedProcsCorrect.emplace_back(resultCorrect.processorOfAssignment);

    processIncomingEdges(v, false, result.processorOfAssignment, modifiedProcs, result.startTime);
    processIncomingEdges(v, real, resultCorrect.processorOfAssignment, modifiedProcsCorrect, resultCorrect.startTime);

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    // both processIncomingEdges do the same, so start times will be the same
    result.startTime = result.processorOfAssignment->getReadyTimeCompute() > result.startTime ? result.processorOfAssignment->getReadyTimeCompute() : result.startTime;

    resultCorrect.startTime = resultCorrect.processorOfAssignment->getReadyTimeCompute() > resultCorrect.startTime ? resultCorrect.processorOfAssignment->getReadyTimeCompute() : resultCorrect.startTime;

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    if (resultCorrect.processorOfAssignment->getAvailableMemory() < sumOut) {
        // only the correct result knows about kicking

        double stillNeedsToBeEvictedToRun = sumOut - resultCorrect.processorOfAssignment->getAvailableMemory();
        double writeTime = resultCorrect.startTime > resultCorrect.processorOfAssignment->getReadyTimeCompute()
            ? resultCorrect.startTime
            : result.processorOfAssignment->getReadyTimeCompute();

        for (auto it = resultCorrect.processorOfAssignment->getPendingMemories().begin();
            it != resultCorrect.processorOfAssignment->getPendingMemories().end() && stillNeedsToBeEvictedToRun > 0;) {
            //  print_edge(*it);
            if ((*it)->head->name != v->name) {
                double weightForTime = real ? (*it)->weight * (*it)->factorForRealExecution : (*it)->weight;
                stillNeedsToBeEvictedToRun -= (*it)->weight;
                writeTime += weightForTime / resultCorrect.processorOfAssignment->writeSpeedDisk;
                it = resultCorrect.processorOfAssignment->removePendingMemory(*it);
            } else {
                ++it;
            }
        }
        if (stillNeedsToBeEvictedToRun > 0) {
            throw runtime_error("stillNeedsToBeEvictedToRun > 0");
        }
        assert(stillNeedsToBeEvictedToRun <= 0);
        assert(resultCorrect.processorOfAssignment->getAvailableMemory() >= sumOut);
        resultCorrect.startTime = writeTime;
        resultCorrect.processorOfAssignment->setReadyTimeWrite(writeTime);
        resultCorrect.processorOfAssignment->setReadyTimeCompute(writeTime);
        assert(resultCorrect.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    }

    double Res = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(v, resultCorrect.processorOfAssignment);
    result.peakMem = (Res < 0) ? 1 : (result.processorOfAssignment->getMemorySize() - Res) / result.processorOfAssignment->getMemorySize();

    result.finishTime = result.startTime + v->time / result.processorOfAssignment->getProcessorSpeed();
    result.processorOfAssignment->setReadyTimeCompute(result.finishTime);

    if (Res < 0) {
        // try finish times with and without memory overflow
        double amountToOffload = -Res;

        resultCorrect.finishTime = resultCorrect.startTime + timeToRun / resultCorrect.processorOfAssignment->getProcessorSpeed() + amountToOffload / resultCorrect.processorOfAssignment->memoryOffloadingPenalty;
        assert(resultCorrect.finishTime > resultCorrect.startTime);

        if (result.finishTime == std::numeric_limits<double>::max()) {
            cout << "perceivedFinishTime inf" << endl;
            resultCorrect.finishTime = std::numeric_limits<double>::max();
            return;
        }
        assert(resultCorrect.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
        resultCorrect.processorOfAssignment->setReadyTimeCompute(resultCorrect.finishTime);
        assert(resultCorrect.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    } else {
        resultCorrect.finishTime = resultCorrect.startTime + timeToRun / resultCorrect.processorOfAssignment->getProcessorSpeed();
        resultCorrect.processorOfAssignment->setReadyTimeCompute(resultCorrect.finishTime);
    }
    result.modifiedProcs = modifiedProcs;
    resultCorrect.modifiedProcs = modifiedProcsCorrect;
    result.resultingVar = 1;
    resultCorrect.resultingVar = 1;
}

void evictAccordingToBestDecision(int& numberWithEvictedCases, SchedulingResult& bestSchedulingResult, vertex_t* pVertex,
    bool real)
{
    switch (bestSchedulingResult.resultingVar) {
    case 1:
        break;
    case 2:
        // cout<<"best with 1 kick"<<endl;
        assert(bestSchedulingResult.edgeToKick != nullptr);
        bestSchedulingResult.processorOfAssignment->delocateToDisk(bestSchedulingResult.edgeToKick, real);
        numberWithEvictedCases++;
        checkIfPendingMemoryCorrect(bestSchedulingResult.processorOfAssignment);
        break;
    case 3:
        // cout<<"best with all kick"<<endl;
        for (auto it = bestSchedulingResult.processorOfAssignment->getPendingMemories().begin();
            it != bestSchedulingResult.processorOfAssignment->getPendingMemories().end();) {
            if ((*it)->head->name != pVertex->name) {
                it = bestSchedulingResult.processorOfAssignment->delocateToDisk(*it, real);
            } else {
                it++;
            }
        }
        assert(bestSchedulingResult.processorOfAssignment->getPendingMemories().empty()
            || (*bestSchedulingResult.processorOfAssignment->getPendingMemories().begin())->head->name == pVertex->name);
        numberWithEvictedCases++;
        checkIfPendingMemoryCorrect(bestSchedulingResult.processorOfAssignment);
        break;
    default:
        throw runtime_error("");
    }
}

void putChangeOnCluster(vertex_t* vertex, SchedulingResult& schedulingResult, Cluster* cluster, int& numberWithEvictedCases,
    bool real, bool isHeft)
{
    evictAccordingToBestDecision(numberWithEvictedCases, schedulingResult, vertex, real);

    for (auto& modifiedProc : schedulingResult.modifiedProcs) {
        checkIfPendingMemoryCorrect(modifiedProc);
        auto procInClusterWithId = cluster->getProcessorById(modifiedProc->id);
        procInClusterWithId->updateFrom(*modifiedProc);
    }

    assert(schedulingResult.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    vertex->assignedProcessorId = schedulingResult.processorOfAssignment->id;

    for (int j = 0; j < vertex->in_degree; j++) {
        edge* ine = vertex->in_edges[j];

        int onWhichProcessor = whatProcessorIsLocatedOn(ine, !real);
        assert(onWhichProcessor == -1 || onWhichProcessor == schedulingResult.processorOfAssignment->id || cluster->getProcessorById(onWhichProcessor)->getPendingMemories().find(ine) == cluster->getProcessorById(onWhichProcessor)->getPendingMemories().end());

        if (onWhichProcessor == schedulingResult.processorOfAssignment->id) {
            schedulingResult.processorOfAssignment->delocateToDisk(ine, !real);
            if (!real) {
                // put edge back, because this is not the true delocation
                locateToThisProcessorFromNowhere(ine, onWhichProcessor);
            }
        } else {
            if (onWhichProcessor != -1) {
                cluster->getProcessorById(onWhichProcessor)->delocateToDiskOptionally(ine, real);
            } else {
                // edge has been read
                // cout<<"bla"<<endl;
                auto proc = findProcessorThatHoldsEdge(ine, cluster);
                assert(proc == nullptr);
            }
            // if(isHeft && onWhichProcessor!=-1){
            //    if(!real)
            //       cluster->getProcessorById(onWhichProcessor)->delocateToDisk(ine);
            //   else
            //       cluster->getProcessorById(onWhichProcessor)->removePendingMemory(ine);
            // }
        }
        if (real)
            ine->locations.clear();
    }

    checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);

    for (int i = 0; i < vertex->out_degree; i++) {
        auto v1 = vertex->out_edges[i];
        schedulingResult.processorOfAssignment->loadFromNowhere(v1);
        checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);
        if (schedulingResult.processorOfAssignment->getAvailableMemory() < 0) {
            cout << "";
        }
    }
    cluster->getProcessorById(schedulingResult.processorOfAssignment->id)->updateFrom(*schedulingResult.processorOfAssignment);
    for (const auto& item : cluster->getProcessors()) {
        checkIfPendingMemoryCorrect(item.second);
    }
}

void realSurplusOfOutgoingEdges(const vertex_t* v, shared_ptr<Processor>& ourModifiedProc, double& sumOut)
{
    for (int i = 0; i < v->in_degree; i++) {
        auto inEdge = v->in_edges[i];
        if (isLocatedOnThisProcessor(inEdge, ourModifiedProc->id)) {
            //     cout<<"in is located here "; print_edge(v->in_edges[i]);
            auto pendingOfProc = ourModifiedProc->getPendingMemories();
            // assert(pendingOfProc.find(inEdge) != pendingOfProc.end());
            if (pendingOfProc.find(inEdge) != pendingOfProc.end()) {
                sumOut -= inEdge->weight;
            } else {
                // cout<<"edge "<<buildEdgeName(inEdge)<<" not anymore found in pending mems of processor "<<ourModifiedProc->id<<endl;
            }
        }
    }
    //  cout << "REQUIRES AT THE END: " << sumOut << endl;
}

void processIncomingEdges(const vertex_t* v, bool real, shared_ptr<Processor>& ourModifiedProc,
    vector<std::shared_ptr<Processor>>& modifiedProcs,
    double& earliestStartingTimeToComputeVertex)
{
    earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeCompute();
    for (int j = 0; j < v->in_degree; j++) {
        edge* incomingEdge = v->in_edges[j];
        vertex_t* predecessor = incomingEdge->tail;

        double edgeWeightToUse = real ? incomingEdge->weight * incomingEdge->factorForRealExecution
                                      : incomingEdge->weight;

        if (predecessor->assignedProcessorId == ourModifiedProc->id) {
            if (!isLocatedOnThisProcessor(incomingEdge, ourModifiedProc->id, !real)) {
                assert(isLocatedOnDisk(incomingEdge, !real));
                ourModifiedProc->setReadyTimeRead(
                    ourModifiedProc->getReadyTimeRead() + edgeWeightToUse / ourModifiedProc->readSpeedDisk);
                earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeRead() > earliestStartingTimeToComputeVertex ? ourModifiedProc->getReadyTimeRead() : earliestStartingTimeToComputeVertex;
            }

        } else {
            if (isLocatedOnDisk(incomingEdge, !real)) {
                // we need to schedule read
                ourModifiedProc->setReadyTimeRead(
                    ourModifiedProc->getReadyTimeRead() + edgeWeightToUse / ourModifiedProc->readSpeedDisk);
                earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeRead() > earliestStartingTimeToComputeVertex ? ourModifiedProc->getReadyTimeRead() : earliestStartingTimeToComputeVertex;
                // TODO evict??
            } else {
                auto predecessorsProcessorsId = predecessor->assignedProcessorId;
                assert(isLocatedOnThisProcessor(incomingEdge, predecessorsProcessorsId, !real));
                shared_ptr<Processor> addedProc;
                auto it = // modifiedProcs.size()==1?
                          //   modifiedProcs.begin():
                    std::find_if(modifiedProcs.begin(), modifiedProcs.end(),
                        [predecessorsProcessorsId](const shared_ptr<Processor>& p) {
                            return p->id == predecessorsProcessorsId;
                        });

                if (it == modifiedProcs.end()) {
                    Cluster* cluster = real ? actualCluster : imaginedCluster;
                    addedProc = make_shared<Processor>(*cluster->getProcessorById(predecessorsProcessorsId));
                    // cout<<"adding modified proc "<<addedProc->id<<endl;
                    modifiedProcs.emplace_back(addedProc);
                    checkIfPendingMemoryCorrect(addedProc);
                } else {
                    addedProc = *it;
                }

                assert(!hasDuplicates(modifiedProcs));

                double whichMakespan = real ? predecessor->makespan : predecessor->makespanPerceived;
                double timeToStartWriting = max(whichMakespan, addedProc->getReadyTimeWrite());
                addedProc->setReadyTimeWrite(timeToStartWriting + edgeWeightToUse / addedProc->writeSpeedDisk);
                double startTimeOfRead = max(addedProc->getReadyTimeWrite(), ourModifiedProc->getReadyTimeRead());
                double endTimeOfRead = startTimeOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk;
                ourModifiedProc->setReadyTimeRead(endTimeOfRead);

                earliestStartingTimeToComputeVertex = max(earliestStartingTimeToComputeVertex, endTimeOfRead);
                // int addpl  = addedProc->pendingMemories.size();
                addedProc->removePendingMemory(incomingEdge);
                // assert(addpl> addedProc->pendingMemories.size());
                checkIfPendingMemoryCorrect(addedProc);
            }
        }
    }
}

graph_t* convertToNonMemRepresentation(graph_t* withMemories, map<int, int>& noMemToWithMem)
{
    enforce_single_source_and_target(withMemories);
    graph_t* noNodeMemories = new_graph();

    for (vertex_t* vertex = withMemories->source; vertex; vertex = next_vertex_in_sorted_topological_order(withMemories,
                                                              vertex,
                                                              &sort_by_increasing_top_level)) {
        vertex_t* invtx = new_vertex(noNodeMemories, vertex->name + "-in", vertex->time, nullptr);
        noMemToWithMem.insert({ invtx->id, vertex->id });
        if (!noNodeMemories->source) {
            noNodeMemories->source = invtx;
        }
        vertex_t* outvtx = new_vertex(noNodeMemories, vertex->name + "-out", 0.0, nullptr);
        noMemToWithMem.insert({ outvtx->id, vertex->id });
        edge_t* e = new_edge(noNodeMemories, invtx, outvtx, vertex->memoryRequirement, nullptr);
        noNodeMemories->target = outvtx;

        for (int i = 0; i < vertex->in_degree; i++) {
            edge* inEdgeOriginal = vertex->in_edges[i];
            string expectedName = inEdgeOriginal->tail->name + "-out";
            vertex_t* outVtxOfCopiedInVtxOfEdge = findVertexByName(noNodeMemories, expectedName);

            if (outVtxOfCopiedInVtxOfEdge == nullptr) {
                print_graph_to_cout(noNodeMemories);
                outVtxOfCopiedInVtxOfEdge = findVertexByName(noNodeMemories, expectedName);
                cout << "expected: " << expectedName << endl;
                throw std::invalid_argument(" no vertex found for expected name.");
            }
            edge_t* e_new = new_edge(noNodeMemories, outVtxOfCopiedInVtxOfEdge, invtx, inEdgeOriginal->weight, nullptr);
        }
    }

    return noNodeMemories;
}

double calculateSimpleBottomUpRank(vertex_t* task)
{
    //    cout<<"rank for "<<task->name<<" ";

    double maxCost = 0.0;
    for (int j = 0; j < task->out_degree; j++) {
        double communicationCost = task->out_edges[j]->weight;
        // cout<<communicationCost<<" ";
        if (task->out_edges[j]->head->bottom_level == -1) {
            // cout<<"-1"<<endl;
            task->out_edges[j]->head->bottom_level = calculateSimpleBottomUpRank(task->out_edges[j]->head);
            // cout<<"then "<<task->out_edges[j]->head->bottom_level<<endl;
        }
        double successorCost = task->out_edges[j]->head->bottom_level; // calculateSimpleBottomUpRank(task->out_edges[j]->head);
        double cost = communicationCost + successorCost;
        maxCost = max(maxCost, cost);
    }
    // cout<<endl;
    double retur = (task->time + maxCost);
    task->bottom_level = retur;
    // cout<<"result "<<retur<<endl;
    return retur;
}

double calculateBLCBottomUpRank(vertex_t* task)
{

    double maxCost = 0.0;
    for (int j = 0; j < task->out_degree; j++) {
        double communicationCost = task->out_edges[j]->weight;
        double successorCost = calculateBLCBottomUpRank(task->out_edges[j]->head);
        double cost = communicationCost + successorCost;
        maxCost = max(maxCost, cost);
    }
    double simpleBl = task->time + maxCost;

    double maxInputCost = 0.0;
    for (int j = 0; j < task->in_degree; j++) {
        double communicationCost = task->in_edges[j]->weight;
        maxInputCost = max(maxInputCost, communicationCost);
    }
    double retur = simpleBl + maxInputCost;
    return retur;
}

std::vector<std::pair<vertex_t*, double>> calculateMMBottomUpRank(graph_t* graphWMems)
{

    map<int, int> noMemToWithMem;
    graph_t* graph = convertToNonMemRepresentation(graphWMems, noMemToWithMem);
    // print_graph_to_cout(graph);

    SP_tree_t* sp_tree = nullptr;
    graph_t* sp_graph = nullptr;

    enforce_single_source_and_target(graph);
    sp_tree = build_SP_decomposition_tree(graph);
    if (sp_tree) {
        sp_graph = graph;
    } else {
        sp_graph = graph_sp_ization(graph);
        sp_tree = build_SP_decomposition_tree(sp_graph);
    }

    std::vector<std::pair<vertex_t*, int>> scheduleOnOriginal;

    if (sp_tree) {
        vertex_t** schedule = compute_optimal_SP_traversal(sp_graph, sp_tree);

        for (int i = 0; i < sp_graph->number_of_vertices; i++) {
            vertex_t* vInSp = schedule[i];
            // cout<<vInSp->name<<endl;
            const map<int, int>::iterator& it = noMemToWithMem.find(vInSp->id);
            if (it != noMemToWithMem.end()) {
                vertex_t* vertexWithMem = graphWMems->vertices_by_id[(*it).second];
                if (std::find_if(scheduleOnOriginal.begin(), scheduleOnOriginal.end(),
                        [vertexWithMem](std::pair<vertex_t*, int> p) {
                            return p.first->name == vertexWithMem->name;
                        })
                    == scheduleOnOriginal.end()) {
                    scheduleOnOriginal.emplace_back(vertexWithMem,
                        sp_graph->number_of_vertices - i); // TODO: #vertices - i?
                }
            }
        }

    } else {
        throw runtime_error("No tree decomposition");
    }
    delete sp_tree;
    delete sp_graph;
    // delete graph;

    std::vector<std::pair<vertex_t*, double>> double_vector;

    // Convert each pair from (vertex_t*, int) to (vertex_t*, double)
    for (const auto& pair : scheduleOnOriginal) {
        double_vector.emplace_back(pair.first, static_cast<double>(pair.second));
    }

    return double_vector;
}

vector<pair<vertex_t*, double>> buildRanksWalkOver(graph_t* graph)
{
    vector<pair<vertex_t*, double>> ranks;
    enforce_single_source_and_target(graph);
    int rank = 0;
    vertex_t* vertex = graph->first_vertex;
    while (vertex != nullptr) {
        if (vertex->in_degree == 0) {
            ranks.emplace_back(vertex, rank);
        }
    }
    for (auto& item : ranks) {
        for (int i = 0; i < item.first->out_degree; i++) {
            //    if(find)
        }
    }
}

vector<pair<vertex_t*, double>> calculateBottomLevels(graph_t* graph, int bottomLevelVariant)
{
    vector<pair<vertex_t*, double>> ranks;
    switch (bottomLevelVariant) {
    case 1: {
        vertex_t* vertex = graph->first_vertex;
        while (vertex != nullptr) {
            double rank = calculateSimpleBottomUpRank(vertex);
            // cout<<"rank for "<<vertex->name<<" is "<<rank<<endl;
            ranks.emplace_back(vertex, rank);
            vertex = vertex->next;
        }
        break;
    }
    case 2: {
        vertex_t* vertex = graph->first_vertex;
        while (vertex != nullptr) {
            double rank = calculateBLCBottomUpRank(vertex);
            ranks.emplace_back(vertex, rank);
            vertex = vertex->next;
        }
        break;
    }
    case 3:
        ranks = calculateMMBottomUpRank(graph);
        break;
    default:
        throw runtime_error("unknon algorithm");
    }
    return ranks;
}

[[maybe_unused]] inline void checkIfPendingMemoryCorrect(const shared_ptr<Processor>& p)
{
    double sumOut = 0;
    for (auto pendingMemorie : p->getPendingMemories()) {
        sumOut += pendingMemorie->weight;
    }
    double busy = p->getAvailableMemory() + sumOut;
    if (abs(p->getMemorySize() - busy) > 0.1)
        //  cout<<"check "<<p->getMemorySize()<<" vs "<< busy<<endl;
        assert(abs(p->getMemorySize() - busy) < 0.1);
    assert(p->getReadyTimeCompute() < std::numeric_limits<double>::max());
}

[[maybe_unused]] inline bool hasDuplicates(const std::vector<shared_ptr<Processor>>& vec)
{
    /*std::unordered_set<int> seenIds;
    for (const auto& obj : vec) {
        if (!seenIds.insert(obj->id).second) {
            // Insert returns {iterator, false} if the value already exists
            return true;
        }
    }
    return false; */
    return false;
}

shared_ptr<Processor> findProcessorThatHoldsEdge(edge_t* incomingEdge, Cluster* clusterToLookIn)
{

    for (auto& pair : clusterToLookIn->getProcessors()) {
        auto iterator = std::find_if(pair.second->getPendingMemories().begin(), pair.second->getPendingMemories().end(),
            [incomingEdge](edge_t* edge) {
                return incomingEdge == edge;
            });
        if (iterator != pair.second->getPendingMemories().end()) {
            return pair.second;
        }
    }
    return nullptr;
}
