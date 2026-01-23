#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "fonda_scheduler/algorithms.hpp"
#include "fonda_scheduler/options.hpp"

#include <iterator>
#include <queue>

Cluster* imaginedCluster;
Cluster* actualCluster;

std::vector<std::shared_ptr<Event>> medih2(graph_t* graph, int algoNum, double& runtime){
    const bool isHeft = (algoNum == fonda_scheduler::ALGORITHMS::HEFT);
    if (isHeft) {
        imaginedCluster->mayBecomeInvalid();
    }
    auto start = std::chrono::system_clock::now();
    std::vector<std::pair<vertex_t*, double>> ranks = calculateBottomLevels(graph, algoNum);
    removeSourceAndTarget(graph, ranks);


    std::unordered_map<vertex_t*, int> remaining_preds;
    std::priority_queue<
        vertex_t*,
        std::vector<vertex_t*>,
        PriorityRankComparator> readyQ;

    for (auto v : graph->vertices_by_id) {
        remaining_preds[v.second] = v.second->in_edges.size();
        if (remaining_preds[v.second] == 0) {
            readyQ.push(v.second);
        }
    }

    double makespan = 0;int numProcessedVertices=0;
    int numberWithEvictedCases = 0;

    std::vector<std::shared_ptr<Event>> res_events;
    while (!readyQ.empty()) {
        vertex_t* vertex = readyQ.top();
        readyQ.pop();
       // std::cout<<"deal w "<<vertex->name<<std::endl;
        numProcessedVertices++;
        SchedulingResult bestSchedulingResult(nullptr);
        SchedulingResult bestSchedulingResultCorrectForHeftOnly(nullptr);
        //  cout << "imagine" << endl;
        start = std::chrono::system_clock::now();

        if (vertex->name=="BWA_MEM_00000682") {
            std::cout << "";
        }
        if (vertex->name=="TRIMGALORE_00000002") {
            std::cout << "";
        }


        auto newevents = bestTentativeAssignment2(isHeft, vertex, bestSchedulingResult, bestSchedulingResultCorrectForHeftOnly);
        checkIfPendingMemoryCorrect(bestSchedulingResult.processorOfAssignment);
        res_events.insert(res_events.end(), newevents.begin(), newevents.end());

      //  for (auto newevent : newevents) {
      //      newevent->printEventDetailed();
       // }

        if (bestSchedulingResult.modifiedProcs.empty()) {
            std::cout << "Invalid assignment of " << vertex->name;
            return {};
        }

        assert((*newevents.rbegin())->id== vertex->name+"-f");
        assert((*newevents.rbegin())->getExpectedTimeFire()== bestSchedulingResult.finishTime);
        /*    std::cout  << vertex->name << " " <<
                     " "<< bestSchedulingResult.startTime << " --- "
                         << bestSchedulingResult.finishTime << " on "
                          << bestSchedulingResult.processorOfAssignment->id
                          << " variant " << bestSchedulingResult.resultingVar
                              <<std::endl;*/
       /* std::cout << "REAL " << vertex->name << " " << bestSchedulingResultOnReal.startTime //<< " --- "
                  << " " << bestSchedulingResultOnReal.finishTime //<< " on proc "
                  << " " << bestSchedulingResultOnReal.processorOfAssignment->id
                  << " duration " << bestSchedulingResultOnReal.finishTime - bestSchedulingResultOnReal.startTime
                  << " variant " << bestSchedulingResultOnReal.resultingVar
                  // <<" with av mem "<<bestSchedulingResultOnReal.processorOfAssignment->getAvailableMemory()
                  << std::endl;
*/

        putChangeOnCluster(vertex, bestSchedulingResult, actualCluster, numberWithEvictedCases, false, isHeft);

        for (auto& item : newevents) {
            item->processor = actualCluster->getProcessorById(item->processor->id);
        }

        for (auto& item : newevents) {
            events.insert(item);
        }


        vertex->makespanPerceived = bestSchedulingResult.finishTime;
        assert(bestSchedulingResult.startTime < bestSchedulingResult.finishTime);

        if (makespan < bestSchedulingResult.finishTime)
            makespan = bestSchedulingResult.finishTime;

        for (const auto& out_edge : vertex->out_edges) {
            vertex_t* succ = out_edge->head;
            remaining_preds[succ]--;
            if (remaining_preds[succ] == 0) {
                readyQ.push(succ);
            }
        }

        vertex->status = Scheduled;


       // std::cout << std::endl<< "[STATIC] Vertex " << vertex->name << " can start at: " << bestSchedulingResult.startTime   <<" on proc "<<bestSchedulingResult.processorOfAssignment->id

     //   <<" end at " <<bestSchedulingResult.finishTime<< std::endl<< std::endl;
    }

    assert(numProcessedVertices==graph->vertices_by_id.size());
    auto  end = std::chrono::system_clock::now();
    auto elapsed_seconds = end - start;
    runtime += elapsed_seconds.count();

    return res_events;

}

std::vector<std::shared_ptr<Event>>  bestTentativeAssignmentHEFT2(const vertex_t* vertex, SchedulingResult& result, SchedulingResult& correctResultForHeftOnly)
{
    result.finishTime = std::numeric_limits<double>::max();
    result.startTime = 0;

    std::vector<std::shared_ptr<Event>> bestEvents;

    for (const auto& [id, processor] : actualCluster->getProcessors()) {
        SchedulingResult tentativeResult(processor);
        SchedulingResult correctTentativeResultForHeftOnly(actualCluster->getProcessorById(processor->id));

        checkIfPendingMemoryCorrect(processor);
         std::vector<std::shared_ptr<Event>> events = tentativeAssignmentHEFT2(vertex, false, tentativeResult, correctTentativeResultForHeftOnly);


        double finTime =tentativeResult.finishTime;
        double epsilon = 1e-9;

        bool isBetterTime = (finTime < result.finishTime - epsilon);
        bool isSameTime = (std::abs(finTime - result.finishTime) < epsilon);

        // Tie-break: Smallest Memory, then Smallest ID
        bool isBetterTieBreak = false;
        if (isSameTime && result.processorOfAssignment != nullptr) {
            size_t currentMem = processor->getMemorySize();
            size_t bestMem = result.processorOfAssignment->getMemorySize();

            if (currentMem < bestMem) {
                isBetterTieBreak = true;
            } else if (currentMem == bestMem) {
                if (processor->id < result.processorOfAssignment->id) { // Force smallest ID
                    isBetterTieBreak = true;
                }
            }
        }

        if (isBetterTime || isBetterTieBreak) {
            //    std::cout << "best actualize to " << tentativeResult.processorOfAssignment->id << " should be free " << tentativeResult.shouldBeFreeOnProcessorDuringTask << std::endl;
            assert(!tentativeResult.modifiedProcs.empty());
            result = tentativeResult;
            correctResultForHeftOnly = correctTentativeResultForHeftOnly;
            result.resultingVar = 1;
            bestEvents = events;
        }
    }
}


std::vector<std::shared_ptr<Event>>  bestTentativeAssignmentMEDIH2(vertex_t* vertex, SchedulingResult& result)
{
    result.finishTime = std::numeric_limits<double>::max();
    result.startTime = 0;
    std::vector<std::shared_ptr<Event>> bestEvents;


    for (auto& [id, processor] : actualCluster->getProcessors()) {
        SchedulingResult tentativeResult(processor);

        checkIfPendingMemoryCorrect(processor);
        std::vector<std::shared_ptr<Event>> createdEVents = tentativeAssignment2(vertex,  tentativeResult);
        checkIfPendingMemoryCorrect(tentativeResult.processorOfAssignment);

        double finTime =tentativeResult.finishTime;
        double epsilon = 1e-9;

        bool isBetterTime = (finTime < result.finishTime - epsilon);
        bool isSameTime = (std::abs(finTime - result.finishTime) < epsilon);

        // Tie-break: Smallest Memory, then Smallest ID
        bool isBetterTieBreak = false;
        if (isSameTime && result.processorOfAssignment != nullptr) {
            size_t currentMem = processor->getMemorySize();
            size_t bestMem = result.processorOfAssignment->getMemorySize();

            if (currentMem < bestMem) {
                isBetterTieBreak = true;
            } else if (currentMem == bestMem) {
                if (processor->id < result.processorOfAssignment->id) { // Force smallest ID
                    isBetterTieBreak = true;
                }
            }
        }

        if (isBetterTime || isBetterTieBreak) {
        //    std::cout << "best actualize to " << tentativeResult.processorOfAssignment->id << " should be free " << tentativeResult.shouldBeFreeOnProcessorDuringTask << std::endl;
            assert(!tentativeResult.modifiedProcs.empty());
            result = tentativeResult;
            bestEvents = createdEVents;
        }
    }

    return bestEvents;
}


std::vector<std::shared_ptr<Event>>  bestTentativeAssignment2(const bool isHeft, vertex_t* vertex, SchedulingResult& result,
    SchedulingResult& correctResultForHeftOnly)
{
    std::vector<std::shared_ptr<Event>> eventsnew;
    if (isHeft) {
       eventsnew =  bestTentativeAssignmentHEFT2(vertex, result, correctResultForHeftOnly);
    } else {
      eventsnew =   bestTentativeAssignmentMEDIH2(vertex, result);
    }
    return eventsnew;
}

std::vector<std::shared_ptr<Event>>  tentativeAssignment2(vertex_t* v, SchedulingResult& result)
{

    if (v->name=="CHECK_DESIGN_00000504" && result.processorOfAssignment->id==30) {
        std::cout << "";
    }

    if (v->name=="MERGED_LIB_BAM_FILTER_00000673" && result.processorOfAssignment->id==35) {
        std::cout << "";
    }
    if (v->name=="SORT_BAM_00000688" && result.processorOfAssignment->id==13) {
        std::cout << "";
    }

    if (v->name=="MAKE_TSS_BED_00000854" && result.processorOfAssignment->id==7) {
        std::cout << "";
    }




    if (result.processorOfAssignment->getMemorySize() < outMemoryRequirement(v) || result.processorOfAssignment->getMemorySize() < inMemoryRequirement(v)) {
      //  std::cout<<"too large outs absolutely on " <<result.processorOfAssignment->id<<std::endl;
        result.finishTime = std::numeric_limits<double>::max();
        return {};
    }

    double sumOut = outMemoryRequirement(v);
    realSurplusOfOutgoingEdges(v, result.processorOfAssignment, sumOut);

    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    modifiedProcs.emplace_back(result.processorOfAssignment);
    std::vector<std::shared_ptr<Event>>  createdEvents;
    processIncomingEdges2(v, result.processorOfAssignment, modifiedProcs, result.startTime,createdEvents);


    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    checkIfPendingMemoryCorrect(result.processorOfAssignment);

    result.startTime =  std::max(result.processorOfAssignment->getReadyTimeCompute() , result.startTime);

    const double Res = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(v, result.processorOfAssignment);

    std::vector<std::shared_ptr<Event>>  additionalEventsFromEvict1, additionalEventsFromEvictAll;

    auto eventStartTask = Event::createEvent(v, nullptr, OnTaskStart, result.processorOfAssignment,
       result.startTime, result.startTime, false,
       v->name + "-s");
    auto eventFinishTask = Event::createEvent(v, nullptr, OnTaskFinish, result.processorOfAssignment,
        result.startTime, result.startTime, false,
        v->name + "-f");
    eventFinishTask->addPredecessorInPlanning(eventStartTask);
    for (auto created_event : createdEvents) {
        eventStartTask->addPredecessorInPlanning(created_event);
    }


    if (Res < 0) {
        // try finish times with and without memory overflow
        const double amountToOffload = -Res;

        double timeToFinishNoEvicted = finishTimeWithMemorySwapping(result.startTime, amountToOffload, v->time, v, result.processorOfAssignment);
        assert(timeToFinishNoEvicted > result.startTime);

        if (sumOut > result.processorOfAssignment->getAvailableMemory()) {
            // cout<<"cant"<<endl;
            timeToFinishNoEvicted = std::numeric_limits<double>::max();
        }

        double timeToTaskFinishBiggestEvicted = std::numeric_limits<double>::max(),
               timeToTaskFinishAllEvicted = std::numeric_limits<double>::max();
        double timeToWriteAllPending = 0;
        std::vector<EdgeChange> changedEdgesOne, changedEdgesAll;

        double startTimeFor1Evicted = result.processorOfAssignment->getReadyTimeWrite() > result.startTime ? result.processorOfAssignment->getReadyTimeWrite() : result.startTime;
        double startTimeForAllEvicted = startTimeFor1Evicted;

        double finishTimeWrite1Evict, finishTimeWriteAllEvict;
        double availableMem1Evict, availableMemAllEvict;
        auto biggestPendingEdge = result.processorOfAssignment->getBiggestPendingEdgeThatIsNotIncomingOfAndLocatedOnProc(
            v);

        if (!result.processorOfAssignment->getPendingMemories().empty() && biggestPendingEdge != nullptr) {
            assert((*result.processorOfAssignment->getPendingMemories().begin())->weight >= (*result.processorOfAssignment->getPendingMemories().rbegin())->weight);

            const auto biggestFileWeight = biggestPendingEdge->weight;
            const double amountToOffloadWithoutBiggestFile = (amountToOffload - biggestFileWeight) > 0 ? (amountToOffload - biggestFileWeight) : 0;
            availableMem1Evict= result.processorOfAssignment->getAvailableMemory() - amountToOffload + biggestFileWeight;
            const double startTimeToWriteBiggestEdge = std::max(result.processorOfAssignment->getReadyTimeWrite(),  biggestPendingEdge->tail->makespanPerceived);
            finishTimeWrite1Evict = startTimeToWriteBiggestEdge + biggestPendingEdge->weight / result.processorOfAssignment->writeSpeedDisk;
            changedEdgesOne.emplace_back(biggestPendingEdge, Location(LocationType::OnDisk, std::nullopt, finishTimeWrite1Evict));

            auto writeStart= Event::createEvent(nullptr, biggestPendingEdge, OnWriteStart, result.processorOfAssignment,startTimeToWriteBiggestEdge, startTimeToWriteBiggestEdge, false, buildEdgeName(biggestPendingEdge) + "-w-s");
            auto writeFinish= Event::createEvent(nullptr, biggestPendingEdge, OnWriteFinish, result.processorOfAssignment,finishTimeWrite1Evict, finishTimeWrite1Evict, false, buildEdgeName(biggestPendingEdge) + "-w-f");
            writeFinish->addPredecessorInPlanning(writeStart);

            const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find(biggestPendingEdge->tail->name + "-f");
            if (eventFinishPredecessorComputing != nullptr) {
                const double prev = eventFinishPredecessorComputing->getActualTimeFire();
                writeStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
                assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
            } else {
                if (biggestPendingEdge->tail->status == Status::Finished) {
                    std::cout << "no event finish predecessor - because tail is finished" << '\n';
                } else {
                    std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
                }
            }

            additionalEventsFromEvict1.emplace_back(writeStart);
            additionalEventsFromEvict1.emplace_back(writeFinish);


            startTimeFor1Evicted = std::max(result.startTime, finishTimeWrite1Evict);
            timeToTaskFinishBiggestEvicted =
                finishTimeWithMemorySwapping(startTimeFor1Evicted, amountToOffloadWithoutBiggestFile, v->time, v, result.processorOfAssignment);

            //startTimeFor1Evicted
               // + timeToRun / result.processorOfAssignment->getProcessorSpeed() + amountToOffloadWithoutBiggestFile / result.processorOfAssignment->memoryOffloadingPenalty;
            assert(timeToTaskFinishBiggestEvicted > startTimeFor1Evicted);

            const double availableMemWithoutBiggest = result.processorOfAssignment->getAvailableMemory() + biggestFileWeight;
            if (sumOut > availableMemWithoutBiggest) {
                timeToTaskFinishBiggestEvicted = std::numeric_limits<double>::max();
            }



            double sumWeightsOfAllPending = 0;
            finishTimeWriteAllEvict = result.processorOfAssignment->getReadyTimeWrite();

            double stillNeedsToBeEvictedToRun = amountToOffload;

            for (auto it = result.processorOfAssignment->getPendingMemories().begin();
                it != result.processorOfAssignment->getPendingMemories().end() && stillNeedsToBeEvictedToRun > 0;) {
                if ((*it)->head->name != v->name) {
                    const double startTimeWrite = std::max(finishTimeWriteAllEvict,
                     (*it)->tail->makespanPerceived);
                    const double itemWeightToWrite = (*it)->weight;

                    timeToWriteAllPending += itemWeightToWrite / result.processorOfAssignment->writeSpeedDisk;
                    finishTimeWriteAllEvict = startTimeWrite + itemWeightToWrite / result.processorOfAssignment->writeSpeedDisk;
                    changedEdgesAll.emplace_back((*it), Location(LocationType::OnDisk, std::nullopt, finishTimeWriteAllEvict));
                    sumWeightsOfAllPending += (*it)->weight;
                    stillNeedsToBeEvictedToRun -= (*it)->weight;

                     writeStart= Event::createEvent(nullptr, (*it), OnWriteStart, result.processorOfAssignment,startTimeWrite, startTimeWrite, false, buildEdgeName( (*it)) + "-w-s");
                     writeFinish= Event::createEvent(nullptr, (*it), OnWriteFinish, result.processorOfAssignment,finishTimeWriteAllEvict, finishTimeWriteAllEvict, false, buildEdgeName( (*it)) + "-w-f");
                    writeFinish->addPredecessorInPlanning(writeStart);
                    if (!additionalEventsFromEvictAll.empty())
                        writeStart->addPredecessorInPlanning((*additionalEventsFromEvictAll.rbegin()));
                    else if (!createdEvents.empty()) {
                        writeStart->addPredecessorInPlanning((*createdEvents.rbegin()));
                    }
                    const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find((*it)->tail->name + "-f");
                    if (eventFinishPredecessorComputing != nullptr) {
                        const double prev = eventFinishPredecessorComputing->getActualTimeFire();
                        writeStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
                        assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
                    } else {
                        if ((*it)->tail->status == Status::Finished) {
                            std::cout << "no event finish predecessor - because tail is finished" << '\n';
                        } else {
                            std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
                        }
                    }

                    additionalEventsFromEvictAll.emplace_back(writeStart);
                    additionalEventsFromEvictAll.emplace_back(writeFinish);


                    ++it;
                } else {
                    ++it;
                }
            }

            const double amountToOffloadWithoutAllFiles = (amountToOffload - sumWeightsOfAllPending > 0) ? amountToOffload - sumWeightsOfAllPending : 0;
            availableMemAllEvict= result.processorOfAssignment->getAvailableMemory() - amountToOffload + sumWeightsOfAllPending;

            //  finishTimeToWrite = result.processorOfAssignment->getReadyTimeWrite() +
            //                     timeToWriteAllPending;
            startTimeForAllEvicted = std::max(startTimeForAllEvicted, finishTimeWriteAllEvict);
            timeToTaskFinishAllEvicted = finishTimeWithMemorySwapping(startTimeForAllEvicted, amountToOffloadWithoutAllFiles, v->time, v, result.processorOfAssignment);

            // startTimeForAllEvicted + timeToRun / result.processorOfAssignment->getProcessorSpeed() + amountToOffloadWithoutAllFiles / result.processorOfAssignment->memoryOffloadingPenalty;
            assert(timeToTaskFinishAllEvicted > startTimeForAllEvicted);
        }

        const double minTTF = std::min(timeToFinishNoEvicted, std::min(timeToTaskFinishBiggestEvicted, timeToTaskFinishAllEvicted));
        if (minTTF == std::numeric_limits<double>::max()) {
            std::cout << "minTTF inf" << '\n';
            result.finishTime = std::numeric_limits<double>::max();
            return {};
        }


        eventFinishTask->adjustBothPlannedFireTimes(minTTF);


            if (timeToTaskFinishBiggestEvicted == minTTF) {
                handleBiggestEvict2(result, changedEdgesOne, startTimeFor1Evicted, biggestPendingEdge, minTTF, finishTimeWrite1Evict);
                createdEvents.insert(createdEvents.end(), additionalEventsFromEvict1.begin(), additionalEventsFromEvict1.end());
                eventStartTask->adjustBothPlannedFireTimes(startTimeFor1Evicted);
                eventStartTask->addPredecessorInPlanning((*createdEvents.rbegin()));
                result.shouldBeFreeOnProcessorDuringTask=std::max(availableMem1Evict, 0.0);
            } else if (timeToTaskFinishAllEvicted == minTTF) {
                handleAllEvict2(result, timeToWriteAllPending, changedEdgesAll, startTimeForAllEvicted, minTTF, finishTimeWriteAllEvict);
                createdEvents.insert(createdEvents.end(), additionalEventsFromEvictAll.begin(), additionalEventsFromEvictAll.end());
                result.shouldBeFreeOnProcessorDuringTask=std::max(availableMemAllEvict, 0.0);;
                eventStartTask->adjustBothPlannedFireTimes(startTimeForAllEvicted);
                eventStartTask->addPredecessorInPlanning((*createdEvents.rbegin()));
            } else {
                result.resultingVar = 1;
                assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
                result.processorOfAssignment->setReadyTimeCompute(minTTF);
                result.finishTime = result.processorOfAssignment->getReadyTimeCompute();
                assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
                result.shouldBeFreeOnProcessorDuringTask=std::max(Res, 0.0);
                eventStartTask->adjustBothPlannedFireTimes(result.startTime) ;

            }



    } else {

        result.shouldBeFreeOnProcessorDuringTask=Res;
        double duration =  v->time / result.processorOfAssignment->getProcessorSpeed();
        result.resultingVar = 1;
        result.processorOfAssignment->setReadyTimeCompute(
            result.startTime + duration);
        result.finishTime = result.processorOfAssignment->getReadyTimeCompute();
        eventStartTask->adjustBothPlannedFireTimes(result.startTime) ;
        eventFinishTask->adjustBothPlannedFireTimes(result.finishTime) ;
    }
    // cout<<endl;
    assert(result.finishTime > result.startTime);
    result.modifiedProcs = modifiedProcs;
    assert(result.resultingVar != -1);
    if ( !result.processorOfAssignment->getLastComputeEvent().expired())
        eventStartTask->addPredecessorInPlanning(result.processorOfAssignment->getLastComputeEvent().lock());
    createdEvents.emplace_back(eventStartTask);
    createdEvents.emplace_back(eventFinishTask);
    result.processorOfAssignment->setLastComputeEvent(eventFinishTask);
    result.finishTime= eventFinishTask->getExpectedTimeFire();
    result.startTime= eventStartTask->getExpectedTimeFire();

    return createdEvents;
}

std::vector<std::shared_ptr<Event>>  tentativeAssignmentHEFT2(const vertex_t* v, const bool shouldUseDeviatedTimes, SchedulingResult& result, SchedulingResult& resultCorrect)
{
    throw new std::runtime_error("not implemented");
    // // cout<<"tent on proc "<<ourModifiedProc->id<< " ";
    // assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    //
    // double sumOut = getSumOut(v);
    // if (result.processorOfAssignment->getMemorySize() < outMemoryRequirement(v) || result.processorOfAssignment->getMemorySize() < inMemoryRequirement(v)) {
    //     //  cout<<"too large outs absolutely"<<endl;
    //     result.finishTime = std::numeric_limits<double>::max();
    //     return {};
    // }
    //
    // // cout<<"sumOut includes ";
    // realSurplusOfOutgoingEdges(v, resultCorrect.processorOfAssignment, sumOut);
    //
    // std::vector<std::shared_ptr<Processor>> modifiedProcs, modifiedProcsCorrect;
    // modifiedProcs.emplace_back(result.processorOfAssignment);
    // modifiedProcsCorrect.emplace_back(resultCorrect.processorOfAssignment);
    //
    // assert(result.resultingVar==-1);
    //  std::vector<std::shared_ptr<Event>>  createdEvents;
    // processIncomingEdges(v, shouldUseDeviatedTimes, true, result.processorOfAssignment, modifiedProcs, result.startTime, createdEvents);
    // processIncomingEdgesByNotGoingIntoPast(v, shouldUseDeviatedTimes,   resultCorrect.processorOfAssignment, modifiedProcsCorrect,
    //     resultCorrect.startTime);
    //
    // assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    // // both processIncomingEdges do the same, so start times will be the same
    // result.startTime = result.processorOfAssignment->getReadyTimeCompute() > result.startTime ? result.processorOfAssignment->getReadyTimeCompute() : result.startTime;
    //
    // resultCorrect.startTime = resultCorrect.processorOfAssignment->getReadyTimeCompute() > resultCorrect.startTime ? resultCorrect.processorOfAssignment->getReadyTimeCompute() : resultCorrect.startTime;
    //
    // assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    //
    // if (resultCorrect.processorOfAssignment->getAvailableMemory() < sumOut) {
    //     // only the correct result knows about kicking
    //
    //     double stillNeedsToBeEvictedToRun = sumOut - resultCorrect.processorOfAssignment->getAvailableMemory();
    //     double writeTime = resultCorrect.startTime;
    //
    //     for (auto it = resultCorrect.processorOfAssignment->getPendingMemories().begin();
    //         it != resultCorrect.processorOfAssignment->getPendingMemories().end() && stillNeedsToBeEvictedToRun > 0;) {
    //         //  print_edge(*it);
    //         if ((*it)->head->name != v->name) {
    //             const double weightForTime = shouldUseDeviatedTimes ? (*it)->weight * (*it)->factorForRealExecution : (*it)->weight;
    //             stillNeedsToBeEvictedToRun -= (*it)->weight;
    //             double startWriteTime = std::max(writeTime, shouldUseDeviatedTimes ? (*it)->tail->makespan : (*it)->tail->makespanPerceived);
    //             auto location_on_processor = getLocationOnProcessor((*it), resultCorrect.processorOfAssignment->id, !shouldUseDeviatedTimes);
    //             assert(location_on_processor!=nullptr);
    //             startWriteTime = std::max( startWriteTime, (*location_on_processor).afterWhen.value());
    //
    //             writeTime = startWriteTime + weightForTime / resultCorrect.processorOfAssignment->writeSpeedDisk;
    //             //   cout<<"tent on proc "<<resultCorrect.processorOfAssignment->id<<" ";
    //             resultCorrect.edgesToChangeStatus.emplace_back((*it), Location(LocationType::OnDisk, std::nullopt, writeTime));
    //             it = resultCorrect.processorOfAssignment->removePendingMemory(*it);
    //
    //         } else {
    //             ++it;
    //         }
    //     }
    //     if (stillNeedsToBeEvictedToRun > 0) {
    //         std::cout << buildEdgeName(*resultCorrect.processorOfAssignment->getPendingMemories().begin()) << '\n';
    //         throw std::runtime_error("stillNeedsToBeEvictedToRun > 0");
    //     }
    //     assert(stillNeedsToBeEvictedToRun <= 0);
    //     assert(resultCorrect.processorOfAssignment->getAvailableMemory() >= sumOut);
    //     resultCorrect.startTime = writeTime;
    //     resultCorrect.processorOfAssignment->setReadyTimeWrite(writeTime);
    //     resultCorrect.processorOfAssignment->setReadyTimeCompute(writeTime);
    //     assert(resultCorrect.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    // }
    //
    // const double Res = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(v, resultCorrect.processorOfAssignment);
    // result.peakMem = (Res < 0) ? 1 : (result.processorOfAssignment->getMemorySize() - Res) / result.processorOfAssignment->getMemorySize();
    //
    // result.finishTime = result.startTime + v->time / result.processorOfAssignment->getProcessorSpeed();
    // result.processorOfAssignment->setReadyTimeCompute(result.finishTime);
    //
    // if (Res < 0) {
    //     // try finish times with and without memory overflow
    //     const double amountToOffload = -Res;
    //
    //    double correctFinishTime= finishTimeWithMemorySwapping(resultCorrect.startTime, amountToOffload, v->time,v, resultCorrect.processorOfAssignment);
    //    double duration =  (correctFinishTime- resultCorrect.startTime)* v->factorForRealExecution;
    //
    //    correctFinishTime = resultCorrect.startTime + duration;
    //
    //     resultCorrect.finishTime =
    //         correctFinishTime;
    //     assert(resultCorrect.finishTime > resultCorrect.startTime);
    //
    //     if (result.finishTime == std::numeric_limits<double>::max()) {
    //         std::cout << "perceivedFinishTime inf" << '\n';
    //         resultCorrect.finishTime = std::numeric_limits<double>::max();
    //         return{};
    //     }
    //     assert(resultCorrect.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    //     resultCorrect.processorOfAssignment->setReadyTimeCompute(resultCorrect.finishTime);
    //     assert(resultCorrect.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    //
    // } else {
    //
    //     double correctFinishTime= resultCorrect.startTime + v->time / resultCorrect.processorOfAssignment->getProcessorSpeed();;
    //     double duration =  (correctFinishTime- resultCorrect.startTime)* v->factorForRealExecution;
    //     correctFinishTime = resultCorrect.startTime + duration;
    //
    //     resultCorrect.finishTime =correctFinishTime;
    //     resultCorrect.processorOfAssignment->setReadyTimeCompute(resultCorrect.finishTime);
    // }
    // result.modifiedProcs = modifiedProcs;
    // resultCorrect.modifiedProcs = modifiedProcsCorrect;
    // result.resultingVar = 1;
    // resultCorrect.resultingVar = 1;
    // return {};

}




void processIncomingEdges2(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs, double& earliestStartingTimeToComputeVertex,  std::vector<std::shared_ptr<Event>>& createdEvents)
{

    earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeCompute();
    for (const auto incomingEdge : v->in_edges) {
        const vertex_t* predecessor = incomingEdge->tail;

        const double edgeWeightToUse = incomingEdge->weight;

        if (predecessor->assignedProcessorId == ourModifiedProc->id) {
            if (!isLocatedOnThisProcessor(incomingEdge, ourModifiedProc->id, true)) {
                const std::shared_ptr<Event>& eventFinishWriteOfThisEdge = events.find(buildEdgeName(incomingEdge) + "-w-f");
                assert(eventFinishWriteOfThisEdge!=nullptr);
                assert(isLocatedOnDisk(incomingEdge, true));

                double startOfRead = std::max({ourModifiedProc->getReadyTimeRead(), (*getLocationOnDisk(incomingEdge, true)).afterWhen.value(), eventFinishWriteOfThisEdge->getExpectedTimeFire()});

                auto readStart= Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,startOfRead, startOfRead, false, buildEdgeName(incomingEdge) + "-r-s");
                auto readFinish= Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,ourModifiedProc->getReadyTimeRead(), ourModifiedProc->getReadyTimeRead(), false, buildEdgeName(incomingEdge) + "-r-f");

                bool isDependentOnLastCompute=false;
                if (startOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startOfRead = ourModifiedProc->getReadyTimeCompute();
                    isDependentOnLastCompute=true;
                }

                ourModifiedProc->setReadyTimeRead(
                    startOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk);
                earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeRead() > earliestStartingTimeToComputeVertex ? ourModifiedProc->getReadyTimeRead() : earliestStartingTimeToComputeVertex;

                 readFinish->addPredecessorInPlanning(readStart);
                if (createdEvents.size()>0) {
                    readStart->addPredecessorInPlanning((*createdEvents.rbegin()));
                }

                const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find(incomingEdge->tail->name + "-f");
                if (eventFinishPredecessorComputing != nullptr) {
                    const double prev = eventFinishPredecessorComputing->getActualTimeFire();
                    readStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
                    assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
                } else {
                    if (incomingEdge->tail->status == Status::Finished) {
                        std::cout << "no event finish predecessor - because tail is finished" << '\n';
                    } else {
                        std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
                    }
                }
                if (isDependentOnLastCompute && !ourModifiedProc->getLastComputeEvent().expired() ) {
                    assert(!ourModifiedProc->getLastComputeEvent().lock()->isDone);
                    readStart->addPredecessorInPlanning(ourModifiedProc->getLastComputeEvent().lock());
                }
                readStart->addPredecessorInPlanning(eventFinishWriteOfThisEdge);

                ourModifiedProc->setLastReadEvent(readFinish);
                createdEvents.emplace_back(readStart);
                createdEvents.emplace_back(readFinish);
            }

        } else {
            if (isLocatedOnDisk(incomingEdge, true)) {
                // we need to schedule read
                const std::shared_ptr<Event>& eventFinishWriteOfThisEdge = events.find(buildEdgeName(incomingEdge) + "-w-f");
                assert(eventFinishWriteOfThisEdge!=nullptr);

                double startOfRead = std::max({ourModifiedProc->getReadyTimeRead(), (*getLocationOnDisk(incomingEdge, true)).afterWhen.value(),
                eventFinishWriteOfThisEdge->getExpectedTimeFire() } );
                bool isDependentOnLastCompute=false;
                if (startOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startOfRead = ourModifiedProc->getReadyTimeCompute();
                    isDependentOnLastCompute=true;
                }
                ourModifiedProc->setReadyTimeRead(
                    startOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk);
                earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeRead() > earliestStartingTimeToComputeVertex ? ourModifiedProc->getReadyTimeRead() : earliestStartingTimeToComputeVertex;

                auto readStart= Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,startOfRead, startOfRead, false, buildEdgeName(incomingEdge) + "-r-s");
                auto readFinish= Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,ourModifiedProc->getReadyTimeRead(), ourModifiedProc->getReadyTimeRead(), false, buildEdgeName(incomingEdge) + "-r-f");
                readFinish->addPredecessorInPlanning(readStart);
                 if (!ourModifiedProc->getLastReadEvent().expired()) {
                    readStart->addPredecessorInPlanning(ourModifiedProc->getLastReadEvent().lock());
                }


                const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find(incomingEdge->tail->name + "-f");
                if (eventFinishPredecessorComputing != nullptr) {
                    const double prev = eventFinishPredecessorComputing->getActualTimeFire();
                    readStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
                    assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
                } else {
                    if (incomingEdge->tail->status == Status::Finished) {
                        std::cout << "no event finish predecessor - because tail is finished" << '\n';
                    } else {
                        std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
                    }
                }
                if (isDependentOnLastCompute && !ourModifiedProc->getLastComputeEvent().expired() ) {
                    assert(!ourModifiedProc->getLastComputeEvent().lock()->isDone);
                    readStart->addPredecessorInPlanning(ourModifiedProc->getLastComputeEvent().lock());
                }
                readStart->addPredecessorInPlanning(eventFinishWriteOfThisEdge);

                ourModifiedProc->setLastReadEvent(readFinish);
                createdEvents.emplace_back(readStart);
                createdEvents.emplace_back(readFinish);

            } else {
                auto predecessorsProcessorsId = predecessor->assignedProcessorId;
                assert(isLocatedOnThisProcessor(incomingEdge, predecessorsProcessorsId, true));
                std::shared_ptr<Processor> addedProc;
                auto it = // modifiedProcs.size()==1?
                          //   modifiedProcs.begin():
                    std::find_if(modifiedProcs.begin(), modifiedProcs.end(),
                        [predecessorsProcessorsId](const std::shared_ptr<Processor>& p) {
                            return p->id == predecessorsProcessorsId;
                        });

                if (it == modifiedProcs.end()) {
                    addedProc = std::make_shared<Processor>(*actualCluster->getProcessorById(predecessorsProcessorsId));
                    // cout<<"adding modified proc "<<addedProc->id<<endl;
                    modifiedProcs.emplace_back(addedProc);
                    checkIfPendingMemoryCorrect(addedProc);
                } else {
                    addedProc = *it;
                }

                const double timeToStartWriting = std::max(predecessor->makespanPerceived, addedProc->getReadyTimeWrite());
                double timeToFinishWriting = timeToStartWriting + edgeWeightToUse / addedProc->writeSpeedDisk;
                addedProc->setReadyTimeWrite(timeToFinishWriting);
                double startTimeOfRead = std::max(timeToFinishWriting, ourModifiedProc->getReadyTimeRead());

                if (startTimeOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startTimeOfRead = ourModifiedProc->getReadyTimeCompute();
                }

                double endTimeOfRead = startTimeOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk;
                ourModifiedProc->setReadyTimeRead(endTimeOfRead);

                earliestStartingTimeToComputeVertex = std::max(earliestStartingTimeToComputeVertex, endTimeOfRead);
                // int addpl  = addedProc->pendingMemories.size();
                addedProc->removePendingMemory(incomingEdge);
                // assert(addpl> addedProc->pendingMemories.size());
                checkIfPendingMemoryCorrect(addedProc);

                auto readStart= Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,startTimeOfRead, startTimeOfRead, false, buildEdgeName(incomingEdge) + "-r-s");
                auto readFinish= Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,endTimeOfRead, endTimeOfRead, false, buildEdgeName(incomingEdge) + "-r-f");

                auto writeStart= Event::createEvent(nullptr, incomingEdge, OnWriteStart, addedProc,timeToStartWriting, timeToStartWriting, false, buildEdgeName(incomingEdge) + "-w-s");
                auto writeFinish= Event::createEvent(nullptr, incomingEdge, OnWriteFinish, addedProc,addedProc->getReadyTimeWrite(), addedProc->getReadyTimeWrite(), false, buildEdgeName(incomingEdge) + "-w-f");

                readFinish->addPredecessorInPlanning(readStart);
                writeFinish->addPredecessorInPlanning(writeStart);
                readStart->addPredecessorInPlanning(writeFinish);
                if (!addedProc->getLastWriteEvent().expired()) {
                    writeStart->addPredecessorInPlanning(addedProc->getLastWriteEvent().lock());
                }

                const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find(incomingEdge->tail->name + "-f");
                if (eventFinishPredecessorComputing != nullptr) {
                    const double prev = eventFinishPredecessorComputing->getActualTimeFire();
                    writeStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
                    assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
                } else {
                    if (incomingEdge->tail->status == Finished) {
                        std::cout << "no event finish predecessor - because tail is finished" << '\n';
                    } else {
                        std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
                    }
                }


                ourModifiedProc->setLastReadEvent(readFinish);
                addedProc->setLastWriteEvent(writeFinish);
                createdEvents.emplace_back(readStart);
                createdEvents.emplace_back(readFinish);
                createdEvents.emplace_back(writeStart);
                createdEvents.emplace_back(writeFinish);
            }
        }
    }

}


