#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "fonda_scheduler/algorithms.hpp"
#include "fonda_scheduler/options.hpp"

#include <iterator>
#include <queue>

Cluster* imaginedCluster;
Cluster* imaginedClusterIncorrect;

Cluster* actualCluster;

std::vector<std::shared_ptr<Event>> medih2(graph_t* graph, int algoNum, double& runtime){
    const bool isHeft = (algoNum == fonda_scheduler::ALGORITHMS::HEFT);
    if (isHeft) {
        imaginedClusterIncorrect->mayBecomeInvalid();
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
        std::cout<<"deal w "<<vertex->name<<std::endl;
        numProcessedVertices++;
        SchedulingResult bestSchedulingResult(nullptr);
        SchedulingResult bestSchedulingResultIncorrect(nullptr);
        //  cout << "imagine" << endl;
        start = std::chrono::system_clock::now();


        auto newevents = bestTentativeAssignment2(isHeft, vertex, bestSchedulingResult, bestSchedulingResultIncorrect);
        if (bestSchedulingResult.modifiedProcs.empty()) {
            std::cout << "Invalid assignment of " << vertex->name;
            return {};
        }

        checkIfPendingMemoryCorrect(bestSchedulingResult.processorOfAssignment);
        res_events.insert(res_events.end(), newevents.begin(), newevents.end());

        std::cout<<"--------------------------------------------------------------------------------------"<<std::endl;
        std::cout << "Best events for vertex "<< vertex->name << ":" << std::endl;

        for (auto newevent : newevents) {
            newevent->printEventShort();
        }

        std::cout<<"for task "<<vertex->name<<" variant "<< bestSchedulingResult.resultingVar << " on proc "<< bestSchedulingResult.processorOfAssignment->id
                 <<" from "<< bestSchedulingResult.startTime <<" to "<< bestSchedulingResult.finishTime
        <<" duration is "<<" "<< bestSchedulingResult.finishTime - bestSchedulingResult.startTime << std::endl;


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

        applySchedulingResultToImaginedCluster(vertex, bestSchedulingResult, imaginedCluster, numberWithEvictedCases, isHeft);
        if (isHeft)
            applySchedulingResultToImaginedCluster(vertex, bestSchedulingResultIncorrect, imaginedClusterIncorrect, numberWithEvictedCases, isHeft);

        for (auto& item : newevents) {
            item->processor = imaginedCluster->getProcessorById(item->processor->id);
        }

        auto taskSTart = findTaskStart(newevents);
        assert(taskSTart!=nullptr);
        taskSTart->memoryVariant= bestSchedulingResult.resultingVar;

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

std::vector<std::shared_ptr<Event>>  bestTentativeAssignmentHEFT2(vertex_t* vertex, SchedulingResult& result, SchedulingResult& resultIncorrect)
{
    resultIncorrect.finishTime = std::numeric_limits<double>::max();
    resultIncorrect.startTime = 0;
    result.finishTime = std::numeric_limits<double>::max();

    std::vector<std::shared_ptr<Event>> bestEvents;
    std::cout << "assigning vertex "<< vertex->name << std::endl;

    for (const auto& [id, processor] : imaginedCluster->getProcessors()) {
        SchedulingResult tentativeResultIncorrect(imaginedClusterIncorrect->getProcessorById(id));
        SchedulingResult tentativeResultCorrect(processor);

        checkIfPendingMemoryCorrect(processor);
         std::vector<std::shared_ptr<Event>> eventsFromCorrect = tentativeAssignmentHEFT_withCorrectionAndEvents(vertex, tentativeResultIncorrect, tentativeResultCorrect );


        double finTime =tentativeResultIncorrect.finishTime;
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
            assert(!tentativeResultCorrect.modifiedProcs.empty());
            result = tentativeResultCorrect;
            resultIncorrect= tentativeResultIncorrect;
            bestEvents = eventsFromCorrect;
        }
    }
    return bestEvents;
}


std::vector<std::shared_ptr<Event>>  bestTentativeAssignmentMEDIH2(vertex_t* vertex, SchedulingResult& result)
{
    result.finishTime = std::numeric_limits<double>::max();
    result.startTime = 0;
    std::vector<std::shared_ptr<Event>> bestEvents;


    for (auto& [id, processor] : imaginedCluster->getProcessors()) {
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


std::vector<std::shared_ptr<Event>>  bestTentativeAssignment2(const bool isHeft, vertex_t* vertex, SchedulingResult& result, SchedulingResult& incorrectResultForHeftOnly)
{
    std::vector<std::shared_ptr<Event>> eventsnew;
    if (isHeft) {
       eventsnew =  bestTentativeAssignmentHEFT2(vertex, result, incorrectResultForHeftOnly);
    } else {
      eventsnew =   bestTentativeAssignmentMEDIH2(vertex, result);
    }
    return eventsnew;
}

std::vector<std::shared_ptr<Event>>  tentativeAssignment2(vertex_t* v, SchedulingResult& result)
{

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
    processIncomingEdges2(v, result.processorOfAssignment, modifiedProcs, result.startTime,createdEvents, false);


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
            availableMem1Evict= (biggestFileWeight-amountToOffload)>0? biggestFileWeight-amountToOffload:0;
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
            std::shared_ptr<Event> lastEvictionEvent = nullptr;
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

                    if (!createdEvents.empty()) {
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

                    if (lastEvictionEvent) {
                        writeStart->addPredecessorInPlanning(lastEvictionEvent);
                    }
                    lastEvictionEvent = writeFinish;

                    additionalEventsFromEvictAll.emplace_back(writeStart);
                    additionalEventsFromEvictAll.emplace_back(writeFinish);


                    ++it;
                } else {
                    ++it;
                }
            }

            const double amountToOffloadWithoutAllFiles = (amountToOffload - sumWeightsOfAllPending > 0) ? amountToOffload - sumWeightsOfAllPending : 0;
            availableMemAllEvict= ( sumWeightsOfAllPending - amountToOffload)>0? sumWeightsOfAllPending - amountToOffload:0;

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
                emulateBiggestEvict2(result, changedEdgesOne, startTimeFor1Evicted, biggestPendingEdge, minTTF, finishTimeWrite1Evict);
                createdEvents.insert(createdEvents.end(), additionalEventsFromEvict1.begin(), additionalEventsFromEvict1.end());
                eventStartTask->adjustBothPlannedFireTimes(startTimeFor1Evicted);
                eventStartTask->addPredecessorInPlanning((*createdEvents.rbegin()));
                result.shouldBeFreeOnProcessorDuringTask=std::max(availableMem1Evict, 0.0);
                result.resultingVar=2;
            } else if (timeToTaskFinishAllEvicted == minTTF) {
                emulateAllEvict2(result, timeToWriteAllPending, changedEdgesAll, startTimeForAllEvicted, minTTF, finishTimeWriteAllEvict);
                createdEvents.insert(createdEvents.end(), additionalEventsFromEvictAll.begin(), additionalEventsFromEvictAll.end());
                result.shouldBeFreeOnProcessorDuringTask=std::max(availableMemAllEvict, 0.0);;
                eventStartTask->adjustBothPlannedFireTimes(startTimeForAllEvicted);
                eventStartTask->addPredecessorInPlanning((*createdEvents.rbegin()));
                result.resultingVar=3;
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
        result.resultingVar = 0;
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

std::vector<std::shared_ptr<Event>>  tentativeAssignmentHEFT2( vertex_t* v, SchedulingResult& result)
{
    // cout<<"tent on proc "<<ourModifiedProc->id<< " ";
    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    double sumOut = getSumOut(v);
    if (result.processorOfAssignment->getMemorySize() < outMemoryRequirement(v) || result.processorOfAssignment->getMemorySize() < inMemoryRequirement(v)) {
        //  cout<<"too large outs absolutely"<<endl;
        result.finishTime = std::numeric_limits<double>::max();
        return {};
    }

    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    modifiedProcs.emplace_back(result.processorOfAssignment);


    assert(result.resultingVar==-1);
     std::vector<std::shared_ptr<Event>>  createdEvents;

    processIncomingEdgesDisregardingMemorySizes(v, result.processorOfAssignment, modifiedProcs, result.startTime );

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    result.startTime = result.processorOfAssignment->getReadyTimeCompute() > result.startTime ? result.processorOfAssignment->getReadyTimeCompute() : result.startTime;

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    result.finishTime = result.startTime + v->time / result.processorOfAssignment->getProcessorSpeed();
    result.processorOfAssignment->setReadyTimeCompute(result.finishTime);

    auto eventStartTask = Event::createEvent(v, nullptr, OnTaskStart,  result.processorOfAssignment,
      result.startTime, result.startTime, false,
      v->name + "-s");
    auto eventFinishTask = Event::createEvent(v, nullptr, OnTaskFinish,  result.processorOfAssignment,
        result.finishTime, result.finishTime, false,
        v->name + "-f");
    eventFinishTask->addPredecessorInPlanning(eventStartTask);
    for (const auto& created_event : createdEvents) {
        eventStartTask->addPredecessorInPlanning(created_event);
    }
    if (!result.processorOfAssignment->getLastComputeEvent().expired()) {
        eventStartTask->addPredecessorInPlanning( result.processorOfAssignment->getLastComputeEvent().lock());
    }
    result.processorOfAssignment->setLastComputeEvent(eventFinishTask);

    createdEvents.emplace_back(eventStartTask);
    createdEvents.emplace_back(eventFinishTask);

    assert(eventFinishTask->getExpectedTimeFire()== result.finishTime);

    result.modifiedProcs = modifiedProcs;
    result.resultingVar = 0;
    return {createdEvents};

}


std::vector<std::shared_ptr<Event>> tentativeAssignmentHEFT_withCorrectionAndEvents( vertex_t* v, SchedulingResult& result, SchedulingResult& resultCorrect)
{
    if (v->name=="BWA_MEM_00000004"&& result.processorOfAssignment->id==30) {
        std::cout << "";
    }
    result.resultingVar = 1;
    resultCorrect.resultingVar = 1;

     assert(result.processorOfAssignment->id == resultCorrect.processorOfAssignment->id);
    auto proc = result.processorOfAssignment;
    auto procCorrect = resultCorrect.processorOfAssignment;

    assert(proc->getReadyTimeCompute() < std::numeric_limits<double>::max());

    double sumOut = getSumOut(v);

    if (proc->getMemorySize() < outMemoryRequirement(v) ||
        proc->getMemorySize() < inMemoryRequirement(v))
    {
        result.finishTime = std::numeric_limits<double>::max();
        resultCorrect.finishTime = std::numeric_limits<double>::max();
        return {};
    }

    realSurplusOfOutgoingEdges(v, resultCorrect.processorOfAssignment, sumOut);

    std::vector<std::shared_ptr<Processor>> modifiedProcs, modifiedProcsCorrect;
    modifiedProcs.emplace_back(proc);
    modifiedProcsCorrect.emplace_back(procCorrect);

    std::vector<std::shared_ptr<Event>> createdEvents;

    /* ===============================
     * Incoming edges
     * =============================== */

    processIncomingEdges2( v, resultCorrect.processorOfAssignment,
        modifiedProcsCorrect, resultCorrect.startTime,createdEvents ,false);
    processIncomingEdgesDisregardingMemorySizes(v, result.processorOfAssignment, modifiedProcs,
        result.startTime);

    result.startTime = std::max(result.startTime, proc->getReadyTimeCompute());
    resultCorrect.startTime = std::max(resultCorrect.startTime, procCorrect->getReadyTimeCompute());

    /* ===============================
     * Memory eviction (correct only)
     * =============================== */

    if (procCorrect->getAvailableMemory() < sumOut) {

        double stillNeedsToBeEvictedToRun =
            sumOut - procCorrect->getAvailableMemory();

        double writeTime = std::max(resultCorrect.startTime,procCorrect->getReadyTimeWrite());
        std::shared_ptr<Event> lastEvictionEvent = nullptr;
        for (auto it = procCorrect->getPendingMemories().begin(); it != procCorrect->getPendingMemories().end() &&
             stillNeedsToBeEvictedToRun > 0;)
        {
            std::cout << buildEdgeName(*it)<< " evicted from  proc " << procCorrect->id <<" to reach sumOUt for "<<v->name<< std::endl;
            edge_t* currentEdge = *it;

            if (currentEdge->head->name != v->name) {
                stillNeedsToBeEvictedToRun -= currentEdge->weight;
                double startWriteTime = std::max(writeTime, currentEdge->tail->makespanPerceived);

                auto loc =
                    getLocationOnProcessor(currentEdge, procCorrect->id, true);
                assert(loc != nullptr);

                startWriteTime = std::max(startWriteTime, loc->afterWhen.value());
                writeTime = startWriteTime + currentEdge->weight / procCorrect->writeSpeedDisk;

                resultCorrect.edgesToChangeStatus.emplace_back(currentEdge, Location(LocationType::OnDisk, std::nullopt, writeTime));
                it = procCorrect->removePendingMemory(currentEdge);

                 auto writeStart= Event::createEvent(nullptr, currentEdge, OnWriteStart, procCorrect,startWriteTime, startWriteTime, false, buildEdgeName(currentEdge) + "-w-s");
                auto writeFinish= Event::createEvent(nullptr, currentEdge, OnWriteFinish, procCorrect,writeTime, writeTime, false, buildEdgeName(currentEdge) + "-w-f");

                std::shared_ptr<Event> firstEventOfThisEdge = writeStart; // or writeStart
                std::shared_ptr<Event> lastEventOfThisEdge  = writeFinish; // or read/write finish

                writeFinish->addPredecessorInPlanning(writeStart);
                if (!procCorrect->getLastWriteEvent().expired()) {
                    writeStart->addPredecessorInPlanning(procCorrect->getLastWriteEvent().lock());
                }


                const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find((currentEdge)->tail->name + "-f");
                if (eventFinishPredecessorComputing != nullptr) {
                    const double prev = eventFinishPredecessorComputing->getActualTimeFire();
                    writeStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
                    assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
                } else {
                    if (currentEdge->tail->status == Finished) {
                        std::cout << "no event finish predecessor - because tail is finished" << '\n';
                    } else {
                        std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
                    }
                }
                if (lastEvictionEvent) {
                    firstEventOfThisEdge->addPredecessorInPlanning(lastEvictionEvent);
                }
                lastEvictionEvent = lastEventOfThisEdge;

                procCorrect->setLastWriteEvent(writeFinish);
                resultCorrect.startTime = std::max( resultCorrect.startTime, writeFinish->getExpectedTimeFire());

                createdEvents.emplace_back(writeStart);
                createdEvents.emplace_back(writeFinish);

            } else {
                ++it;
            }
            resultCorrect.shouldBeFreeOnProcessorDuringTask=std::max(stillNeedsToBeEvictedToRun, 0.0);
            resultCorrect.resultingVar=3;
        }

        if (stillNeedsToBeEvictedToRun > 0) {
            throw std::runtime_error("stillNeedsToBeEvictedToRun > 0");
        }

        resultCorrect.startTime = writeTime;
        procCorrect->setReadyTimeWrite(writeTime);
        procCorrect->setReadyTimeCompute(writeTime);
    }

    /* ===============================
     * Finish times
     * =============================== */

    result.finishTime =  result.startTime + v->time / proc->getProcessorSpeed();
    proc->setReadyTimeCompute(result.finishTime);

    const double Res = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(v, procCorrect);
    if (Res < 0) {
        const double amountToOffload = -Res;
        double correctFinishTime = finishTimeWithMemorySwapping( resultCorrect.startTime, amountToOffload, v->time,  v,   proc);
        resultCorrect.finishTime =correctFinishTime;
        resultCorrect.shouldBeFreeOnProcessorDuringTask=0;

    }
    else {
        double correctFinishTime = resultCorrect.startTime +  v->time / procCorrect->getProcessorSpeed();
        resultCorrect.finishTime =correctFinishTime;
        resultCorrect.shouldBeFreeOnProcessorDuringTask=Res;
    }

    proc->setReadyTimeCompute(resultCorrect.finishTime);

    /* ===============================
     * Task events (CORRECT ONLY)
     * =============================== */

    auto eventStart = Event::createEvent(v, nullptr, OnTaskStart, procCorrect, resultCorrect.startTime, resultCorrect.startTime, false, v->name + "-s");
    auto eventFinish = Event::createEvent(v, nullptr, OnTaskFinish, procCorrect, resultCorrect.finishTime, resultCorrect.finishTime, false, v->name + "-f");

    eventFinish->addPredecessorInPlanning(eventStart);

    for (auto& e : createdEvents) {
        //finishes depend on their corresponding starts, so event start only neeed to depend on finishes
        if (e->isFinish())
            eventStart->addPredecessorInPlanning(e);
    }

    if (!procCorrect->getLastComputeEvent().expired()) {
        eventStart->addPredecessorInPlanning( procCorrect->getLastComputeEvent().lock());
    }

    procCorrect->setLastComputeEvent(eventFinish);

    createdEvents.emplace_back(eventStart);
    createdEvents.emplace_back(eventFinish);

    /* ===============================
     * Final bookkeeping
     * =============================== */

    result.modifiedProcs = modifiedProcs;
    resultCorrect.modifiedProcs = modifiedProcsCorrect;

    return createdEvents;
}




void processIncomingEdges2(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs, double& earliestStartingTimeToComputeVertex,  std::vector<std::shared_ptr<Event>>& createdEvents,
    bool forbidLookingIntoPast)
{
    const double preparationBarrier =
    forbidLookingIntoPast
        ? ourModifiedProc->getReadyTimeCompute()
        : -std::numeric_limits<double>::infinity();

    earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeCompute();
    for (const auto incomingEdge : v->in_edges) {
        const vertex_t* predecessor = incomingEdge->tail;

        const double edgeWeightToUse = incomingEdge->weight;

        if (predecessor->assignedProcessorId == ourModifiedProc->id) {
            if (!isLocatedOnThisProcessor(incomingEdge, ourModifiedProc->id, true)) {
                const std::shared_ptr<Event>& eventFinishWriteOfThisEdge = events.find(buildEdgeName(incomingEdge) + "-w-f");
                assert(eventFinishWriteOfThisEdge!=nullptr);
                assert(isLocatedOnDisk(incomingEdge, true));

                double startOfRead = std::max({ourModifiedProc->getReadyTimeRead(), (*getLocationOnDisk(incomingEdge, true)).afterWhen.value(), eventFinishWriteOfThisEdge->getExpectedTimeFire(), preparationBarrier});
                double finishOfRead = startOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk;
                auto readStart= Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,startOfRead, startOfRead, false, buildEdgeName(incomingEdge) + "-r-s");
                auto readFinish= Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,finishOfRead, finishOfRead, false, buildEdgeName(incomingEdge) + "-r-f");
                std::shared_ptr<Event> firstEventOfThisEdge = readStart; // or writeStart
                std::shared_ptr<Event> lastEventOfThisEdge  = readFinish; // or read/write finish

                bool isDependentOnLastCompute=false;
                if (startOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startOfRead = ourModifiedProc->getReadyTimeCompute();
                    isDependentOnLastCompute=true;
                }

                readFinish->addPredecessorInPlanning(readStart);

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
                if (!ourModifiedProc->getLastReadEvent().expired()) {
                    readStart->addPredecessorInPlanning(ourModifiedProc->getLastReadEvent().lock());
                }
                readStart->addPredecessorInPlanning(eventFinishWriteOfThisEdge);

                ourModifiedProc->setLastReadEvent(readFinish);
                createdEvents.emplace_back(readStart);
                createdEvents.emplace_back(readFinish);

                earliestStartingTimeToComputeVertex = std::max(readFinish->getExpectedTimeFire(), earliestStartingTimeToComputeVertex);
            }

        } else {
            if (isLocatedOnDisk(incomingEdge, true)) {
                // we need to schedule read
                const std::shared_ptr<Event>& eventFinishWriteOfThisEdge = events.find(buildEdgeName(incomingEdge) + "-w-f");
                assert(eventFinishWriteOfThisEdge!=nullptr);

                double startOfRead = std::max({ourModifiedProc->getReadyTimeRead(), (*getLocationOnDisk(incomingEdge, true)).afterWhen.value(),
                eventFinishWriteOfThisEdge->getExpectedTimeFire(), preparationBarrier } );
                bool isDependentOnLastCompute=false;
                if (startOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startOfRead = ourModifiedProc->getReadyTimeCompute();
                    isDependentOnLastCompute=true;
                }
                ourModifiedProc->setReadyTimeRead(
                    startOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk);


                auto readStart= Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,startOfRead, startOfRead, false, buildEdgeName(incomingEdge) + "-r-s");
                auto readFinish= Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,ourModifiedProc->getReadyTimeRead(), ourModifiedProc->getReadyTimeRead(), false, buildEdgeName(incomingEdge) + "-r-f");
                std::shared_ptr<Event> firstEventOfThisEdge = readStart; // or writeStart
                std::shared_ptr<Event> lastEventOfThisEdge  = readFinish; // or read/write finish
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

                earliestStartingTimeToComputeVertex = std::max(readFinish->getExpectedTimeFire(), earliestStartingTimeToComputeVertex);

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
                    addedProc = std::make_shared<Processor>(*imaginedCluster->getProcessorById(predecessorsProcessorsId));
                    // cout<<"adding modified proc "<<addedProc->id<<endl;
                    modifiedProcs.emplace_back(addedProc);
                    checkIfPendingMemoryCorrect(addedProc);
                } else {
                    addedProc = *it;
                }

                double timeFromLocation = (*getLocationOnProcessor(incomingEdge, addedProc->id, true)).afterWhen.value();
                const double timeToStartWriting = std::max({ timeFromLocation, predecessor->makespanPerceived, addedProc->getReadyTimeWrite(), preparationBarrier});
                double timeToFinishWriting = timeToStartWriting + edgeWeightToUse / addedProc->writeSpeedDisk;
                addedProc->setReadyTimeWrite(timeToFinishWriting);
                double startTimeOfRead = std::max(timeToFinishWriting, ourModifiedProc->getReadyTimeRead());

                if (startTimeOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startTimeOfRead = ourModifiedProc->getReadyTimeCompute();
                }

                double endTimeOfRead = startTimeOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk;
                ourModifiedProc->setReadyTimeRead(endTimeOfRead);


                // int addpl  = addedProc->pendingMemories.size();
                addedProc->removePendingMemory(incomingEdge);
                // assert(addpl> addedProc->pendingMemories.size());
                checkIfPendingMemoryCorrect(addedProc);

                auto readStart= Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,startTimeOfRead, startTimeOfRead, false, buildEdgeName(incomingEdge) + "-r-s");
                auto readFinish= Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,endTimeOfRead, endTimeOfRead, false, buildEdgeName(incomingEdge) + "-r-f");

                auto writeStart= Event::createEvent(nullptr, incomingEdge, OnWriteStart, addedProc,timeToStartWriting, timeToStartWriting, false, buildEdgeName(incomingEdge) + "-w-s");
                auto writeFinish= Event::createEvent(nullptr, incomingEdge, OnWriteFinish, addedProc,addedProc->getReadyTimeWrite(), addedProc->getReadyTimeWrite(), false, buildEdgeName(incomingEdge) + "-w-f");

                std::shared_ptr<Event> firstEventOfThisEdge = writeStart; // or writeStart
                std::shared_ptr<Event> lastEventOfThisEdge  = readFinish; // or read/write finish

                readFinish->addPredecessorInPlanning(readStart);
                writeFinish->addPredecessorInPlanning(writeStart);
                readStart->addPredecessorInPlanning(writeFinish);
                if (!addedProc->getLastWriteEvent().expired()) {
                    writeStart->addPredecessorInPlanning(addedProc->getLastWriteEvent().lock());
                }
                if (!ourModifiedProc->getLastReadEvent().expired()) {
                    readStart->addPredecessorInPlanning(ourModifiedProc->getLastReadEvent().lock());
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

                earliestStartingTimeToComputeVertex = std::max(earliestStartingTimeToComputeVertex, readFinish->getExpectedTimeFire());

                createdEvents.emplace_back(writeStart);
                createdEvents.emplace_back(writeFinish);
                createdEvents.emplace_back(readStart);
                createdEvents.emplace_back(readFinish);
            }
        }
    }
    std::cout << "";
}

void processIncomingEdgesDisregardingMemorySizes(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs, double& earliestStartingTimeToComputeVertex)
{
    // This is for HEFT only, so always use imaginedClusterIncorrect
    //assume that the edge is there, where the predecessor task left it.

    std::shared_ptr<Event> lastIncomingEdgeEvent = nullptr;
    earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeCompute();

    for (const auto incomingEdge : v->in_edges) {
        const vertex_t* predecessor = incomingEdge->tail;
        int predecessorProcessorId = predecessor->assignedProcessorId;
        if (predecessorProcessorId!= ourModifiedProc->id) {
            auto it = std::find_if(modifiedProcs.begin(), modifiedProcs.end(),
               [ predecessorProcessorId ](const std::shared_ptr<Processor>& p) {
                   return p->id ==   predecessorProcessorId;
               });
            std::shared_ptr<Processor> addedProc;
            if (it == modifiedProcs.end()) {
                addedProc = std::make_shared<Processor>(*imaginedClusterIncorrect->getProcessorById(predecessorProcessorId));
                // cout<<"adding modified proc "<<addedProc->id<<endl;
                modifiedProcs.emplace_back(addedProc);
                checkIfPendingMemoryCorrect(addedProc);
            } else {
                addedProc = *it;
            }

            const double timeToStartWriting = std::max({ predecessor->makespanPerceived, addedProc->getReadyTimeWrite()});
            double timeToFinishWriting = timeToStartWriting + incomingEdge->weight / addedProc->writeSpeedDisk;
            addedProc->setReadyTimeWrite(timeToFinishWriting);

            double startTimeOfRead = std::max(timeToFinishWriting, ourModifiedProc->getReadyTimeRead());
            double endTimeOfRead = startTimeOfRead + incomingEdge->weight / ourModifiedProc->readSpeedDisk;
            ourModifiedProc->setReadyTimeRead(endTimeOfRead);

            addedProc->removePendingMemory(incomingEdge);

            earliestStartingTimeToComputeVertex = std::max(earliestStartingTimeToComputeVertex, endTimeOfRead);
        }
    }
}




