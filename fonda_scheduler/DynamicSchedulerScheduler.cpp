#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"

std::vector<std::shared_ptr<Event>> bestTentativeAssignment(vertex_t* vertex, std::vector<std::shared_ptr<Processor>>& bestModifiedProcs,
    std::shared_ptr<Processor>& bestProcessorToAssign, const double notEarlierThan, int &bestResultingVar)
{

    if (vertex->in_edges.size()>1 ) {
        averageSpreadPredecessors+=  vertex->in_edges.size() /  uniquePredecessorProcs(vertex);
        numTasksComputedPredecessors++;
    }


    //std::cout << "!!!START BEST tent assign for " << vertex->name << std::endl;
    double bestStartTime = std::numeric_limits<double>::max();
    double bestFinishTime = std::numeric_limits<double>::max();
    double bestReallyUsedMem = std::numeric_limits<double>::max();
    bestResultingVar =-2;
    std::vector<std::shared_ptr<Event>> bestEvents;

    for (auto& [id, processor] : cluster->getProcessors()) {
        double finTime = -1;
        double startTime = -1;
        double reallyUsedMem = 0;
        int resultingEvictionVariant = -1;

        // Copy the processor to modify it without affecting the original
        const auto ourModifiedProc = std::make_shared<Processor>(*processor);

       // std::cout<<"adding our proc "<<ourModifiedProc->id<<std::endl;
        std::vector<std::shared_ptr<Event>> newEvents = {};
        // checkIfPendingMemoryCorrect(ourModifiedProc);
        const std::vector<std::shared_ptr<Processor>> modifiedProcs = tentativeAssignment(vertex, ourModifiedProc,
            finTime,
            startTime, resultingEvictionVariant,
            newEvents, reallyUsedMem, notEarlierThan);

        // cout<<"on "<<processor->id<<" fin time "<<finTime<<endl;
        if (finTime < bestFinishTime) {
            // cout << "best acutalize to " << ourModifiedProc->id << " act used mem " << reallyUsedMem << endl;
            assert(!modifiedProcs.empty());
            bestModifiedProcs = modifiedProcs;
            bestFinishTime = finTime;
            bestStartTime = startTime;

            bestProcessorToAssign = ourModifiedProc;
            bestEvents = newEvents;
            bestReallyUsedMem = reallyUsedMem;
            bestResultingVar= resultingEvictionVariant;
        }
    }
    // cout << "!!!END BEST"<<endl;

    if(bestProcessorToAssign== nullptr){
        throw std::runtime_error("No suitable processor found for "+ vertex->name);
    }

    /*std::cout  << vertex->name << " best " <<
       " "<< bestStartTime << " --- "
           << bestFinishTime
              // << " duration "<< bestFinishTime
             <<   " on "
             << bestProcessorToAssign->id
            << " variant " << bestResultingVar<<std::endl; */
    //  <<" with av mem "<<bestSchedulingResult.processorOfAssignment->getAvailableMemory()<<std::endl;

    // Assert that the best processor is not empty and has enough memory
    assert(bestProcessorToAssign != nullptr);
    assert(bestProcessorToAssign->getAvailableMemory() >= vertex->actuallyUsedMemory);

    vertex->assignedProcessorId = bestProcessorToAssign->id;
    vertex->actuallyUsedMemory = bestReallyUsedMem;
    vertex->status = Status::Scheduled;

    buildPendingMemoriesAfter(bestProcessorToAssign, vertex);

    for (auto& modifiedProc : bestModifiedProcs) {
        auto& [id, processor] = *cluster->getProcessors().find(modifiedProc->id);
        processor->updateFrom(*modifiedProc);
        assert(processor->getPendingMemories().size() == modifiedProc->getPendingMemories().size());
    }

    for (const auto& event : bestEvents) {
        event->processor = cluster->getProcessorById(event->processor->id);
    }

    for (const auto& in_edge : vertex->in_edges) {
        assert( // vertex->in_edges[j]->tail->makespan==-1 ||
            bestFinishTime > in_edge->tail->makespan);
    }

    processorLoads[bestProcessorToAssign->id] += bestFinishTime-bestStartTime;
    processorWorkTimes[bestProcessorToAssign->id].emplace_back(bestStartTime,
        bestFinishTime);


    // cout << "resulting var " << resultingVar<<" on "<<bestProcessorToAssign->id << endl;
    return bestEvents;
}

std::vector<std::shared_ptr<Processor>>
tentativeAssignment(vertex_t* vertex, const std::shared_ptr<Processor>& ourModifiedProc,
    double& finTime, double& startTime, int& resultingVar, std::vector<std::shared_ptr<Event>>& newEvents,
    double& actuallyUsedMemory, double notEarlierThan)
{
    // cout << "try " << ourModifiedProc->id << " for " << vertex->name << endl;
    assert(ourModifiedProc->getAvailableMemory() <= ourModifiedProc->getMemorySize());

    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    modifiedProcs.emplace_back(ourModifiedProc);

    transferAfterMemoriesToBefore(ourModifiedProc);

    startTime = std::max(notEarlierThan, ourModifiedProc->getExpectedOrActualReadyTimeCompute());
    auto eventStartTask = Event::createEvent(vertex, nullptr, OnTaskStart, ourModifiedProc,
        startTime, startTime, false,
        vertex->name + "-s");
    auto finishTime = startTime + vertex->time / ourModifiedProc->getProcessorSpeed();
    auto eventFinishTask = Event::createEvent(vertex, nullptr, OnTaskFinish, ourModifiedProc,
        finishTime, finishTime, false,
        vertex->name + "-f");
    eventFinishTask->addPredecessorInPlanning(eventStartTask);

    double sumOut = getSumOut(vertex);
    assert(sumOut == outMemoryRequirement(vertex));
    if (ourModifiedProc->getMemorySize() < outMemoryRequirement(vertex) || ourModifiedProc->getMemorySize() < inMemoryRequirement(vertex)) {
        //   cout<<"too large outs or ins absolutely for tast "<<vertex->name<<endl;
        finTime = std::numeric_limits<double>::max();
        return {};
    }

    realSurplusOfOutgoingEdges(vertex, ourModifiedProc, sumOut);
    double sumIn = getSumIn(vertex);
    realSurplusOfOutgoingEdges(vertex, ourModifiedProc, sumIn);

    double amountToOffloadWithoutBiggestFile = 0;
    double amountToOffloadWithoutAllFiles = 0;
    double Res = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(vertex, ourModifiedProc);
    if (Res < 0) {
        // cout<<" overflow! ";
        double amountToOffload = -Res;

        double timeToFinishNoEvicted = finishTimeWithMemorySwapping(startTime,  Res, vertex->time, vertex, ourModifiedProc);
            // startTime + vertex->time / ourModifiedProc->getProcessorSpeed() + amountToOffload / ourModifiedProc->memoryOffloadingPenalty;
        assert(timeToFinishNoEvicted > startTime);
        if (sumOut > ourModifiedProc->getAvailableMemory()) {
            // cout<<"cant"<<endl;
            timeToFinishNoEvicted = std::numeric_limits<double>::max();
        }
        if (sumIn > ourModifiedProc->getAvailableMemory()) {
            timeToFinishNoEvicted = std::numeric_limits<double>::max();
        }

        double timeToFinishBiggestEvicted = std::numeric_limits<double>::max(),
               timeToFinishTaskAllEvicted = std::numeric_limits<double>::max();

        double startTimeOfTaskFor1Evicted, startTimeOfTaskForAllEvicted;
        startTimeOfTaskFor1Evicted = startTimeOfTaskForAllEvicted = ourModifiedProc->getExpectedOrActualReadyTimeCompute() > startTime ? ourModifiedProc->getExpectedOrActualReadyTimeCompute() : startTime;

        edge_t* biggestPendingEdge = ourModifiedProc->getBiggestPendingEdgeThatIsNotIncomingOfAndLocatedOnProc(vertex);
        if (!ourModifiedProc->getPendingMemories().empty() && biggestPendingEdge != nullptr) {
            assert(biggestPendingEdge->weight >= (*ourModifiedProc->getPendingMemories().rbegin())->weight);
            //  cout << "case 2, before " << buildEdgeName(biggestPendingEdge);
            biggestPendingEdge = ourModifiedProc->getBiggestPendingEdgeThatIsNotIncomingOfAndLocatedOnProc(vertex);

            //      cout << " after " << buildEdgeName(biggestPendingEdge) << endl;
            double biggestFileWeight = biggestPendingEdge->weight;
            double startTimeToWriteBiggestEdge = std::max(std::max(ourModifiedProc->getExpectedOrActualReadyTimeWrite(),
                biggestPendingEdge->tail->makespan), notEarlierThan);
            amountToOffloadWithoutBiggestFile = (amountToOffload - biggestFileWeight) > 0 ? (amountToOffload - biggestFileWeight)
                                                                                          : 0;
            double finishTimeToWrite = startTimeToWriteBiggestEdge + biggestFileWeight / ourModifiedProc->writeSpeedDisk;
            startTimeOfTaskFor1Evicted = std::max(startTime, finishTimeToWrite);
            timeToFinishBiggestEvicted =
                finishTimeWithMemorySwapping(startTimeOfTaskFor1Evicted, amountToOffloadWithoutBiggestFile, vertex->time, vertex, ourModifiedProc);
               // startTimeFor1Evicted
               // + vertex->time / ourModifiedProc->getProcessorSpeed() + amountToOffloadWithoutBiggestFile / ourModifiedProc->memoryOffloadingPenalty;
            assert(timeToFinishBiggestEvicted > startTimeOfTaskFor1Evicted);

            const double availableMemWithoutBiggest = ourModifiedProc->getAvailableMemory() + biggestFileWeight;
            if (sumOut > availableMemWithoutBiggest) {
                timeToFinishBiggestEvicted = std::numeric_limits<double>::max();
            }

            double sumWeightsOfAllPending = 0;
            double timeToWriteAllPending = 0;
            finishTimeToWrite = std::max(notEarlierThan,ourModifiedProc->getExpectedOrActualReadyTimeWrite());
            for (const auto& item : ourModifiedProc->getPendingMemories()) {
                if (item->head->name != vertex->name) {
                    double startTimeThisWrite = std::max(finishTimeToWrite, item->tail->makespan);
                    timeToWriteAllPending += item->weight / ourModifiedProc->writeSpeedDisk;
                    finishTimeToWrite = startTimeThisWrite + item->weight / ourModifiedProc->writeSpeedDisk;
                    sumWeightsOfAllPending += item->weight;
                }
            }

            amountToOffloadWithoutAllFiles = (amountToOffload - sumWeightsOfAllPending > 0) ? amountToOffload - sumWeightsOfAllPending : 0;

            assert(amountToOffloadWithoutAllFiles >= 0);
            // finishTimeToWrite = ourModifiedProc->getExpectedOrActualReadyTimeWrite() +
            //                    timeToWriteAllPending;
            startTimeOfTaskForAllEvicted = std::max(startTimeOfTaskForAllEvicted, finishTimeToWrite);
            timeToFinishTaskAllEvicted =
                finishTimeWithMemorySwapping(startTimeOfTaskForAllEvicted, amountToOffloadWithoutAllFiles, vertex->time, vertex, ourModifiedProc);
                //startTimeForAllEvicted + vertex->time / ourModifiedProc->getProcessorSpeed() + amountToOffloadWithoutAllFiles / ourModifiedProc->memoryOffloadingPenalty;
            assert(timeToFinishTaskAllEvicted > startTimeOfTaskForAllEvicted);
        }

        double minTTF = std::min(timeToFinishNoEvicted, std::min(timeToFinishBiggestEvicted, timeToFinishTaskAllEvicted));
        if (minTTF == std::numeric_limits<double>::max()) {
            // cout << "minTTF inf" << endl;
            finTime = std::numeric_limits<double>::max();
            return {};
        }

        // ourModifiedProc->readyTimeCompute = minTTF;
        // finTime = ourModifiedProc->readyTimeCompute;
        // assert(ourModifiedProc->readyTimeCompute < std::numeric_limits<double>::max());

        if (timeToFinishNoEvicted == minTTF) {
            resultingVar = 1;
        } else if (timeToFinishBiggestEvicted == minTTF) {

            std::shared_ptr<Event> eventStartFromQueue = events.find(
                buildEdgeName(biggestPendingEdge) + "-w-s");
            std::shared_ptr<Event> eventFinishFromQueue = events.find(buildEdgeName(biggestPendingEdge) + "-w-f");
            if (eventStartFromQueue == nullptr && eventFinishFromQueue == nullptr) {
                // not scheduled to write yet or already written

                if (isLocatedOnDisk(biggestPendingEdge, false)) {
                    // already written to disk, enough to remove from pending memories.
                    ourModifiedProc->removePendingMemory(biggestPendingEdge);
                } else {
                    std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>> writeEvents;
                    scheduleWriteForEdge(ourModifiedProc, biggestPendingEdge, writeEvents, notEarlierThan);
                    newEvents.emplace_back(writeEvents.first);
                    newEvents.emplace_back(writeEvents.second);
                }
            } else{
                if(ourModifiedProc->getPendingMemories().find(biggestPendingEdge)!=ourModifiedProc->getPendingMemories().end() ){
                    //edge scheduled for writing, still needs to be removed from pending memories now
                    ourModifiedProc->removePendingMemory(biggestPendingEdge);
                    assert(!eventFinishFromQueue || eventFinishFromQueue->getVisibleTimeFireForPlanning()<= startTimeOfTaskFor1Evicted);
                }
            }

            resultingVar = 2;
            assert(startTime <= startTimeOfTaskFor1Evicted);
            startTime = startTimeOfTaskFor1Evicted;

            assert(biggestPendingEdge != nullptr);
        } else if (timeToFinishTaskAllEvicted == minTTF) {
            //  cout<<"case 3 avail mem "<<ourModifiedProc->getAvailableMemory()<<" "<<ourModifiedProc->getAfterAvailableMemory()<<endl;
            resultingVar = 3;
            std::set<edge_t*, std::function<bool(edge_t*, edge_t*)>> temp(Processor::comparePendingMemories);
            while (!ourModifiedProc->getPendingMemories().empty()) {
                edge_t* edge = (*ourModifiedProc->getPendingMemories().begin());
                //    cout << buildEdgeName(edge) << endl;
                if (edge->head->name == vertex->name) {
                    temp.insert(edge);
                    ourModifiedProc->removePendingMemory(edge);
                } else {
                    std::shared_ptr<Event> eventStartFromQueue = events.find(
                        buildEdgeName(edge) + "-w-s");
                    std::shared_ptr<Event> eventFinishFromQueue = events.find(buildEdgeName(edge) + "-w-f");

                    if (eventStartFromQueue == nullptr && eventFinishFromQueue == nullptr) {
                        // not scheduled to write yet or already written
                        if (isLocatedOnDisk(edge, false)) {
                            // already written to disk, enough to remove from pending memories.
                            ourModifiedProc->removePendingMemory(edge);
                        } else {
                            std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>> writeEvents;
                            scheduleWriteForEdge(ourModifiedProc, edge, writeEvents, notEarlierThan);
                            newEvents.emplace_back(writeEvents.first);
                            newEvents.emplace_back(writeEvents.second);
                        }
                    } else {
                        ourModifiedProc->removePendingMemory(edge);
                    }
                }
            }
            for (auto& temEd : temp) {
                ourModifiedProc->addPendingMemory(temEd);
            }

            assert(startTime <= startTimeOfTaskForAllEvicted);
            startTime = startTimeOfTaskForAllEvicted;
        }

        processIncomingEdges(vertex, eventStartTask, ourModifiedProc, modifiedProcs, newEvents, notEarlierThan);

        if (timeToFinishNoEvicted == minTTF) {
            assert(ourModifiedProc->getAvailableMemory() <= ourModifiedProc->getMemorySize());
            actuallyUsedMemory = ourModifiedProc->getAvailableMemory();
            ourModifiedProc->setAvailableMemory(0);
        } else if (timeToFinishBiggestEvicted == minTTF) {
            actuallyUsedMemory = std::min(ourModifiedProc->getAvailableMemory(), peakMemoryRequirementOfVertex(vertex));
            assert(actuallyUsedMemory <= ourModifiedProc->getMemorySize());
            ourModifiedProc->setAvailableMemory(std::max(0.0,
                ourModifiedProc->getAvailableMemory() - vertex->memoryRequirement));
        } else if (timeToFinishTaskAllEvicted == minTTF) {
            actuallyUsedMemory = std::min(ourModifiedProc->getMemorySize(), peakMemoryRequirementOfVertex(vertex));
            ourModifiedProc->setAvailableMemory(std::max(0.0,
                ourModifiedProc->getMemorySize() - peakMemoryRequirementOfVertex(vertex)));
            //  cout<<"case 3 end avail mem "<<ourModifiedProc->getAvailableMemory()<<" "<<ourModifiedProc->getAfterAvailableMemory()<<endl;
        }

    } else {
        processIncomingEdges(vertex, eventStartTask, ourModifiedProc, modifiedProcs, newEvents, notEarlierThan);

        actuallyUsedMemory = peakMemoryRequirementOfVertex(vertex);
        assert(actuallyUsedMemory <= ourModifiedProc->getMemorySize());
        //  ourModifiedProc->setAvailableMemory(
        //         ourModifiedProc->getAvailableMemory() - peakMemoryRequirementOfVertex(vertex));
    }
    startTime = std::max({ ourModifiedProc->getExpectedOrActualReadyTimeCompute(),
        ourModifiedProc->getExpectedOrActualReadyTimeRead(),
        startTime,
        eventStartTask->getExpectedTimeFire(), notEarlierThan });

    eventStartTask->adjustBothPlannedFireTimes(startTime);

    for (auto& newEvent : newEvents) {
        if (newEvent->id.find("-f") != std::string::npos) {
            eventStartTask->addPredecessorInPlanning(newEvent);
        }
    }

    finishTime =
        resultingVar<0? ( eventStartTask->getExpectedTimeFire() + vertex->time / ourModifiedProc->getProcessorSpeed()):
        finishTimeWithMemorySwapping(eventStartTask->getExpectedTimeFire() , resultingVar==1 ? std::abs(Res) :
            resultingVar==2 ? amountToOffloadWithoutBiggestFile  :amountToOffloadWithoutAllFiles,  vertex->time, vertex, ourModifiedProc );

     if (vertex->time == 0) {
        finishTime = finishTime + 0.0001;
    }

    eventFinishTask->adjustBothPlannedFireTimes(finishTime);

    if (eventStartTask->getExpectedTimeFire() >= eventFinishTask->getExpectedTimeFire()) {
        std::cout << " BAD START/FINSH TIME TASK " << eventStartTask->getExpectedTimeFire() << " "
                  << eventFinishTask->getExpectedTimeFire()
                  << "FOR TASK " << vertex->name << " vertex time  " << vertex->time << '\n';
    }
    assert(vertex->time == 0 || vertex->time / ourModifiedProc->getProcessorSpeed() < 0.001 || eventStartTask->getExpectedTimeFire() < eventFinishTask->getExpectedTimeFire());
    assert(eventStartTask->getExpectedTimeFire() == eventStartTask->getActualTimeFire());

    newEvents.emplace_back(eventStartTask);
    newEvents.emplace_back(eventFinishTask);

    if (eventStartTask->getExpectedTimeFire() >= eventFinishTask->getExpectedTimeFire()) {
        std::cout << " BAD START/FINSH TIME TASK " << eventStartTask->getExpectedTimeFire() << " "
                  << eventFinishTask->getExpectedTimeFire()
                  << "FOR TASK " << vertex->name << " vertex time  " << vertex->time << " duarion " << vertex->time / ourModifiedProc->getProcessorSpeed() << '\n';
    }
    assert(vertex->time == 0 || vertex->time / ourModifiedProc->getProcessorSpeed() < 0.001 || eventStartTask->getExpectedTimeFire() < eventFinishTask->getExpectedTimeFire());

    eventFinishTask->addPredecessorInPlanning(eventStartTask);
    if (!ourModifiedProc->getLastComputeEvent().expired() && !ourModifiedProc->getLastComputeEvent().lock()->isDone) {
        eventStartTask->addPredecessorInPlanning(ourModifiedProc->getLastComputeEvent().lock());
    }
    //  cout << "SET LAST COMPUTE EVENT TO " << eventFinishTask->id << endl;
    ourModifiedProc->setLastComputeEvent(eventFinishTask);
    finTime = ourModifiedProc->getReadyTimeCompute();

    return modifiedProcs;
}

double
processIncomingEdges(const vertex_t* v, const std::shared_ptr<Event>& ourEvent, const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs,
    std::vector<std::shared_ptr<Event>>& createdEvents, double notEarlierThan)
{

    assert(ourEvent->getExpectedTimeFire()>=notEarlierThan);
    // cout<<"processing, avail mem "<<ourModifiedProc->getAvailableMemory()<<endl;
    double howMuchWasLoaded = ourModifiedProc->getAvailableMemory();
    for (auto* incomingEdge : v->in_edges) {
        if (ourModifiedProc->getPendingMemories().find(incomingEdge) != ourModifiedProc->getPendingMemories().end()) {
           ourModifiedProc->getPendingMemories().erase(incomingEdge);
            continue;
        }

        const vertex_t* predecessor = incomingEdge->tail;
        if (predecessor->makespan > 0) {
            ourEvent->adjustBothPlannedFireTimes(std::max(ourEvent->getExpectedTimeFire(), predecessor->makespan));
        }

        if (isLocatedNowhere(incomingEdge, false)) {
            auto plannedOnThisProcIt = std::find_if(
                cluster->getProcessors().begin(), cluster->getProcessors().end(),
                [&](const auto& item) {
                    const auto& [proc_id, processor] = item;
                    if (proc_id == ourModifiedProc->id) {
                        return false; // Skip our own processor
                    }
                    if (processor->getPendingMemories().find(incomingEdge) != processor->getPendingMemories().end()) {
                        return true; // Found a processor with the edge in pending memories
                    }
                    if (processor->getAfterPendingMemories().find(incomingEdge) != processor->getAfterPendingMemories().end()) {
                        return true; // Found a processor with the edge in after pending memories
                    }
                    return false;
                });

            if (plannedOnThisProcIt == cluster->getProcessors().end()) {
                // it has been written to disk, but not yet fired the event
                //std::cout << "inc edge written to disk, not fired "<<buildEdgeName(incomingEdge)<<" "<< ourEvent->getExpectedTimeFire()<<" "<<notEarlierThan << std::endl;
                organizeAReadWhenWriteExists(v, incomingEdge, ourEvent, ourModifiedProc, createdEvents, notEarlierThan);

            } else {
                const auto& plannedOnThisProc = plannedOnThisProcIt->second;
                assert(findPredecessorsProcessor(incomingEdge, modifiedProcs)->id == plannedOnThisProc->id);
                std::shared_ptr<Event> eventStartFromQueue = events.find(buildEdgeName(incomingEdge) + "-w-s");
                std::shared_ptr<Event> eventFinishFromQueue = events.find(buildEdgeName(incomingEdge) + "-w-f");
                if (eventStartFromQueue != nullptr || eventFinishFromQueue != nullptr) {
                    // the write has already started, no other option but to finish it schedule only a read
                    organizeAReadWhenWriteExists(v, incomingEdge, ourEvent, ourModifiedProc, createdEvents, std::max(eventFinishFromQueue->getVisibleTimeFireForPlanning(), notEarlierThan));
                }
                scheduleWriteAndRead(v, ourEvent, createdEvents, ourEvent->getExpectedTimeFire(), ourModifiedProc, incomingEdge, modifiedProcs, notEarlierThan);
            }
        } else if (isLocatedOnThisProcessor(incomingEdge, ourModifiedProc->id, false)) {
            //   std::cout << "edge " << buildEdgeName(incomingEdge) << " already on proc" << std::endl;

                //there may be a scheduled write of this edge, we need to re-read it into the memory of our processor
                std::shared_ptr<Event> eventStartFromQueue = events.find(buildEdgeName(incomingEdge) + "-w-s");
                std::shared_ptr<Event> eventFinishFromQueue = events.find(buildEdgeName(incomingEdge) + "-w-f");
            if (eventStartFromQueue != nullptr || eventFinishFromQueue != nullptr) {
                // the write has already started, no other option but to finish it schedule only a read
                organizeAReadWhenWriteExists(v, incomingEdge, ourEvent, ourModifiedProc, createdEvents, std::max(notEarlierThan,eventFinishFromQueue->getVisibleTimeFireForPlanning()));
            }
        } else if (isLocatedOnDisk(incomingEdge, false)) {
            // schedule a read
            double atThisTime =  std::max(notEarlierThan,getLocationOnDisk(incomingEdge, false)->afterWhen.value());
            scheduleARead(v, ourEvent, createdEvents, ourEvent->getExpectedTimeFire(), ourModifiedProc, incomingEdge, atThisTime);
            if (atThisTime > ourEvent->getExpectedTimeFire()) {
                ourEvent->propagateAllSuccessorsForward(atThisTime - ourEvent->getExpectedTimeFire(), true);
                ourEvent->adjustBothPlannedFireTimes(atThisTime);
            }
        } else if (isLocatedOnAnyProcessor(incomingEdge, false)) {
            const auto predecessorsProc = findPredecessorsProcessor(incomingEdge, modifiedProcs);
            if (predecessorsProc->getAfterPendingMemories().find(incomingEdge) == predecessorsProc->getAfterPendingMemories().end()) {
                //    std::cout << "edge " << buildEdgeName(incomingEdge) << " not found in after pending mems on proc "
                //        << predecessorsProc->id << std::endl;
                auto plannedWriteFinishOfIncomingEdge = events.find(buildEdgeName(incomingEdge) + "-w-f");
                assert(plannedWriteFinishOfIncomingEdge != nullptr);
                std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>> readEVents;
                const double prev = plannedWriteFinishOfIncomingEdge->getVisibleTimeFireForPlanning();
                if (plannedWriteFinishOfIncomingEdge->getVisibleTimeFireForPlanning() > ourEvent->getExpectedTimeFire()
                    && plannedWriteFinishOfIncomingEdge->getVisibleTimeFireForPlanning() > ourModifiedProc->getExpectedOrActualReadyTimeRead()) {
                    double atWhatTime = std::max(plannedWriteFinishOfIncomingEdge->getVisibleTimeFireForPlanning(), notEarlierThan);
                    readEVents = scheduleARead(v, ourEvent, createdEvents, ourEvent->getExpectedTimeFire(), ourModifiedProc, incomingEdge, atWhatTime);

                    if (atWhatTime > ourEvent->getExpectedTimeFire()) {
                        ourEvent->adjustBothPlannedFireTimes(atWhatTime);
                    }
                } else {

                    readEVents = scheduleARead(v, ourEvent, createdEvents, ourEvent->getExpectedTimeFire(), ourModifiedProc, incomingEdge, notEarlierThan);
                }
                readEVents.first->addPredecessorInPlanning(plannedWriteFinishOfIncomingEdge);
                assert(prev == plannedWriteFinishOfIncomingEdge->getVisibleTimeFireForPlanning());
                //assert(incomingEdge->weight < 1 || readEVents.first->getActualTimeFire() < readEVents.second->getActualTimeFire());
            } else {
                // schedule a write
                std::shared_ptr<Event> eventStartFromQueue = events.find(buildEdgeName(incomingEdge) + "-w-s");
                std::shared_ptr<Event> eventFinishFromQueue = events.find(buildEdgeName(incomingEdge) + "-w-f");
                if (eventStartFromQueue != nullptr || eventFinishFromQueue != nullptr) {
                    // the write has already started, no other option but to finish it schedule only a read
                    organizeAReadWhenWriteExists(v, incomingEdge, ourEvent, ourModifiedProc, createdEvents, std::max(eventFinishFromQueue->getVisibleTimeFireForPlanning(), notEarlierThan));
                } else {
                    scheduleWriteAndRead(v, ourEvent, createdEvents, ourEvent->getExpectedTimeFire(), ourModifiedProc, incomingEdge, modifiedProcs, notEarlierThan);
                }
            }
        }
    }
    howMuchWasLoaded = howMuchWasLoaded - ourModifiedProc->getAvailableMemory();
    assert(howMuchWasLoaded >= 0);
    //   cout<<"loaded all incoming, avail mem "<<ourModifiedProc->getAvailableMemory()<<endl;
    ourModifiedProc->setAvailableMemory(ourModifiedProc->getAvailableMemory() + howMuchWasLoaded);
    //  cout<<"finally, avail mem "<<ourModifiedProc->getAvailableMemory()<<endl;
    return 0;
}

void organizeAReadWhenWriteExists(const vertex_t* v, edge_t* incomingEdge, const std::shared_ptr<Event>& ourEvent,
    const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Event>>& createdEvents, const double afterWhen)
{
    double atWhatTime = afterWhen;
    const auto readEvents = scheduleARead(v, ourEvent, createdEvents, ourEvent->getExpectedTimeFire(),
        ourModifiedProc,
        incomingEdge, atWhatTime);
    assert(readEvents.first.get()->getExpectedTimeFire()>= atWhatTime);
    assert(readEvents.second.get()->getExpectedTimeFire()>= atWhatTime);

    const std::shared_ptr<Event>& eventFinishThisEdgeWrite = events.find(
        buildEdgeName(incomingEdge) + "-w-f");
    if (eventFinishThisEdgeWrite != nullptr) {
        const double eventFinishThisEdgeWritebef = eventFinishThisEdgeWrite->getVisibleTimeFireForPlanning();
        readEvents.first->addPredecessorInPlanning(eventFinishThisEdgeWrite);
        assert(eventFinishThisEdgeWritebef == eventFinishThisEdgeWrite->getExpectedTimeFire());
    } else {
        if (isLocatedOnDisk(incomingEdge, false) || incomingEdge->tail->name == "GRAPH_SOURCE") {
        } else {
            std::cout << "no event finish write - AND THE FILE IS NOT ON DISK " << buildEdgeName(incomingEdge)
                      << '\n';
        }
    }
}

std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>>
scheduleARead(const vertex_t* v, const std::shared_ptr<Event>& ourEvent, std::vector<std::shared_ptr<Event>>& createdEvents,
    const double startTimeOfTask,
    const std::shared_ptr<Processor>& ourModifiedProc, edge_t* incomingEdge, double notEarlierThan)
{
    assert(events.find(buildEdgeName(incomingEdge) + "-r-s") == nullptr);
    assert(events.find(buildEdgeName(incomingEdge) + "-r-f") == nullptr);

    double estimatedStartOfRead = std::max(startTimeOfTask - incomingEdge->weight / ourModifiedProc->readSpeedDisk, notEarlierThan);
    estimatedStartOfRead = std::max(estimatedStartOfRead, ourModifiedProc->getExpectedOrActualReadyTimeRead());

    std::vector<std::shared_ptr<Event>> predsOfRead;
    std::vector<std::weak_ptr<Event>> succsOfRead;
    auto startOfTaskAtTheEndOfEdge = events.find(v->name + "-s");
    if (startOfTaskAtTheEndOfEdge != nullptr) {
        //why does it exist?
        throw new std::runtime_error( "already exists a task start for a planned read, read of "+ buildEdgeName(incomingEdge));
    }

    // if this start of the read is happening during the runtime  of the previous task
    assert(ourModifiedProc->getLastComputeEvent().expired() ||  !ourModifiedProc->getLastComputeEvent().lock()->isDone
        || std::abs(ourModifiedProc->getReadyTimeCompute() - ourModifiedProc->getLastComputeEvent().lock()->getActualTimeFire()) < 0.001);



    bool isDependentOnLastCompute=false;
    if (estimatedStartOfRead < ourModifiedProc->getExpectedOrActualReadyTimeCompute() && ourModifiedProc->availableMemoryDuringPreviousTask < incomingEdge->weight) {
        assert(!ourModifiedProc->getLastComputeEvent().lock()->isDone);
        estimatedStartOfRead = ourModifiedProc->getExpectedOrActualReadyTimeCompute();
        isDependentOnLastCompute = true;

    }

    std::vector<std::shared_ptr<Event>> newEvents = evictFilesUntilThisFits(ourModifiedProc, incomingEdge, notEarlierThan);
    createdEvents.insert(createdEvents.end(), newEvents.begin(), newEvents.end());

    //??????If the starting time of the read is during the execution of the previous task and there
    // is enough memory to read this file, then we move the read forward to the beginning of this previous task.
    auto eventStartRead = Event::createEvent(nullptr, incomingEdge, OnReadStart, ourModifiedProc,
        estimatedStartOfRead, estimatedStartOfRead, predsOfRead,    succsOfRead, false,
        buildEdgeName(incomingEdge) + "-r-s");
    createdEvents.emplace_back(eventStartRead);
    if (!ourModifiedProc->getLastReadEvent().expired() && !ourModifiedProc->getLastReadEvent().lock()->isDone) {
        const double prev = ourModifiedProc->getLastReadEvent().lock()->getActualTimeFire();
        eventStartRead->addPredecessorInPlanning(ourModifiedProc->getLastReadEvent().lock());
        assert(prev == ourModifiedProc->getLastReadEvent().lock()->getActualTimeFire());
    }

    auto finishWriteAlreadyExisting = events.find(buildEdgeName(incomingEdge) + "-w-f");
    if (finishWriteAlreadyExisting != nullptr) {
        eventStartRead->addPredecessorInPlanning(finishWriteAlreadyExisting);
    }

    const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find(incomingEdge->tail->name + "-f");
    if (eventFinishPredecessorComputing != nullptr) {
        const double prev = eventFinishPredecessorComputing->getActualTimeFire();
        eventStartRead->addPredecessorInPlanning(eventFinishPredecessorComputing);
        assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
    } else {
        if (incomingEdge->tail->status == Status::Finished) {
            if (eventStartRead->getExpectedTimeFire() < incomingEdge->tail->makespan) {
                eventStartRead->adjustBothPlannedFireTimes(incomingEdge->tail->makespan);
            }
        } else {
            std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
        }
    }

    if (isDependentOnLastCompute && !ourModifiedProc->getLastComputeEvent().expired() ) {
        assert(!ourModifiedProc->getLastComputeEvent().lock()->isDone);
        eventStartRead->addPredecessorInPlanning(ourModifiedProc->getLastComputeEvent().lock());
    }

    const double estimatedTimeOfFinishRead = eventStartRead->getExpectedTimeFire() + incomingEdge->weight / ourModifiedProc->readSpeedDisk;

    predsOfRead.clear();
    succsOfRead.clear();
    auto eventFinishRead = Event::createEvent(nullptr, incomingEdge, OnReadFinish, ourModifiedProc,
        estimatedTimeOfFinishRead, estimatedTimeOfFinishRead, predsOfRead,
        succsOfRead, false,
        buildEdgeName(incomingEdge) + "-r-f");
    ourEvent->addPredecessorInPlanning(eventFinishRead);

    if (startOfTaskAtTheEndOfEdge != nullptr) {
        const auto eventSucc = startOfTaskAtTheEndOfEdge;
        eventSucc->addPredecessorInPlanning(eventFinishRead);
    }
    eventFinishRead->addPredecessorInPlanning(eventStartRead);

    createdEvents.emplace_back(eventFinishRead);

    // ourModifiedProc->readyTimeRead = estimatedTimeOfFinishRead;
    ourModifiedProc->setLastReadEvent(eventFinishRead);
    ourModifiedProc->addPendingMemory(incomingEdge);
    assert(eventFinishRead->getActualTimeFire() == eventFinishRead->getExpectedTimeFire());
    if (incomingEdge->weight / ourModifiedProc->readSpeedDisk > 0.2) {
        if (eventFinishRead->getExpectedTimeFire() <= eventStartRead->getExpectedTimeFire()) {
            std::cout << "BAD TIMES FINISH AND START READ FOR " << buildEdgeName(incomingEdge) << " FINISH AT "
                      << eventFinishRead->getExpectedTimeFire()
                      << " START AT " << eventStartRead->getExpectedTimeFire() << " planned finish  at "
                      << estimatedTimeOfFinishRead << " duration of edge "
                      << incomingEdge->weight / ourModifiedProc->readSpeedDisk << '\n';
            std::cout << "was finihs moved? "
                      << (eventFinishRead->getExpectedTimeFire() == estimatedTimeOfFinishRead ? "no" : "yes") << '\n';
        }
        assert(eventFinishRead->getExpectedTimeFire() >= eventStartRead->getExpectedTimeFire());
    }
    const auto actualLength = eventFinishRead->getExpectedTimeFire() - eventStartRead->getExpectedTimeFire();
    // if(abs(actualLength - incomingEdge->weight / ourModifiedProc->readSpeedDisk) > 0.00001){
    // cerr<<"WRONG LENGTH OF READ PLANNED ON "<<buildEdgeName(incomingEdge)<<" actual length "<<actualLength<<" should be "<<to_string(incomingEdge->weight / ourModifiedProc->readSpeedDisk)<<endl;
    //}
    assert(std::abs(actualLength - incomingEdge->weight / ourModifiedProc->readSpeedDisk) < 1);

    assert(eventStartRead.get()->getExpectedTimeFire()>=notEarlierThan);
    assert(eventFinishRead.get()->getExpectedTimeFire()>=notEarlierThan);
    return { eventStartRead, eventFinishRead };
}

std::vector<std::shared_ptr<Event>> evictFilesUntilThisFits(const std::shared_ptr<Processor>& thisProc, edge_t* edgeToFit, double notEarlierThan)
{
    assert(thisProc->getPendingMemories().find(edgeToFit) == thisProc->getPendingMemories().end());
    const double weightToFit = edgeToFit->weight;
    std::vector<std::shared_ptr<Event>> newEvents;
    if (thisProc->getAvailableMemory() >= weightToFit) {
        return newEvents;
    }
    //  cout<<"evict for "<<buildEdgeName(edgeToFit)<<endl;
    auto begin = thisProc->getPendingMemories().begin();
    while (begin != thisProc->getPendingMemories().end()) {
        edge_t* edgeToEvict = *begin;
        if (thisProc->getAvailableMemory() < weightToFit && edgeToEvict->head->name != edgeToFit->head->name) {
            // cout<<"evict "<<buildEdgeName(edgeToEvict)<<endl;

            std::shared_ptr<Event> eventPreemptiveStart = events.find(buildEdgeName(edgeToEvict) + "-w-s");
            std::shared_ptr<Event> eventPreemptiveFinish = events.find(buildEdgeName(edgeToEvict) + "-w-f");

            if (eventPreemptiveStart == nullptr && eventPreemptiveFinish == nullptr) {
                if (isLocatedOnDisk(edgeToEvict, false)) {
                    //it has been written out preemptively and can be simply removed from the pending memories
                    delocateFromThisProcessorToNowhere(edgeToEvict, thisProc->id,imaginedCluster);
                    begin= thisProc->removePendingMemory(edgeToEvict);
                }
                else {
                    assert(!isLocatedOnDisk(edgeToEvict, false));
                    std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>> writeEvents;
                    begin = scheduleWriteForEdge(thisProc, edgeToEvict, writeEvents, notEarlierThan);
                    newEvents.emplace_back(writeEvents.first);
                    newEvents.emplace_back(writeEvents.second);
                }

            } else {
                ++begin;
            }

            // cout<<"evicted, new candidate is "<<buildEdgeName(*begin)<<endl;
        } else {
            //   cout<<"not evict"<<endl;
            ++begin;
        }
    }
    return newEvents;
}

// std::pair<shared_ptr<Event>, shared_ptr<Event>>
std::set<edge_t*, bool (*)(edge_t*, edge_t*)>::iterator
scheduleWriteForEdge(const std::shared_ptr<Processor>& thisProc, edge_t* edgeToWrite,
    std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>>& writeEvents, double notEarlierThan, const bool onlyPreemptive)
{
    // cout << "schedule write for edge evicting " << buildEdgeName(edgeToEvict) << endl;

    assert(events.find(buildEdgeName(edgeToWrite) + "-w-s") == nullptr);
    assert(events.find(buildEdgeName(edgeToWrite) + "-w-f") == nullptr);

    double possibleStartWrite = std::max(notEarlierThan, std::max(thisProc->getExpectedOrActualReadyTimeWrite(), edgeToWrite->tail->makespan));
    const auto eventStartWrite = Event::createEvent(nullptr, edgeToWrite, OnWriteStart, thisProc,
      possibleStartWrite,  possibleStartWrite, false, buildEdgeName(edgeToWrite) + "-w-s");

    if (!thisProc->getLastWriteEvent().expired() && !thisProc->getLastWriteEvent().lock()->isDone) {
        const double prev = thisProc->getLastWriteEvent().lock()->getActualTimeFire();
        eventStartWrite->addPredecessorInPlanning(thisProc->getLastWriteEvent().lock());
        eventStartWrite->addPredecessorInPlanning(thisProc->getLastWriteEvent().lock());
        assert(prev == thisProc->getLastWriteEvent().lock()->getActualTimeFire());
    }

    const auto eventOfFinishPredecessor = events.find(edgeToWrite->tail->name + "-f");
    if (eventOfFinishPredecessor == nullptr) {
        //  cout << " no event of finish prdecesor found for edge " << buildEdgeName(edgeToEvict) << endl;
        eventStartWrite->adjustBothPlannedFireTimes(std::max(eventStartWrite->getExpectedTimeFire(), edgeToWrite->tail->makespan));
    } else {
        // cout << " event of finish prdecesor FOUND for edge " << buildEdgeName(edgeToEvict) << endl;
        // cout << "it is " << eventOfFinishPredecessor->id << " at " << eventOfFinishPredecessor->actualTimeFire << endl;
        if (!eventOfFinishPredecessor->isDone) {
            eventStartWrite->addPredecessorInPlanning(eventOfFinishPredecessor);
        }
    }

    const double timeFinishWrite = eventStartWrite->getExpectedTimeFire() + edgeToWrite->weight / thisProc->writeSpeedDisk;

    const auto eventFinishWrite = Event::createEvent(nullptr, edgeToWrite, OnWriteFinish, thisProc,
        timeFinishWrite,  timeFinishWrite,   false,    buildEdgeName(edgeToWrite) + "-w-f");

    eventFinishWrite->addPredecessorInPlanning(eventStartWrite);
    thisProc->setLastWriteEvent(eventFinishWrite);

    assert(thisProc->getAvailableMemory() >= 0);
    writeEvents.first = eventStartWrite;
    writeEvents.second = eventFinishWrite;

    assert(eventStartWrite->getActualTimeFire() == eventStartWrite->getExpectedTimeFire());
    assert(eventFinishWrite->getExpectedTimeFire() == eventStartWrite->getExpectedTimeFire() + edgeToWrite->weight / thisProc->writeSpeedDisk);

    if (!onlyPreemptive) {
        auto it = thisProc->getAfterPendingMemories().find(edgeToWrite);
        if (it != thisProc->getAfterPendingMemories().end()) {
            thisProc->removePendingMemoryAfter(edgeToWrite);
        }
        return thisProc->removePendingMemory(edgeToWrite);
    }

    writeEvents.first->onlyPreemptive = true;
    writeEvents.second->onlyPreemptive = true;
    return thisProc->getPendingMemories().find(edgeToWrite);
}

void scheduleWriteAndRead(const vertex_t* v, const std::shared_ptr<Event>& ourEvent, std::vector<std::shared_ptr<Event>>& createdEvents,
    const double startTimeOfTask, const std::shared_ptr<Processor>& ourModifiedProc, edge_t* incomingEdge,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs, double notEarlierThan)
{
    // cout << "scehdule write and read for " << buildEdgeName(incomingEdge) << endl;

    const auto predecessorsProc = findPredecessorsProcessor(incomingEdge, modifiedProcs);
    double estimatedStartOfRead = std::max(startTimeOfTask - incomingEdge->weight / ourModifiedProc->readSpeedDisk, notEarlierThan);
    estimatedStartOfRead = std::max(estimatedStartOfRead, ourModifiedProc->getExpectedOrActualReadyTimeRead());

    assert(ourModifiedProc->getLastComputeEvent().expired() ||
        ! ourModifiedProc->getLastComputeEvent().lock()->isDone ||
            std::abs(ourModifiedProc->getReadyTimeCompute() - ourModifiedProc->getLastComputeEvent().lock()->getActualTimeFire()) < 0.001);

    if (estimatedStartOfRead < ourModifiedProc->getExpectedOrActualReadyTimeCompute() && ourModifiedProc->getAvailableMemory() < incomingEdge->weight) {
        estimatedStartOfRead = ourModifiedProc->getExpectedOrActualReadyTimeCompute();
    }

    double estimatedStartOfWrite = estimatedStartOfRead - incomingEdge->weight / predecessorsProc->writeSpeedDisk;
    estimatedStartOfWrite = std::max(estimatedStartOfWrite,  std::max(notEarlierThan, predecessorsProc->getExpectedOrActualReadyTimeWrite()));

    auto finishOfTailTask = events.find(incomingEdge->tail->name + "-f");
    if (finishOfTailTask != nullptr) {
        estimatedStartOfWrite = std::max(estimatedStartOfWrite,
            finishOfTailTask->getVisibleTimeFireForPlanning());
    } else {
        assert(incomingEdge->tail->makespan != -1);
        estimatedStartOfWrite = std::max(estimatedStartOfWrite, incomingEdge->tail->makespan);
    }

    estimatedStartOfRead = estimatedStartOfWrite + incomingEdge->weight / predecessorsProc->writeSpeedDisk;
    const double estimatedTimeOfReadyRead = estimatedStartOfRead + incomingEdge->weight / ourModifiedProc->readSpeedDisk;

    const std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>> readEvents = scheduleARead(v, ourEvent, createdEvents,
        startTimeOfTask, ourModifiedProc,
        incomingEdge, estimatedStartOfRead);
    if (readEvents.second->getExpectedTimeFire() > estimatedTimeOfReadyRead) {
        const double slack = readEvents.second->getExpectedTimeFire() - estimatedTimeOfReadyRead;
        estimatedStartOfWrite = estimatedStartOfWrite + slack;
    }

    auto eventStartWrite = Event::createEvent(nullptr, incomingEdge, OnWriteStart, predecessorsProc,
        estimatedStartOfWrite, estimatedStartOfWrite, false,
        buildEdgeName(incomingEdge) + "-w-s");
    assert(eventStartWrite->getExpectedTimeFire() == estimatedStartOfWrite);
    assert(eventStartWrite->getExpectedTimeFire() == eventStartWrite->getActualTimeFire());

    if (!predecessorsProc->getLastWriteEvent().expired() && !predecessorsProc->getLastWriteEvent().lock()->isDone) {
        eventStartWrite->addPredecessorInPlanning(predecessorsProc->getLastWriteEvent().lock());
    }

    if (finishOfTailTask != nullptr) {
        assert(finishOfTailTask->getVisibleTimeFireForPlanning() <= eventStartWrite->getExpectedTimeFire());
        eventStartWrite->addPredecessorInPlanning(finishOfTailTask);
    }

    createdEvents.emplace_back(eventStartWrite);

    double estimatedTimeOfFinishWrite = eventStartWrite->getExpectedTimeFire() + incomingEdge->weight / predecessorsProc->writeSpeedDisk;
    if ( estimatedTimeOfFinishWrite == eventStartWrite->getExpectedTimeFire()) {
        estimatedTimeOfFinishWrite+=2;
    }
    auto eventFinishWrite = Event::createEvent(nullptr, incomingEdge, OnWriteFinish, predecessorsProc,
        estimatedTimeOfFinishWrite, estimatedTimeOfFinishWrite, false,
        buildEdgeName(incomingEdge) + "-w-f");

    auto startOfHeadTask = events.find(v->name + "-s");
    if (startOfHeadTask) {
        throw new std::runtime_error("When scheduling an incoming edge "+ buildEdgeName(incomingEdge)+", found start of task of head: "+ startOfHeadTask->id);
    }
    eventFinishWrite->addPredecessorInPlanning(eventStartWrite);
    readEvents.first->addPredecessorInPlanning(eventFinishWrite);

    assert(estimatedTimeOfFinishWrite <= readEvents.first->getExpectedTimeFire());
    assert(eventStartWrite->getExpectedTimeFire()>=notEarlierThan);
    assert(readEvents.first->getExpectedTimeFire()>=notEarlierThan);

    assert(incomingEdge->weight < 1 || estimatedTimeOfFinishWrite > eventStartWrite->getExpectedTimeFire());

    createdEvents.emplace_back(eventFinishWrite);
    predecessorsProc->setLastWriteEvent(eventFinishWrite);

    predecessorsProc->removePendingMemoryAfter(incomingEdge);
    readEvents.first->addPredecessorInPlanning(eventFinishWrite);

    // cout << buildEdgeName(incomingEdge)<< " start write at " << eventStartWrite->getActualTimeFire() << " finish at "
    //      << eventFinishWrite->getActualTimeFire() << endl;
    assert(incomingEdge->weight < 1 || eventStartWrite->getActualTimeFire() < eventFinishWrite->getActualTimeFire());
    //assert(eventFinishWrite->getActualTimeFire() == eventStartWrite->getActualTimeFire() + incomingEdge->weight / predecessorsProc->writeSpeedDisk);
}

void buildPendingMemoriesAfter(const std::shared_ptr<Processor>& ourModifiedProc, const vertex_t* ourVertex)
{
    assert(ourVertex->memoryRequirement == 0 || (ourVertex->actuallyUsedMemory != -1 && ourVertex->actuallyUsedMemory != 0));
    //   cout << "act used " << ourVertex->actuallyUsedMemory << endl;
    // ourModifiedProc->setAfterAvailableMemory(ourModifiedProc->getAvailableMemory() + ourVertex->actuallyUsedMemory);
    ourModifiedProc->setAfterAvailableMemory(ourModifiedProc->getMemorySize());
    bool wasMemWrong = false;
    for (auto& item : ourModifiedProc->getPendingMemories()) {
        try {
            ourModifiedProc->addPendingMemoryAfter(item);
        } catch (...) {
           // std::cout << "memor temporaroly wrong!" << '\n';
            wasMemWrong = true;
        }
    }
    //  assert(ourModifiedProc->getAfterAvailableMemory() >= 0);
    // cout << "after adding " << endl;
    for (auto in_edge : ourVertex->in_edges) {
        if (ourModifiedProc->getAfterPendingMemories().find(in_edge) == ourModifiedProc->getAfterPendingMemories().end()) {
            //  cout << "edge " << buildEdgeName(ourVertex->in_edges[j]) << " not found in after pending mems on proc "
            //      << ourModifiedProc->id << endl;
        } else {
            ourModifiedProc->removePendingMemoryAfter(in_edge);
        }
    }
    for (int j = 0; j < ourVertex->out_edges.size(); j++) {
        ourModifiedProc->addPendingMemoryAfter(ourVertex->out_edges.at(j));
        assert(ourVertex->time == 0 || ourModifiedProc->getAfterPendingMemories().find(ourVertex->out_edges.at(j)) != ourModifiedProc->getAfterPendingMemories().end());
    }
    // ourModifiedProc->setAfterAvailableMemory(
    //        min( ourModifiedProc->getMemorySize(),
    //       ourModifiedProc->getAfterAvailableMemory()+ourVertex->actuallyUsedMemory));

    if (wasMemWrong) {
        assert(ourModifiedProc->getAvailableMemory() >= 0 && ourModifiedProc->getAvailableMemory() < ourModifiedProc->getMemorySize());
        assert(ourModifiedProc->getAfterAvailableMemory() >= 0 && ourModifiedProc->getAfterAvailableMemory() < ourModifiedProc->getMemorySize());
    }
}

double assessWritingOfEdge(const edge_t* edge, const std::shared_ptr<Processor>& proc)
{
    return edge->weight / proc->writeSpeedDisk;
}

void checkBestEvents(std::vector<std::shared_ptr<Event>>& bestEvents)
{
    for (auto& item : bestEvents) {
        std::cout << item->id << " at " << item->getExpectedTimeFire() << ", " << '\n';
        assert(item->getExpectedTimeFire() == item->getActualTimeFire());
        assert(!item->checkCycleFromEvent());

        if (item->id.find("-w-f") != std::string::npos) {
            auto itWriteStart = std::find_if(bestEvents.begin(), bestEvents.end(),
                [item](const std::shared_ptr<Event>& e) {
                    return e->id == item->id.substr(0, item->id.length() - 4) + "-w-s";
                });
            if (itWriteStart != bestEvents.end()) {
                const auto actualLength = item->getExpectedTimeFire() - (*itWriteStart)->getExpectedTimeFire();
                assert(std::abs(actualLength - item->edge->weight / item->processor->writeSpeedDisk) < 0.00001);
            } else {
                throw std::runtime_error("no pair found ofr " + item->id);
            }
        }
        if (item->id.find("-r-f") != std::string::npos) {
            auto itReadStart = std::find_if(bestEvents.begin(), bestEvents.end(),
                [item](const std::shared_ptr<Event>& e) {
                    return e->id == item->id.substr(0, item->id.length() - 4) + "-r-s";
                });
            if (itReadStart != bestEvents.end()) {
                const auto actualLength = item->getExpectedTimeFire() - (*itReadStart)->getExpectedTimeFire();
                assert(std::abs(actualLength - item->edge->weight / item->processor->readSpeedDisk) < 0.00001);
                std::cout << "ys" << '\n';
            } else {
                throw std::runtime_error("no pair found ofr " + item->id);
            }
        }
    }
    std::cout << '\n';
}
