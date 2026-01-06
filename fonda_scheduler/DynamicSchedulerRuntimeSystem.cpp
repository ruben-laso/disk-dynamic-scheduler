#include <queue>
#include <random>

#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "fonda_scheduler/algorithms.hpp"

Cluster* cluster;
EventManager events;
ReadyQueue readyQueue;
int devationVariant;
bool usePreemptiveWrites;

std::string lastEventName;
double runtimeOfScheduler;
double timeInSystem;

double dynMedih(graph_t* graph, Cluster* cluster1, const int algoNum, const int deviationNumber, const bool upw, double& runtime)
{
    double resMakespan = -1;
    cluster = cluster1;
    enforce_single_source_and_target_with_minimal_weights(graph);
    compute_bottom_and_top_levels(graph);
    devationVariant = deviationNumber;
    usePreemptiveWrites = upw;
    timeInSystem=0;

    const auto start = std::chrono::system_clock::now();
    vertex_t* vertex = graph->first_vertex;
    switch (algoNum) {
    case fonda_scheduler::HEFT:
    case fonda_scheduler::HEFT_BL: {
        while (vertex != nullptr) {
            vertex->rank = calculateSimpleBottomUpRank(vertex);
            vertex = vertex->next;
        }
        break;
    }
    case fonda_scheduler::HEFT_BLC: {
        while (vertex != nullptr) {
            vertex->rank = calculateBLCBottomUpRank(vertex);
            vertex = vertex->next;
        }
        break;
    }
    case fonda_scheduler::HEFT_MM: {
        std::vector<std::pair<vertex_t*, double>> ranks = calculateMMBottomUpRank(graph);
        std::for_each(ranks.begin(), ranks.end(), [](const std::pair<vertex_t*, double>& pair) {
            pair.first->rank = pair.second;
        });
        break;
    }
    default:
        throw std::runtime_error("unknown algorithm");
    }

    if (findVertexByName(graph, "GRAPH_SOURCE") != nullptr) {
        remove_vertex(graph, findVertexByName(graph, "GRAPH_SOURCE"));
        remove_vertex(graph, findVertexByName(graph, "GRAPH_TARGET"));
    }

    vertex = graph->first_vertex;
    while (vertex != nullptr) {
        // Schedule events without predecessors (i.e., starting tasks)
        if (vertex->in_edges.empty()) {
            //  cout << "starting task " << vertex->name << endl;
            std::vector<std::shared_ptr<Processor>> bestModifiedProcs;
            std::shared_ptr<Processor> bestProcessorToAssign;
            int bestResultingVar;
            std::vector<std::shared_ptr<Event>> newEvents = bestTentativeAssignment(vertex, bestModifiedProcs, bestProcessorToAssign, 0, bestResultingVar);

            for (auto& item : newEvents) {
                events.insert(item);
            }
            vertex->status = Status::Scheduled;
        }
        vertex = vertex->next;
    }

    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> elapsed_seconds = end - start;
    runtimeOfScheduler += elapsed_seconds.count();

    int cntr = 0;
    while (!events.empty()) {
        cntr++;

        // if (not events.checkPredecessorsSuccessors()) {
        //     throw std::runtime_error(std::to_string(cntr) + " - Graph has inconsistent event dependencies");
        // }

        const auto firstEvent = events.earliestReady();

        if (firstEvent->isDone) {
            throw std::runtime_error("Event " + firstEvent->id + " is already done, but it is in the queue.");
        }

        //  cout<<"finally event "<<firstEvent->id<<endl;
        assert(timeInSystem<=firstEvent->getActualTimeFire());
        timeInSystem= firstEvent->getActualTimeFire();
        firstEvent->fire();
        const bool removed = events.remove(firstEvent->id);
        assert(removed);
        resMakespan = std::max(resMakespan, firstEvent->getActualTimeFire());
        lastEventName = firstEvent->id;

        //  cout<<"events now "; events.printAll();
    }
    runtime = runtimeOfScheduler;
    averageSpreadPredecessors/=numTasksComputedPredecessors;

    double cv_of_processor_loads = CVOfProcessorLoads(processorLoads);
    double idleToWork = idleTimePercentage(processorWorkTimes);
    double idleToWorkOn30s = idleTimePercentageOn30s(processorWorkTimes);

 /*   std::cout << "avg spread "<<averageSpreadPredecessors<<
    " cv_proc_load "<< cv_of_processor_loads <<
    " num_used_procs "<<processorLoads.size()<<
    " idle_to_work "<<idleToWork<<
           " idle_to_work_30s "<<idleToWorkOn30s
    <<" "; */
    return resMakespan;
}

void Event::fireTaskStart()
{

   // std::cout << "On "<< this->processor->id <<" task start " << this->task->name << " at " << this->getActualTimeFire()<< std::endl;

    const auto canRun = dealWithPredecessors(shared_from_this());
    if (!canRun) {
        events.insert(shared_from_this());
        return;
    }

    //std::cout<<this->task->name<<" start "<<this->actualTimeFire<<" "<<this->processor->id<<"\n";
    removeFromDependencies();
    this->task->status = Status::Running;
    const auto ourFinishEvent = events.find(this->task->name + "-f");
    if (ourFinishEvent == nullptr) {
        throw std::runtime_error("NOt found finish event to " + this->task->name);
    }
    double durationTask = ourFinishEvent->getExpectedTimeFire() - this->getExpectedTimeFire();
    assert(durationTask > 0);
    assert(this->task->name == "GRAPH_SOURCE" || durationTask >= this->task->time / this->processor->getProcessorSpeed()
        || std::abs(durationTask - this->task->time / this->processor->getProcessorSpeed()) < 1);

    const auto factor = applyDeviationTo(durationTask);
    this->task->factorForRealExecution = factor;
    //std::cout << " duration "<<durationTask<< " factor "<< factor<<std::endl;
    assert(factor > 0);

    cleanupSuccessors();

    for (auto& weak_succ : successors) {
        if (auto succ = weak_succ.lock()) {
            if (succ->task == nullptr) {
                throw std::runtime_error("edge-based event depends on task start " + this->id);
            }
        }
        else {
            throw std::runtime_error("Invalid (nullptr) successor to " + this->task->name);
        }
    }


    const double d = this->getActualTimeFire() + durationTask;
    // cout << "on start  setting finish time from "<< ourFinishEvent->actualTimeFire <<" to " << d << endl;
    // ourFinishEvent->setActualTimeFire(d);
    events.reschedule(ourFinishEvent->id, d);
    cleanupSuccessors();
    for ( auto succ : ourFinishEvent->getSuccessors()) {
        if (!succ.expired() && succ.lock()->getActualTimeFire() < ourFinishEvent->getActualTimeFire()) {
            bool success = events.reschedule(succ.lock()->id, ourFinishEvent->getActualTimeFire());
            assert(success);
        }
    }

    for (auto inEdge : this->task->in_edges) {
        const std::string& edgeName = buildEdgeName(inEdge);
        std::shared_ptr<Event> startWrite = events.find(edgeName + "-w-s");
        std::shared_ptr<Event> finishWrite = events.find(buildEdgeName(inEdge) + "-w-f");
        if (startWrite != nullptr) {
            events.remove(startWrite->id);
            events.remove(finishWrite->id);
            // startWrite->removeFromSuccessors();
            // finishWrite->removeFromSuccessors();
            startWrite->removeFromDependencies();
            finishWrite->removeFromDependencies();

            startWrite->isDone = true;
            finishWrite->isDone = true;

            std::unordered_set<std::shared_ptr<Event>> visited;
            propagateChainInPlanning(finishWrite,
                startWrite->getActualTimeFire() - finishWrite->getActualTimeFire(), visited);
        } else if (finishWrite != nullptr) {
            bool success = events.reschedule(finishWrite->id, this->getActualTimeFire());
            assert(success);
        }

        for (auto& [proc_id, processor] : cluster->getProcessors()) {
            if (processor->writingQueue.empty()) {
                continue;
            }

            // Erase the inEdge from the writingQueue of the processor (if it exists)
            processor->writingQueue.erase(
                std::remove(processor->writingQueue.begin(), processor->writingQueue.end(), inEdge),
                processor->writingQueue.end());
        }
    }
    // cout << endl;
}

void Event::fireTaskFinish()
{
    const vertex_t* thisTask = this->task;
   // std::cout << "On "<<this->processor->id <<" task finish " << this->task->name << " at " << this->getActualTimeFire() << std::endl;

    const auto canRun = dealWithPredecessors(shared_from_this());
    if (!canRun) {
        events.insert(shared_from_this());
        return;
    }

    //std::cout<<this->task->name<<" finish "<<this->actualTimeFire<<" "<<this->processor->id<<"\n";
    removeFromDependencies();

    // set its status to finished
    this->task->status = Finished;
    this->isDone = true;
    this->task->makespan = this->actualTimeFire;

    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());

    assert(this->processor->getAvailableMemory() <= this->processor->getMemorySize());

    std::string thisId = this->id;

    for (const auto out_edge : thisTask->out_edges) {
        locateToThisProcessorFromNowhere(out_edge, this->processor->id, false,
            this->getActualTimeFire());
    }

    for (auto out_edge : thisTask->out_edges) {
        vertex_t* childTask = out_edge->head;
      //  std::cout << "deal with child " << childTask->name << std::endl;
        bool isReady = true;
        for (const auto& in_edge : childTask->in_edges) {
            if (in_edge->tail->status == Status::Unscheduled) {
              //  std::cout<<"unscheduled parent "<<in_edge->tail->name<<"\n";
                isReady = false;
            }
        }

        std::vector<std::shared_ptr<Event>> pred, succ;

        if (isReady && childTask->status == Status::Unscheduled) {
           // std::cout<<"inserting child task "<<childTask->name<<" into ready "<<std::endl;
            readyQueue.readyTasks.insert(childTask);
        }

        if (usePreemptiveWrites) {
            cluster->getProcessorById(this->processor->id)->writingQueue.emplace_back(out_edge);
        }
    }

    this->isDone = true;

    bool foundSomeTaskForOurProcessor = false;
    bool existsIdleProcessor = false;

    while ((!foundSomeTaskForOurProcessor || existsIdleProcessor) && !readyQueue.readyTasks.empty()) {
        vertex_t* mostReadyVertex = *readyQueue.readyTasks.begin();
        std::vector<std::shared_ptr<Processor>> bestModifiedProcs;
        std::shared_ptr<Processor> bestProcessorToAssign;
       // std::cout<<"assigning most ready vertex "<<mostReadyVertex->name<<" \n";
        auto start = std::chrono::system_clock::now();
        int bestResultingVar;
        std::vector<std::shared_ptr<Event>> newEvents = bestTentativeAssignment(mostReadyVertex, bestModifiedProcs, bestProcessorToAssign,
            this->actualTimeFire, bestResultingVar);

        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed_seconds = end - start;
        runtimeOfScheduler += elapsed_seconds.count();

        mostReadyVertex->status = Status::Scheduled;
        readyQueue.readyTasks.erase(mostReadyVertex);

        for (auto in_edge : mostReadyVertex->in_edges) {
            if (isLocatedOnAnyProcessor(in_edge, false)) {
                auto location = getLocationOnAnyProcessor(in_edge, false);
                auto p = cluster->getProcessorById((*location).processorId.value());

                const auto edgeInWritingQueue = std::find(p->writingQueue.begin(), p->writingQueue.end(),
            in_edge);
                if (edgeInWritingQueue != p->writingQueue.end()) {
                    // cluster->getProcessorById(this->processor->id)->writingQueue.erase(edgeInWritingQueue);
                    p->writingQueue.erase(edgeInWritingQueue);
                }
            }

        }

        if (bestProcessorToAssign->id == this->processor->id) {
            foundSomeTaskForOurProcessor = true;
        }

        for (auto& item : bestModifiedProcs) {
            //  cout<<item.use_count()<<" ";
            item.reset();
        }
        //  cout<<endl;
        // cout<<bestProcessorToAssign.use_count()<<endl;
        bestProcessorToAssign.reset();

        for (const auto& item : newEvents) {
            assert(cluster->getProcessorById(item->processor->id).use_count() == item->processor.use_count());
            events.insert(item);
            // cout<<"new event "<<item->id<<" w preds "<<endl;
            // for (const auto &item1: item->predecessors){
            //    cout<<item1->id<<", ";
            //}
            // cout <<endl<<"successors ";
            // for (const auto &item1: item->successors){
            //     cout<<item1->id<<", ";
            // }
            //  cout <<endl;
        }

        existsIdleProcessor = std::any_of(cluster->getProcessors().begin(), cluster->getProcessors().end(),
            [&](const auto& item) {
                const auto& [procId, processor] = item;
                return processor->getReadyTimeCompute() < this->getActualTimeFire();
            });
    }
}

std::shared_ptr<Processor> findPredecessorsProcessor(const edge_t* incomingEdge, std::vector<std::shared_ptr<Processor>>& modifiedProcs)
{
    const vertex_t* predecessor = incomingEdge->tail;
    const auto predecessorsProcessorsId = predecessor->assignedProcessorId;
    // assert(isLocatedOnThisProcessor(incomingEdge, predecessorsProcessorsId));

    const auto it = std::find_if(modifiedProcs.begin(), modifiedProcs.end(),
        [&](const std::shared_ptr<Processor>& p) {
            return p->id == predecessorsProcessorsId;
        });

    if (it != modifiedProcs.end()) {
        // Return the processor if found
        return *it;
    }

    // If not found, create a new processor based on the predecessor's assigned processor ID
    auto addedProc = std::make_shared<Processor>(*cluster->getProcessorById(predecessorsProcessorsId));
    modifiedProcs.emplace_back(addedProc);

    return addedProc;
}

void Event::fireReadStart()
{
    //std::cout << "On "<<this->processor->id <<" read start " << buildEdgeName(this->edge) << " at " << this->getActualTimeFire() << std::endl;
    // assert(finishRead->getActualTimeFire()> this->getActualTimeFire());
    const auto canRun = dealWithPredecessors(shared_from_this());

    if (!canRun) {
        //  cout << "BAD because #preds " << this->predecessors.size() << " esp " << (*this->predecessors.begin())->id
        //      << endl;
        events.insert(shared_from_this());
        return;
    }

    // cout << "DONE" << endl;
    removeFromDependencies();

    double durationOfRead = this->edge->weight / this->processor->readSpeedDisk;
    const double factor = applyDeviationTo(durationOfRead);
    assert(factor > 0);
    this->edge->factorForRealExecution = factor;
    const double expectedTimeFireFinish = this->actualTimeFire + durationOfRead;

    this->isDone = true;

    const std::shared_ptr<Event> finishRead = events.find(buildEdgeName(this->edge) + "-r-f");
    if (finishRead == nullptr) {
        throw std::runtime_error("NO read finish found for " + this->id);
    }

    events.reschedule(finishRead->id, expectedTimeFireFinish);

    for ( auto succ : finishRead->getSuccessors()) {
        if (!succ.expired() && succ.lock()->getActualTimeFire() < finishRead->getActualTimeFire()) {
            bool success = events.reschedule(succ.lock()->id, finishRead->getActualTimeFire());
            assert(success);
        }
    }

}

void Event::fireReadFinish()
{
   // std::cout << "On "<<this->processor->id <<" read finish " << buildEdgeName(this->edge) << " at " << this->getActualTimeFire() << std::endl;

    std::shared_ptr<Event> startRead = events.find(buildEdgeName(this->edge) + "-r-s");

    const auto canRun = dealWithPredecessors(shared_from_this());
    if (!canRun) {
        //  cout << "BAD because #preds " << this->predecessors.size() << " esp " << (*this->predecessors.begin())->id
        //    << endl;
        events.insert(shared_from_this());
        return;
    }
    // cout << "DONE " << endl;
    removeFromDependencies();

    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());
    if (!isLocatedOnDisk(this->edge, false)) {
        const auto ptr = events.find(buildEdgeName(this->edge) + "-w-f");
        assert(ptr != nullptr);
        auto ptr1 = events.find(buildEdgeName(this->edge) + "-r-s");
        assert(ptr1->getActualTimeFire() < this->getActualTimeFire());
    }
    locateToThisProcessorFromDisk(this->edge, this->processor->id, false, this->getActualTimeFire());
    this->isDone = true;
    this->edge->accountedFor=true;
}

void Event::fireWriteStart()
{
  //  std::cout << "On "<<this->processor->id <<" write start " << buildEdgeName(this->edge) << " at " << this->getActualTimeFire() << std::endl;

    const auto canRun = dealWithPredecessors(shared_from_this());
    if (!canRun) {
        //   cout << "BAD because #preds " << this->predecessors.size() << " esp " << (*this->predecessors.begin())->id
        //        << endl;
        events.insert(shared_from_this());
        return;
    }

    // cout << "DONE" << endl;
    removeFromDependencies();

    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());

    double durationOfWrite = this->edge->weight / this->processor->writeSpeedDisk;
    const double factor = applyDeviationTo(durationOfWrite);
    this->edge->factorForRealExecution = factor;
    assert(factor > 0);

    const double actualTimeFireFinish = this->actualTimeFire + durationOfWrite;
    this->isDone = true;
    const std::shared_ptr<Event> finishWrite = events.find(buildEdgeName(this->edge) + "-w-f");

    if (finishWrite == nullptr) {
        throw std::runtime_error("NO write finish found for " + this->id);
    }

    events.reschedule(finishWrite->id, actualTimeFireFinish);
    assert(finishWrite->getActualTimeFire()==actualTimeFireFinish);
    for ( auto succ : finishWrite->getSuccessors()) {
        if (!succ.expired() && succ.lock()->getActualTimeFire() < finishWrite->getActualTimeFire()) {
            bool success = events.reschedule(succ.lock()->id, finishWrite->getActualTimeFire());
            assert(success);
        }
    }

    assert(!finishWrite->checkCycleFromEvent());

    std::string thisid = buildEdgeName(this->edge);
    const auto edgeInWritingQueue = std::find(this->processor->writingQueue.begin(), this->processor->writingQueue.end(),
        this->edge);
    if (edgeInWritingQueue != this->processor->writingQueue.end()) {
        // cluster->getProcessorById(this->processor->id)->writingQueue.erase(edgeInWritingQueue);
        this->processor->writingQueue.erase(edgeInWritingQueue);
    }
}

void Event::fireWriteFinish()
{
  //  std::cout << "On "<<this->processor->id <<" write finish " << buildEdgeName(this->edge) << " at " << this->getActualTimeFire() << std::endl;

    const auto canRun = dealWithPredecessors(shared_from_this());
    if (!canRun) {
        // cout << "BAD because #preds " << this->predecessors.size() << " esp " << (*this->predecessors.begin())->id
        //     << endl;
        events.insert(shared_from_this());
        return;
    }

    // cout << "DONE" << endl;
    removeFromDependencies();

    if (this->onlyPreemptive) {
        locateToDisk(this->edge, false, this->getActualTimeFire());
        assert(isLocatedOnThisProcessor(this->edge, this->processor->id, false));
    } else {
        delocateFromThisProcessorToDisk(this->edge, this->processor->id, false, this->getActualTimeFire());
    }
    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());

    const auto positionInWriteQ = std::find(this->processor->writingQueue.begin(), this->processor->writingQueue.end(),
        this->edge);
    if (positionInWriteQ != this->processor->writingQueue.end()) {
        this->processor->writingQueue.erase(positionInWriteQ);
    }

    this->isDone = true;

    if (this->processor->writingQueue.empty()) {
        return;
    }

    edge_t* edgeToWriteJustInCase = this->processor->writingQueue.at(0);

    if (events.find(buildEdgeName(edgeToWriteJustInCase) + "-w-s") != nullptr || events.find(buildEdgeName(edgeToWriteJustInCase) + "-w-f") != nullptr
        || this->processor->getPendingMemories().find(edgeToWriteJustInCase) == this->processor->getPendingMemories().end()) {
        //std:: cout << "event for " << buildEdgeName(edgeToWriteJustInCase) << " already in queue" << endl;
        this->processor->writingQueue.erase(this->processor->writingQueue.begin());
        return;
    }

    const double presumedLength = assessWritingOfEdge(edgeToWriteJustInCase, this->processor);
    double startOfNextWrite = std::numeric_limits<double>::max();
    const std::set<std::shared_ptr<Event>, CompareByTimestamp> eventsOnThisProc = events.eventsForProcessor(
        this->processor->id);
    assert((*eventsOnThisProc.begin())->getActualTimeFire() <= (*eventsOnThisProc.rbegin())->getActualTimeFire());

    for (auto& item : eventsOnThisProc) {
        //  cout<<"event on proc "<<item->id<<" at "<<item->getActualTimeFire()<<endl;
        // events are ordered by timestamp, so iterating from earliest to latest
        if (item->type == eventType::OnWriteStart && item->getActualTimeFire() > this->getActualTimeFire()) {
            startOfNextWrite = item->getActualTimeFire();
            break;
        }
    }

    if (this->getActualTimeFire() + presumedLength < startOfNextWrite) {
        // can fit
        //std::cout << "scheduling extra write for " << buildEdgeName(edgeToWriteJustInCase) << std::endl;
        assert(events.find(buildEdgeName(edgeToWriteJustInCase) + "-w-s") == nullptr);
        assert(events.find(buildEdgeName(edgeToWriteJustInCase) + "-w-f") == nullptr);
        std::pair<std::shared_ptr<Event>, std::shared_ptr<Event>> writeEvents;
        scheduleWriteForEdge(this->processor, edgeToWriteJustInCase, writeEvents, true);
        events.insert(writeEvents.first);
        events.insert(writeEvents.second);
        assert(isLocatedOnThisProcessor(edgeToWriteJustInCase, this->processor->id, false));
        this->processor->writingQueue.erase(this->processor->writingQueue.begin());
        // for (const auto &item: this->processor->writingQueue){
        //    cout<<buildEdgeName(item)<<endl;
        // }
        // cout<<"----------"<<endl;
        // for (const auto &item: cluster->getProcessorById(this->processor->id)->writingQueue){
        //     cout<<buildEdgeName(item)<<endl;
        //  }
    }
}

void Event::removeFromPredecessors()
{
    this->cleanupSuccessors();
    for (const auto& predecessor : this->predecessors) {
        if (!predecessor) {
            continue;
        }
        for (auto predsucIt = predecessor->successors.begin();
            predsucIt != predecessor->successors.end();) {
            const auto predsuc = predsucIt->lock();
            if (!predsuc || predsuc->id == this->id) {
                predsucIt = predecessor->successors.erase(predsucIt);
            } else {
                ++predsucIt;
            }
        }
    }
    this->predecessors.clear();
}

void Event::removeFromSuccessors()
{
    this->cleanupSuccessors();
    for (auto succIt = this->successors.begin(); succIt != this->successors.end();) {
        const auto successor = succIt->lock();
        if (!successor) {
            throw std::runtime_error("Invalid (nullptr) successor to " + this->id);
        }

        for (auto succspredIt = successor->predecessors.begin();
            succspredIt != successor->predecessors.end();) {
            const auto succspred = *succspredIt;
            if (succspred->id == this->id) {
                succspredIt = successor->predecessors.erase(succspredIt);
            } else {
                ++succspredIt;
            }
        }
        ++succIt;
    }
    this->successors.clear();
}

void Event::removeFromDependencies()
{
    this->cleanupSuccessors();
    removeFromPredecessors();
    removeFromSuccessors();
}

void Cluster::printProcessorsEvents()
{

    /* for (const auto &[key, value]: this->processors) {
         if (!value->getEvents().empty()) {
             cout << "Processor " << value->id << "with memory " << value->getMemorySize() << ", speed "
                  << value->getProcessorSpeed() << endl;
             cout << "Events: " << endl;
             if (value->getLastComputeEvent().lock())
                 cout << "\t" << value->getLastComputeEvent().lock()->id << " ";
             if (value->getLastReadEvent().lock())
                 cout << value->getLastReadEvent().lock()->id << " ";
             if (value->getLastWriteEvent().lock())
                 cout << value->getLastWriteEvent().lock()->id;
             cout << endl;
             for (auto &item: value->getEvents()) {
                 auto eventPrt = item.second.lock();
                 if (eventPrt) {
                     cout << "\t" << eventPrt->id << " " << eventPrt->getExpectedTimeFire() << " "
                          << eventPrt->getActualTimeFire()
                          << endl;
                 }
             }

             cout << "\t"
                  << " ready time compute " << value->getReadyTimeCompute()
                  << " ready time read " << value->getReadyTimeRead()
                  << " ready time write " << value->getReadyTimeWrite()
                  //<< " ready time write soft " << value->softReadyTimeWrite
                  //<< " avail memory " << value->availableMemory
                  << " pending in memory " << value->getPendingMemories().size() << " pcs: ";

             for (const auto &item: value->getPendingMemories()) {
                 print_edge(item);
             }
             cout << endl;
             cout << "after pending in memory " << value->getAfterPendingMemories().size() << " pcs: ";
             for (const auto &item: value->getAfterPendingMemories()) {
                 print_edge(item);
             }
             cout << endl;
         }

     } */
}


// bool dealWithPredecessors(const std::shared_ptr<Event>& us)
// {
//     if (!us->getPredecessors().empty()) {
//
//         auto it = us->getPredecessors().begin();
//         while (it != us->getPredecessors().end()) {
//             if ((*it)->isDone) {
//                 it = us->getPredecessors().erase(it);
//             } else {
//                 it++;
//             }
//         }
//
//         // cout << "predecessors not empty for " << us->id << endl;
//         for (const auto& item : us->getPredecessors()) {
//             //      cout << "predecessor " << item->id << ", ";
//             if (item->getActualTimeFire() > us->getActualTimeFire()) {
//                 //  cout<<"predecessor "<<item->id<<"'s fire time is larger than ours. "<<item->getActualTimeFire()<<" vs "<<us->getActualTimeFire()<<endl;
//                 us->setActualTimeFire(
//                     item->getActualTimeFire());
//             }
//         }
//     }
//     return us->getPredecessors().empty();
// }

bool dealWithPredecessors(const std::shared_ptr<Event>& us)
{
    auto& preds = us->getPredecessors();

    // Remove completed predecessors
    for (auto it = preds.begin(); it != preds.end();) {
        const auto& pred = *it;
        if (!pred) {
            throw std::runtime_error("Invalid (nullptr) predecessor to " + us->id);
        }
        if (pred->isDone) {
            // cout << "removing done predecessor " << pred->id << endl;
            it = preds.erase(it);
        } else {
            ++it;
        }
    }

    // Adjust actual fire time based on remaining predecessors
    for (const auto& pred : preds) {
        if (pred->getActualTimeFire() > us->getActualTimeFire()) {
            bool success = events.reschedule(us->id, pred->getActualTimeFire());
            assert(success);
        }
    }

    return preds.empty();
}

void transferAfterMemoriesToBefore(const std::shared_ptr<Processor>& ourModifiedProc)
{
    ourModifiedProc->availableMemoryDuringPreviousTask = ourModifiedProc->getAvailableMemory();
    ourModifiedProc->resetPendingMemories();
    ourModifiedProc->setAvailableMemory(ourModifiedProc->getMemorySize());
    for (auto& item : ourModifiedProc->getAfterPendingMemories()) {
        ourModifiedProc->addPendingMemory(item);
    }
    ourModifiedProc->setAvailableMemory(ourModifiedProc->getAfterAvailableMemory());
    ourModifiedProc->resetAfterPendingMemories();
    ourModifiedProc->setAfterAvailableMemory(ourModifiedProc->getMemorySize());
}

double applyDeviationTo(double& in)
{
    static std::random_device rd;
    static std::mt19937 gen(rd()); // Mersenne Twister PRNG

    double stddev;
    switch (devationVariant) {
    case 1:
        stddev = in * 0.1;
        break;
    case 2:
        stddev = in * 0.5;
        break;
    case 3:
    case 4:
        stddev = 0;
        break;
    case 5:
        stddev = in * 0.3;
        break;
    default:
        throw std::runtime_error("unknown deviation variant");
    }

    std::normal_distribution<double> dist(in, stddev);
    double result = dist(gen);
    result = (devationVariant != 3 && devationVariant != 4) ? std::max(result, 1.0) : result;
    if (devationVariant == 4) {
        result *= 2;
    }
    const double factor = result / in;
    in = result;
    if (devationVariant == 3)
        assert(factor == 1);
    return factor;
}

void Processor::setLastWriteEvent(const std::shared_ptr<Event>& lwe)
{
    this->lastWriteEvent = lwe;
    this->readyTimeWrite = lwe->getActualTimeFire();
}

void Processor::setLastReadEvent(const std::shared_ptr<Event>& lre)
{
    this->lastReadEvent = lre;
    this->readyTimeRead = lre->getActualTimeFire();
}

void Processor::setLastComputeEvent(const std::shared_ptr<Event>& lce)
{
    this->lastComputeEvent = lce;
    this->readyTimeCompute = lce->getActualTimeFire();
}

double Processor::getReadyTimeCompute()
{
    if (!this->lastComputeEvent.expired() && this->lastComputeEvent.lock()->getActualTimeFire() != this->readyTimeCompute) {
        this->readyTimeCompute = lastComputeEvent.lock()->getActualTimeFire();
    }
    return this->readyTimeCompute;
}

double Processor::getReadyTimeWrite()
{
    if (!this->lastWriteEvent.expired() && this->lastWriteEvent.lock()->getActualTimeFire() != this->readyTimeWrite) {
        this->readyTimeWrite = lastWriteEvent.lock()->getActualTimeFire();
    }
    return this->readyTimeWrite;
}

double Processor::getReadyTimeRead()
{
    if (!this->lastReadEvent.expired() && this->lastReadEvent.lock()->getActualTimeFire() != this->readyTimeRead) {
        this->readyTimeRead = lastReadEvent.lock()->getActualTimeFire();
    }
    return this->readyTimeRead;
}

double Processor::getExpectedOrActualReadyTimeCompute() const
{
    if (const auto lastEvent = this->lastComputeEvent.lock()) {
        return lastEvent->isDone ? lastEvent->getActualTimeFire() : lastEvent->getExpectedTimeFire();
    }
    return this->readyTimeCompute;
}

double Processor::getExpectedOrActualReadyTimeWrite() const
{
    if (const auto lastEvent = this->lastWriteEvent.lock()) {
        return lastEvent->isDone ? lastEvent->getActualTimeFire() : lastEvent->getExpectedTimeFire();
    }
    return this->readyTimeWrite;
}

double Processor::getExpectedOrActualReadyTimeRead() const
{
    if (const auto& lastEvent = this->lastReadEvent.lock()) {
        return lastEvent->isDone ? lastEvent->getActualTimeFire() : lastEvent->getExpectedTimeFire();
    }
    return this->readyTimeRead;
}