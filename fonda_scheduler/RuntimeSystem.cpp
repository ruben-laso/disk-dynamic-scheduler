#include <queue>
#include <random>

#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
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
bool isHeft= false;

double correctOflineMedihWithEvents(graph_t* graph, Cluster* cluster1, const int algoNum, const int deviationNumber, double& runtime)
{
    double resMakespan = -1;
    devationVariant = deviationNumber;
    cluster = cluster1;
    isHeft = (algoNum == fonda_scheduler::ALGORITHMS::HEFT);

    const auto start = std::chrono::system_clock::now();
    std::vector<std::shared_ptr<Event>> newEvents = medih2(graph, algoNum, runtime);
    events.verifySchedule(graph->vertices_by_id);

    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> elapsed_seconds = end - start;
    runtimeOfScheduler += elapsed_seconds.count();

    int cntr = 0;
    int numEvents= events.size();
    while (!events.empty()) {

        cntr++;
        events.assertQueueSorted("Before Everything");

        auto e = events.earliestReady(); // earliest by time
        if (!e) {
            throw std::runtime_error("Deadlock: no ready events");
        }

        // STEP 1: resolve readiness
        if (!e->cleanupPredecessors()) {
            double newTime = e->earliestAllowedTimeNoPlanning();
            if (newTime > e->getActualTimeFire()) {
                events.reschedule(e->id, newTime);
            }
            continue;
        }

        double fireTime = std::max(timeInSystem, e->getActualTimeFire());
        timeInSystem = fireTime;

        if (e->isDone) {
            throw std::runtime_error("Event " + e->id + " is already done, but it is in the queue.");
        }

        // STEP 3: fire
        // std::cout<<"about to fire event "<<e->id<<" at " <<e->getActualTimeFire()<<" TIMES FIRED "<<e->timesFired<<std::endl;
        e->cleanupSuccessors();
        e->fire(deviationNumber);

        const bool removed = events.remove(e->id);
        assert(removed);

        resMakespan = std::max(resMakespan, e->getActualTimeFire());
        lastEventName = e->id;
    }
    assert(numEvents == cntr);
    runtime = runtimeOfScheduler;
    averageSpreadPredecessors /= numTasksComputedPredecessors;

    return resMakespan;
}

double onlineMedih(graph_t* graph, Cluster* cluster1, const int algoNum, const int deviationNumber, const bool upw, double& runtime)
{
    double resMakespan = -1;
    cluster = cluster1;
    enforce_single_source_and_target_with_minimal_weights(graph);
    compute_bottom_and_top_levels(graph);
    devationVariant = deviationNumber;
    usePreemptiveWrites = upw;
    timeInSystem = 0;

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
        events.assertQueueSorted("Before Everything");

        auto e = events.earliestReady(); // earliest by time
        if (!e) {
            throw std::runtime_error("Deadlock: no ready events");
        }

        // STEP 1: resolve readiness
        if (!e->cleanupPredecessors()) {
            double newTime = e->earliestAllowedTimeNoPlanning();
            if (newTime > e->getActualTimeFire()) {
                events.reschedule(e->id, newTime);
            }
            continue;
        }

        double fireTime = std::max(timeInSystem, e->getActualTimeFire());
        timeInSystem = fireTime;

        if (e->isDone) {
            throw std::runtime_error("Event " + e->id + " is already done, but it is in the queue.");
        }

        // STEP 3: fire
        // std::cout<<"about to fire event "<<e->id<<" at " <<e->getActualTimeFire()<<" TIMES FIRED "<<e->timesFired<<std::endl;
        e->cleanupSuccessors();
        e->fire(deviationNumber);

        const bool removed = events.remove(e->id);
        assert(removed);

        resMakespan = std::max(resMakespan, e->getActualTimeFire());
        lastEventName = e->id;
    }
    runtime = runtimeOfScheduler;
    averageSpreadPredecessors /= numTasksComputedPredecessors;

    return resMakespan;
}

void Event::fireTaskStart()
{

    this->task->status = Status::Running;

    auto ourFinishEvent = events.find(this->task->name + "-f");
    if (!ourFinishEvent) {
        throw std::runtime_error("No finish event found for " + this->task->name);
    }

    // Compute actual duration
    double durationTask = ourFinishEvent->getExpectedTimeFire() - this->getExpectedTimeFire();
    assert(durationTask > 0);
    assert(this->task->name == "GRAPH_SOURCE" || durationTask >= this->task->time / this->processor->getProcessorSpeed()
        || std::abs(durationTask - this->task->time / this->processor->getProcessorSpeed()) < 1);

    const double factor = getOrApplyDeviationFactor(this->task->factorForRealExecution, durationTask);
    assert(factor > 0);
    this->task->factorForRealExecution = factor;


    // Clean expired successors
    cleanupSuccessors();

    // Compute new finish time and update the event manager
    double newFinishTime = this->getActualTimeFire() + durationTask;
    events.reschedule(ourFinishEvent->id, newFinishTime);

    for (auto inEdge : this->task->in_edges) {
        std::string startName = buildEdgeName(inEdge) + "-w-s";
        std::string finishName = buildEdgeName(inEdge) + "-w-f";

        auto startWrite = events.find(startName);
        auto finishWrite = events.find(finishName);

        if (startWrite) {
            std::cout << " remove start and end write " << startWrite->id << " " << finishWrite->id << std::endl;

            // reschedule to potentially pull their successors to earlier if this is beneficial
            bool success = events.reschedule(startWrite->id, this->getActualTimeFire());
            assert(success);
            success = events.reschedule(finishWrite->id, this->getActualTimeFire());
            assert(success);

            startWrite->removeFromDependencies();
            finishWrite->removeFromDependencies();

            startWrite->isDone = true;
            finishWrite->isDone = true;

            events.remove(startName);
            events.remove(finishName);

            assert(this->getActualTimeFire() <= startWrite->getActualTimeFire());

        } else if (finishWrite) {
            // If only finish write exists, just reschedule
            bool success = events.reschedule(finishWrite->id, this->getActualTimeFire());
            assert(success);
            events.remove(finishName);
            finishWrite->removeFromDependencies();
            finishWrite->isDone = true;
        }

        // Clean up writing queues on all processors
        for (auto& [proc_id, processor] : cluster->getProcessors()) {
            if (!processor->writingQueue.empty()) {
                processor->writingQueue.erase(
                    std::remove(processor->writingQueue.begin(), processor->writingQueue.end(), inEdge),
                    processor->writingQueue.end());
            }
        }
    }
}



void Event::fireTaskFinish()
{
    const vertex_t* thisTask = this->task;

    this->task->status = Finished;
    this->isDone = true;
    this->task->makespan = this->actualTimeFire;

    removeFromDependencies();

    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());
    assert(this->processor->getAvailableMemory() <= this->processor->getMemorySize());

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
               // std::cout<<"unscheduled parent "<<in_edge->tail->name<<"\n";
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

    scheduleTasksUntilFoundForThisProc();
}

void Event::fireReadStart()
{
    assert(isLocatedOnDisk(this->edge, false));

    double durationOfRead = this->edge->weight / this->processor->readSpeedDisk;
    const double factor = getOrApplyDeviationFactor(this->edge->factorForRealExecution, durationOfRead);
    assert(factor > 0);

    this->edge->factorForRealExecution = factor;
    const double expectedTimeFireFinish = this->actualTimeFire + durationOfRead;

    this->isDone = true;

    const std::shared_ptr<Event> finishRead = events.find(buildEdgeName(this->edge) + "-r-f");
    if (finishRead == nullptr) {
        throw std::runtime_error("NO read finish found for " + this->id);
    }
    events.reschedule(finishRead->id, expectedTimeFireFinish);
}

void Event::fireReadFinish()
{
    std::shared_ptr<Event> startRead = events.find(buildEdgeName(this->edge) + "-r-s");

    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());
    if (!isLocatedOnDisk(this->edge, false)) {
        const auto ptr = events.find(buildEdgeName(this->edge) + "-w-f");
        assert(ptr != nullptr);
        auto ptr1 = events.find(buildEdgeName(this->edge) + "-r-s");
        assert(ptr1->getActualTimeFire() < this->getActualTimeFire());
    }
    locateToThisProcessorFromDisk(this->edge, this->processor->id, false, this->getActualTimeFire());
    this->isDone = true;
    this->edge->accountedFor = true;
}

void Event::fireWriteStart()
{
    assert(isLocatedOnThisProcessor(this->edge, this->processor->id, false));

    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());

    double durationOfWrite = this->edge->weight / this->processor->writeSpeedDisk;
    const double factor = getOrApplyDeviationFactor(this->edge->factorForRealExecution, durationOfWrite);
    assert(factor > 0);
    this->edge->factorForRealExecution = factor;
    assert(factor > 0);

    const double actualTimeFireFinish = this->actualTimeFire + durationOfWrite;
    this->isDone = true;
    const std::shared_ptr<Event> finishWrite = events.find(buildEdgeName(this->edge) + "-w-f");

    if (finishWrite == nullptr) {
        throw std::runtime_error("NO write finish found for " + this->id);
    }

    events.reschedule(finishWrite->id, actualTimeFireFinish);
    assert(abs(finishWrite->getActualTimeFire() - actualTimeFireFinish)<0.001);

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
    if (this->onlyPreemptive) {
        locateToDisk(this->edge, false, this->getActualTimeFire());
        assert(isLocatedOnThisProcessor(this->edge, this->processor->id, false));
    } else {
        assert(this->edge->locations.size() >= 0);
        assert(this->edge->imaginedLocations.size() >= 0);
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
        // std:: cout << "event for " << buildEdgeName(edgeToWriteJustInCase) << " already in queue" << endl;
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

    if (this->getActualTimeFire() + presumedLength < startOfNextWrite && usePreemptiveWrites) {
        // can fit
        // std::cout << "scheduling extra write for " << buildEdgeName(edgeToWriteJustInCase) << std::endl;
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
    // do not throw in teardown paths
    std::vector<std::shared_ptr<Event>> preds = predecessors;

    predecessors.clear();

    for (auto& predecessor : preds) {
        if (!predecessor)
            continue;

        auto& succs = predecessor->successors;

        succs.erase(
            std::remove_if(
                succs.begin(),
                succs.end(),
                [&](const std::weak_ptr<Event>& w) {
                    auto sp = w.lock();
                    return !sp || sp.get() == this;
                }),
            succs.end());
    }
}

void Event::removeFromSuccessors()
{
    std::vector<std::weak_ptr<Event>> succs = successors;

    successors.clear();

    for (auto& w : succs) {
        auto successor = w.lock();
        if (!successor)
            continue;

        auto& preds = successor->predecessors;

        preds.erase(
            std::remove_if(
                preds.begin(),
                preds.end(),
                [&](const std::shared_ptr<Event>& p) {
                    return !p || p.get() == this;
                }),
            preds.end());
    }
}

void Event::removeFromDependencies()
{
    this->cleanupSuccessors();
    removeFromPredecessors();
    removeFromSuccessors();
}

bool Event::cleanupPredecessors()
{
    auto preds = this->getPredecessors();

    for (auto it = preds.begin(); it != preds.end();) {
        if ((*it)->isDone) {
            it = preds.erase(it);
        } else {
            ++it;
        }
    }

    return preds.empty();
}

void Event::scheduleTasksUntilFoundForThisProc()
{
    bool foundTaskForThisProc = false;

    while (!readyQueue.readyTasks.empty()) {
        vertex_t* v = *readyQueue.readyTasks.begin();

        std::vector<std::shared_ptr<Processor>> modified;
        std::shared_ptr<Processor> assigned;
        int bestVar;

        auto newEvents = bestTentativeAssignment(
            v,
            modified,
            assigned,
            this->getActualTimeFire(),
            bestVar);

        v->status = Status::Scheduled;
        readyQueue.readyTasks.erase(v);

        if (assigned->id == this->processor->id) {
            foundTaskForThisProc = true;
        }

        for (auto& e : newEvents) {
            events.insert(e); // SAFE: new events only
        }

        if (foundTaskForThisProc) {
            break;
        }

        bool idleExists = std::any_of(
            cluster->getProcessors().begin(),
            cluster->getProcessors().end(),
            [&](const auto& p) {
                return p.second->getReadyTimeCompute() <= this->getActualTimeFire();
            });

        if (!idleExists) {
            break;
        }
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

void transferAfterMemoriesToBefore(const std::shared_ptr<Processor>& ourModifiedProc, double notEarlierThan)
{
    ourModifiedProc->setAvailableMemoryDuringPreviousTask(ourModifiedProc->getAvailableMemory());
    double startOfLastTask = ourModifiedProc->getLastComputeEvent().expired() ?
    0:
    (ourModifiedProc->getLastComputeEvent().lock()->predecessors.size()>0?
       ourModifiedProc->getLastComputeEvent().lock() ->getCorrespondingStart()->getVisibleTimeFireForPlanning(): notEarlierThan);
    ourModifiedProc->setStartOfLastTask(startOfLastTask);
    ourModifiedProc->resetPendingMemories();
    ourModifiedProc->setAvailableMemory(ourModifiedProc->getMemorySize());
    for (auto& item : ourModifiedProc->getAfterPendingMemories()) {
        ourModifiedProc->addPendingMemory(item);
    }
    ourModifiedProc->setAvailableMemory(ourModifiedProc->getAfterAvailableMemory());
    ourModifiedProc->resetAfterPendingMemories();
    ourModifiedProc->setAfterAvailableMemory(ourModifiedProc->getMemorySize());
}

double getOrApplyDeviationFactor(double& factorForRealExecution, double & duration)
{
    if (factorForRealExecution == -1) {
        // Not yet applied, compute new factor
        factorForRealExecution = applyDeviationTo(duration);
    }else {
        // Already applied, just scale duration by the existing factor
        duration = duration * factorForRealExecution;
        duration = (devationVariant != 3 && devationVariant != 4) ? std::max(duration, 1.0) : duration;
    }
    return factorForRealExecution;
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
    this->setStartOfLastTask(lce->getCorrespondingStart()->getActualTimeFire());
}

double Processor::getReadyTimeCompute() const
{
    //  if (!this->lastComputeEvent.expired() && this->lastComputeEvent.lock()->getActualTimeFire() != this->readyTimeCompute) {
    //      this->readyTimeCompute = lastComputeEvent.lock()->getActualTimeFire();
    //  }
    return this->readyTimeCompute;
}

double Processor::getReadyTimeWrite() const
{
    //  if (!this->lastWriteEvent.expired() && this->lastWriteEvent.lock()->getActualTimeFire() != this->readyTimeWrite) {
    //     this->readyTimeWrite = lastWriteEvent.lock()->getActualTimeFire();
    //   }
    return this->readyTimeWrite;
}

double Processor::getReadyTimeRead()
{
    // if (!this->lastReadEvent.expired() && this->lastReadEvent.lock()->getActualTimeFire() != this->readyTimeRead) {
    //     this->readyTimeRead = lastReadEvent.lock()->getActualTimeFire();
    // }
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