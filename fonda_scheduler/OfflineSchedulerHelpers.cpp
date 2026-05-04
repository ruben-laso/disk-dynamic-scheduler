#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "fonda_scheduler/algorithms.hpp"
#include "fonda_scheduler/options.hpp"

#include <iterator>
#include <queue>

void emulateAllEvict2(SchedulingResult& result, const double timeToWriteAllPending, const std::vector<EdgeChange>& changedEdgesAll,
    const double startTimeForAllEvicted, const double readyTimeCompute, double readyTimeWrite)
{

    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    result.processorOfAssignment->setReadyTimeCompute(readyTimeCompute);
    result.finishTime = result.processorOfAssignment->getReadyTimeCompute();
    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    result.resultingVar = 3;
    // cout<<"best tentative with all Evicted ";
    result.processorOfAssignment->setReadyTimeWrite(
        result.processorOfAssignment->getReadyTimeWrite() + timeToWriteAllPending);
    assert(result.startTime <= startTimeForAllEvicted);
    result.startTime = startTimeForAllEvicted;
    result.edgesToChangeStatus = changedEdgesAll;
    result.processorOfAssignment->setReadyTimeWrite(readyTimeWrite);
}


void emulateBiggestEvict2( SchedulingResult& result, const std::vector<EdgeChange>& changedEdgesOne,
    const double startTimeForTask, edge_t* biggestPendingEdge, const double readyTimeCompute, double readyTimeWrite)
{

    assert(biggestPendingEdge != nullptr);
    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    result.processorOfAssignment->setReadyTimeCompute(readyTimeCompute);
    result.finishTime = result.processorOfAssignment->getReadyTimeCompute();
    assert(result.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());

    result.edgeToKick = biggestPendingEdge;
    // cout<<"best tentative with biggest Evicted "; print_edge(toKick);
    result.resultingVar = 2;

    result.processorOfAssignment->setReadyTimeWrite(readyTimeWrite);
    // ourModifiedProc->pendingMemories.erase()
    // penMemsAsVector.erase(penMemsAsVector.begin());
    result.edgesToChangeStatus = changedEdgesOne;
    assert(result.startTime <= startTimeForTask);
    result.startTime = startTimeForTask;
    assert(result.edgeToKick != nullptr);
    assert(!result.edgeToKick->imaginedLocations.empty());
    assert(isLocatedOnThisProcessor(result.edgeToKick, result.processorOfAssignment->id, true));
}


std::vector<std::pair<vertex_t*, double>> calculateBottomLevels(graph_t* graph, const int algoNum)
{
    std::vector<std::pair<vertex_t*, double>> ranks;
    switch (algoNum) {
    case fonda_scheduler::ALGORITHMS::HEFT:
    case fonda_scheduler::ALGORITHMS::HEFT_BL: {
        vertex_t* vertex = graph->first_vertex;
        while (vertex != nullptr) {
            double rank = calculateSimpleBottomUpRank(vertex);
            ranks.emplace_back(vertex, rank);
            vertex = vertex->next;
        }
        break;
    }
    case fonda_scheduler::ALGORITHMS::HEFT_BLC: {
        vertex_t* vertex = graph->first_vertex;
        while (vertex != nullptr) {
            double rank = calculateBLCBottomUpRank(vertex);
            ranks.emplace_back(vertex, rank);
            vertex = vertex->next;
        }
        break;
    }
    case fonda_scheduler::ALGORITHMS::HEFT_MM:
        ranks = calculateMMBottomUpRank(graph);
        break;
    case fonda_scheduler::ALGORITHMS::HEFT_L: {
        vertex_t* vertex = graph->first_vertex;
        while (vertex != nullptr) {
            double rank = calculateLRank(vertex);
            ranks.emplace_back(vertex, rank);
            vertex = vertex->next;
        }
        break;
    }
    default:
        throw std::runtime_error("unknown algorithm");
    }
    return ranks;
}


graph_t* convertToNonMemRepresentation(graph_t* withMemories, std::map<int, int>& noMemToWithMem)
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
        std::ignore = new_edge(noNodeMemories, invtx, outvtx, vertex->memoryRequirement, nullptr);
        noNodeMemories->target = outvtx;

        for (const auto inEdgeOriginal : vertex->in_edges) {
            const std::string expectedName = inEdgeOriginal->tail->name + "-out";
            vertex_t* outVtxOfCopiedInVtxOfEdge = findVertexByName(noNodeMemories, expectedName);

            if (outVtxOfCopiedInVtxOfEdge == nullptr) {
                print_graph_to_cout(noNodeMemories);
                std::cout << "expected: " << expectedName << '\n';
                throw std::invalid_argument(" no vertex found for expected name.");
            }
            std::ignore = new_edge(noNodeMemories, outVtxOfCopiedInVtxOfEdge, invtx, inEdgeOriginal->weight, nullptr);
        }
    }

    return noNodeMemories;
}

double calculateSimpleBottomUpRank(vertex_t* task)
{
    double maxCost = 0.0;
    for (const auto& out_edge : task->out_edges) {
        const double communicationCost = out_edge->weight;
        if (out_edge->head->bottom_level == -1) {
            out_edge->head->bottom_level = calculateSimpleBottomUpRank(out_edge->head);
        }
        const double successorCost = out_edge->head->bottom_level;
        double cost = communicationCost + successorCost;
        maxCost = std::max(maxCost, cost);
    }
    const double retur = (task->time + maxCost);
    task->bottom_level = retur;
    task->rank= retur;
    return retur;
}

double calculateBLCBottomUpRank(vertex_t* task)
{

    double maxCost = 0.0;
    for (const auto out_edge : task->out_edges) {
        const double communicationCost = out_edge->weight;
        const double successorCost = calculateBLCBottomUpRank(out_edge->head);
        double cost = communicationCost + successorCost;
        maxCost = std::max(maxCost, cost);
    }
    const double simpleBl = task->time + maxCost;

    double maxInputCost = 0.0;
    for (const auto in_edge : task->in_edges) {
        double communicationCost = in_edge->weight;
        maxInputCost = std::max(maxInputCost, communicationCost);
    }
    double retur = simpleBl + maxInputCost;
    task->rank = retur;
    return retur;
}

double calculateLRank(vertex_t* task)
{    double inputCost = 0.0;
    for (const auto in_edge : task->in_edges) {
        inputCost += in_edge->weight;
    }
    double retur = inputCost;
    task->rank = retur;
    return retur;
}


std::vector<std::pair<vertex_t*, double>> calculateMMBottomUpRank(graph_t* graphWMems)
{
    std::map<int, int> noMemToWithMem;
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

        for (int i = 0; i < sp_graph->vertices_by_id.size(); i++) {
            const vertex_t* vInSp = schedule[i];
            // cout<<vInSp->name<<endl;
            const std::map<int, int>::iterator& it = noMemToWithMem.find(vInSp->id);
            if (it != noMemToWithMem.end()) {
                vertex_t* vertexWithMem = graphWMems->vertices_by_id[it->second];
                if (std::find_if(scheduleOnOriginal.begin(), scheduleOnOriginal.end(),
                        [vertexWithMem](const std::pair<vertex_t*, int>& p) {
                            return p.first->name == vertexWithMem->name;
                        })
                    == scheduleOnOriginal.end()) {
                    scheduleOnOriginal.emplace_back(vertexWithMem,
                        sp_graph->vertices_by_id.size() - i); // TODO: #vertices - i?
                }
            }
        }

    } else {
        throw std::runtime_error("No tree decomposition");
    }
    delete sp_tree;
    delete sp_graph;
    // delete graph;

    std::vector<std::pair<vertex_t*, double>> double_vector;
    double_vector.reserve(scheduleOnOriginal.size());

    // Convert each pair from (vertex_t*, int) to (vertex_t*, double)
    for (const auto& [vertex, rank] : scheduleOnOriginal) {
        double_vector.emplace_back(vertex, static_cast<double>(rank));
        vertex->rank = static_cast<double>(rank);
    }

    return double_vector;
}

void realSurplusOfOutgoingEdges(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc, double& sumOut)
{
    for (auto inEdge : v->in_edges) {
        auto pendingOfProc = ourModifiedProc->getPendingMemories();
        if (pendingOfProc.find(inEdge) != pendingOfProc.end()) {
            sumOut -= inEdge->weight;
        } else {
            // cout<<"edge "<<buildEdgeName(inEdge)<<" not anymore found in pending mems of processor "<<ourModifiedProc->id<<endl;
        }
    }
    //  cout << "REQUIRES AT THE END: " << sumOut << endl;
}

double finishTimeWithMemorySwapping(double startTime, double amountToOffload, double timeToRun, const vertex_t* task,
    const std::shared_ptr<Processor>& p, const fonda::Options& options)
{
    double pSize=options.pageSize;
    double result = startTime + timeToRun / p->getProcessorSpeed();

    int mswapPsize = ceil((std::abs(amountToOffload)/pSize));
    int numpages = ceil(((1 + task->swapRate)* mswapPsize)) ;

     double penaltyToSwap =  //(std::abs(amountToOffload) / task->memoryRequirement) *
         numpages * (pSize/p->readSpeedDisk)
         * options.penaltyCoefficient;
     result += penaltyToSwap;

     if(result<startTime){
         std::cout<<"bad computed result with memory swapping on vertex "<<task->name<<std::endl;
     }

    // std::cout<<"compute duration from start time  "<<startTime<<" w amountToOffload "<<amountToOffload<< "is "<< result-startTime<< "w penalty "<< penaltyToSwap <<std::endl;
     return result;
}

double howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(const vertex_t* v, const std::shared_ptr<Processor>& pj)
{
    assert(!pj->getIsKeptValid() || pj->getAvailableMemory() >= 0);

    double sumPend=0;
    for (const auto& item : pj->getPendingMemories()){
        sumPend+=item->weight;
    }

    // assert(std::abs(sumPend+pj->getAvailableMemory()-pj->getMemorySize())<1);

    double Res = pj->getAvailableMemory() - peakMemoryRequirementOfVertex(v);
    for (auto inEdge : v->in_edges) {
        if (pj->getPendingMemories().find(inEdge) != pj->getPendingMemories().end()) {
            // incoming edge occupied memory
            Res += inEdge->weight;
        }
    }
    return Res;
}

[[maybe_unused]] void checkIfPendingMemoryCorrect(const std::shared_ptr<Processor>& p)
{
    double allPending = 0;
    for (const auto pendingMemorie : p->getPendingMemories()) {
        allPending += pendingMemorie->weight;
    }
    const double accountedFor = p->getAvailableMemory() + allPending;
    assert(std::abs(accountedFor-p->getMemorySize())<1);

    //assert(std::abs(p->getMemorySize() - busy) < 1);
    assert(p->getReadyTimeCompute() < std::numeric_limits<double>::max());
}

double uniquePredecessorProcs(vertex_t* vertex)
{
    std::set<int> uniqueProcessorIds;
    for (const auto& in_edge : vertex->in_edges) {
        int processorId = in_edge->tail->assignedProcessorId;
        assert(processorId!=-1);
        uniqueProcessorIds.insert(processorId);
    }
    return uniqueProcessorIds.size();
}

void applySchedulingResultToImaginedCluster(vertex_t* vertex, SchedulingResult& schedulingResult, Cluster* ourCluster, int& numberWithEvictedCases,  bool isHeft)
{
    constexpr bool useImagined = true;
    auto proc = schedulingResult.processorOfAssignment;

    ///////////////////////sanity checks
    if(!isHeft)
        checkIfPendingMemoryCorrect(proc);
    assert(schedulingResult.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    switch (schedulingResult.resultingVar) {
    case 0:
    case 1:
        assert(schedulingResult.edgesToChangeStatus.size()==0);
        break;
    case 3:
        assert(schedulingResult.edgesToChangeStatus.size()>=1);
        break;
    case 2:
        assert(schedulingResult.edgesToChangeStatus.size()==1);
        break;
    default:
        throw std::runtime_error("unknown resultingVar of "+std::to_string(schedulingResult.resultingVar));
    };
    ///////////////////////

    proc->setAvailableMemoryDuringPreviousTask( schedulingResult.shouldBeFreeOnProcessorDuringTask, isHeft&&!proc->getIsKeptValid());
    proc->setStartOfLastTask( schedulingResult.startTime, isHeft&&!proc->getIsKeptValid());

    //Apply processor mutations
    for (auto& modifiedProc : schedulingResult.modifiedProcs) {
        if(!isHeft)
            checkIfPendingMemoryCorrect(proc);
        ourCluster->getProcessorById(modifiedProc->id)
            ->updateFrom(*modifiedProc);
    }

    for (auto e : schedulingResult.edgesToChangeStatus) {
        if (isLocatedOnThisProcessor(e.edge, proc->id, useImagined)) {
            delocateFromThisProcessorToDisk(e.edge, proc->id, useImagined, e.newLocation.afterWhen.value());
        }
        if (proc->getPendingMemories().find(e.edge) != proc->getPendingMemories().end()) {
            proc->removePendingMemory(e.edge);
        }
    }
    if (schedulingResult.resultingVar>0) {
        numberWithEvictedCases++;
    }

    vertex->assignedProcessorId = proc->id;

    for (const auto ine : vertex->in_edges) {

        const int onWhichProcessor = whatProcessorIsLocatedOn(ine, useImagined);
        assert(onWhichProcessor == -1 || onWhichProcessor == schedulingResult.processorOfAssignment->id ||
            ourCluster->getProcessorById(onWhichProcessor)->getPendingMemories().find(ine) == ourCluster->getProcessorById(onWhichProcessor)->getPendingMemories().end());

        if (onWhichProcessor == proc->id) {
            proc->delocateToNowhereOptionally(ine, useImagined);
        } else if (onWhichProcessor != -1) {
            ourCluster->getProcessorById(onWhichProcessor)
                ->delocateToNowhereOptionally(ine, useImagined);
        } else {
            if (auto holder = findProcessorThatHoldsEdge(ine, ourCluster)) {
                holder->delocateToNowhereOptionally(ine, useImagined);
            }
        }
        ine->imaginedLocations.clear();
    }

    for (const auto out : vertex->out_edges) {
        proc->loadFromNowhere(out, useImagined, schedulingResult.finishTime);
        checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);
        if (schedulingResult.processorOfAssignment->getAvailableMemory() < 0) {
            //std::cout << "NO AVAILABLE MEMORY\n";
        }
    }

    ourCluster->getProcessorById(proc->id)->updateFrom(*proc);
    if(!isHeft)
        for (const auto& [_, p] : ourCluster->getProcessors()) {
            checkIfPendingMemoryCorrect(p);
        }
}



void evictAccordingToBestDecision(int& numberWithEvictedCases, SchedulingResult& bestSchedulingResult, const vertex_t* pVertex, bool isHeft)
{
    edge_t* edgeToKick = bestSchedulingResult.edgeToKick;
    switch (bestSchedulingResult.resultingVar) {
    case 0:
        break;
    case 1:
        break;
    case 2: {
       // std::cout<<"best with 1 kick "<<buildEdgeName(bestSchedulingResult.edgesToChangeStatus.at(0).edge)<<std::endl;
        assert(edgeToKick != nullptr);
        assert(bestSchedulingResult.edgesToChangeStatus.size() == 1);

        const auto findEdgeInChanges = std::find_if(
            bestSchedulingResult.edgesToChangeStatus.begin(),
            bestSchedulingResult.edgesToChangeStatus.end(), [edgeToKick](const EdgeChange& e) {
                return edgeToKick == e.edge;
            });
        assert(findEdgeInChanges != bestSchedulingResult.edgesToChangeStatus.end());

        bestSchedulingResult.processorOfAssignment->delocateToDisk(edgeToKick,true, findEdgeInChanges->newLocation.afterWhen.value());
        numberWithEvictedCases++;
        checkIfPendingMemoryCorrect(bestSchedulingResult.processorOfAssignment);
        break;
    }
    case 3: {
     //   std::cout<<"best with all kick"<<std::endl;
        assert(bestSchedulingResult.edgesToChangeStatus.size() > 1);

        for (auto it = bestSchedulingResult.processorOfAssignment->getPendingMemories().begin();
             it != bestSchedulingResult.processorOfAssignment->getPendingMemories().end();) {

             edge_t* nextEdge = *it;
           //  std:: cout << buildEdgeName(nextEdge) << std::endl;
             if ( nextEdge->head->name != pVertex->name) {

                 const auto findEdgeInChanges1 = std::find_if(
                     bestSchedulingResult.edgesToChangeStatus.begin(),
                     bestSchedulingResult.edgesToChangeStatus.end(), [nextEdge](const EdgeChange& e) {
                         return nextEdge == e.edge;
                     });

                 if (findEdgeInChanges1 != bestSchedulingResult.edgesToChangeStatus.end()) {

                     it = isHeft ? bestSchedulingResult.processorOfAssignment->delocateToDiskOptionally(nextEdge,
                                                    true,
                                                    findEdgeInChanges1->newLocation.afterWhen.value()):
                     bestSchedulingResult.processorOfAssignment->delocateToDisk(nextEdge,
                                                    true,
                                                    findEdgeInChanges1->newLocation.afterWhen.value());
                     assert(isLocatedOnDisk(nextEdge, true));
                 }
                 else {
                     ++it;
                 }

             } else {
                 ++it;
             }
         }
       numberWithEvictedCases++;
        checkIfPendingMemoryCorrect(bestSchedulingResult.processorOfAssignment);
        break;
    }
    default:
        throw std::runtime_error("");
    }
}

std::shared_ptr<Processor> findProcessorThatHoldsEdge(edge_t* incomingEdge, Cluster* clusterToLookIn)
{
    for (auto& [proc_id, processor] : clusterToLookIn->getProcessors()) {
        auto iterator = std::find_if(processor->getPendingMemories().begin(), processor->getPendingMemories().end(),
            [incomingEdge](const edge_t* edge) {
                return incomingEdge == edge;
            });
        if (iterator != processor->getPendingMemories().end()) {
            return processor;
        }
    }
    return nullptr;
}

std::shared_ptr<Event> findTaskStart(const std::vector<std::shared_ptr<Event>>& someEvents)
{
    for (auto some_event : someEvents) {
        if (some_event->type==OnTaskStart)
            return some_event;
    }
    return nullptr;
}

std::shared_ptr<Event> findLatest(const std::vector<std::shared_ptr<Event>>& someEvents)
{
    double latestTime = -1;
    std::shared_ptr<Event> latestEvent = nullptr;
    for (auto some_event : someEvents) {
       if (some_event->getActualTimeFire()>latestTime){
           latestTime= some_event->getActualTimeFire();
           latestEvent= some_event;
       }
    }
    return latestEvent;
}