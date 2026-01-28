#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "fonda_scheduler/algorithms.hpp"
#include "fonda_scheduler/options.hpp"

#include <iterator>
#include <queue>

void handleAllEvict2(SchedulingResult& result, const double timeToWriteAllPending, const std::vector<EdgeChange>& changedEdgesAll,
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


void handleBiggestEvict2( SchedulingResult& result, const std::vector<EdgeChange>& changedEdgesOne,
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
    //    cout<<"rank for "<<task->name<<" ";

    double maxCost = 0.0;
    for (const auto& out_edge : task->out_edges) {
        const double communicationCost = out_edge->weight;
        // cout<<communicationCost<<" ";
        if (out_edge->head->bottom_level == -1) {
            // cout<<"-1"<<endl;
            out_edge->head->bottom_level = calculateSimpleBottomUpRank(out_edge->head);
            // cout<<"then "<<task->out_edges[j]->head->bottom_level<<endl;
        }
        const double successorCost = out_edge->head->bottom_level; // calculateSimpleBottomUpRank(task->out_edges[j]->head);
        double cost = communicationCost + successorCost;
        maxCost = std::max(maxCost, cost);
    }
    // cout<<endl;
    const double retur = (task->time + maxCost);
    task->bottom_level = retur;
    task->rank= retur;
    // cout<<"result "<<retur<<endl;
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

double finishTimeWithMemorySwapping(double startTime, double amountToOffload, double timeToRun, const vertex_t* task, const std::shared_ptr<Processor>& p){
         //std::cout<<"swap rate "<<task->swapRate<<std::endl;
         double result = startTime + timeToRun / p->getProcessorSpeed();

         double penaltyToSwap = (1 + task->swapRate) * (std::abs(amountToOffload) / task->memoryRequirement) *
             (std::abs(amountToOffload) / p->writeSpeedDisk);

         //double penaltyToSwap = (std::abs(amountToOffload)) *1000;
         result += penaltyToSwap;

         if(result<startTime){
             std::cout<<"bad computed result with memory swapping on vertex "<<task->name<<std::endl;
         }
        // double timeToWriteEdgeOfThisSize= std::abs(amountToOffload)/p->writeSpeedDisk;
        // if (timeToWriteEdgeOfThisSize<penaltyToSwap) {
         //    std::cout<<"cheaper than writing\n";
        // }
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

[[maybe_unused]] inline void checkIfPendingMemoryCorrect(const std::shared_ptr<Processor>& p)
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

void putChangeOnCluster(vertex_t* vertex, SchedulingResult& schedulingResult, Cluster* cluster, int& numberWithEvictedCases,
     const bool isHeft)
{
    checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);

    schedulingResult.processorOfAssignment->setAvailableMemoryDuringPreviousTask( schedulingResult.shouldBeFreeOnProcessorDuringTask);

    evictAccordingToBestDecision(numberWithEvictedCases, schedulingResult, vertex, isHeft);

    for (auto& modifiedProc : schedulingResult.modifiedProcs) {
        checkIfPendingMemoryCorrect(modifiedProc);
        const auto procInClusterWithId = cluster->getProcessorById(modifiedProc->id);
        procInClusterWithId->updateFrom(*modifiedProc);
    }

    for (auto e : schedulingResult.edgesToChangeStatus) {
        //  cout<<"change status "<<buildEdgeName(e.edge)<<endl;

        if (isLocatedOnThisProcessor(e.edge, schedulingResult.processorOfAssignment->id, true)) {
            delocateFromThisProcessorToDisk(e.edge, schedulingResult.processorOfAssignment->id, true,
                e.newLocation.afterWhen.value());
        }
    }

    assert(schedulingResult.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    vertex->assignedProcessorId = schedulingResult.processorOfAssignment->id;

    for (const auto ine : vertex->in_edges) {
        const int onWhichProcessor = whatProcessorIsLocatedOn(ine, true);
        assert(onWhichProcessor == -1 || onWhichProcessor == schedulingResult.processorOfAssignment->id || cluster->getProcessorById(onWhichProcessor)->getPendingMemories().find(ine) == cluster->getProcessorById(onWhichProcessor)->getPendingMemories().end());

        if (onWhichProcessor == schedulingResult.processorOfAssignment->id) {
            // optionally, because edge could have been force removed during calculation of caorrect result in HEFT
            schedulingResult.processorOfAssignment->delocateToNowhereOptionally(ine, true);
        } else {
            if (onWhichProcessor != -1) {
                cluster->getProcessorById(onWhichProcessor)->delocateToNowhereOptionally(ine, true);
            } else {
                // edge has been read
                // cout<<"bla"<<endl;
                // cout << "NOWHERE! " << buildEdgeName(ine) << endl;
                if (const auto proc = findProcessorThatHoldsEdge(ine, cluster); proc != nullptr) {
                    if (proc->id == schedulingResult.processorOfAssignment->id)
                        schedulingResult.processorOfAssignment->delocateToNowhereOptionally(ine, true);
                    else
                        proc->delocateToNowhereOptionally(ine, true);
                }

                // assert(proc == nullptr);
            }
        }
        if (true)
            ine->imaginedLocations.clear();
        else
            ine->locations.clear();
    }

    checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);

    for (const auto v1 : vertex->out_edges) {
        schedulingResult.processorOfAssignment->loadFromNowhere(v1, true, schedulingResult.finishTime);
        checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);
        if (schedulingResult.processorOfAssignment->getAvailableMemory() < 0) {
            std::cout << "";
        }
    }
    cluster->getProcessorById(schedulingResult.processorOfAssignment->id)->updateFrom(*schedulingResult.processorOfAssignment);
    for (const auto& [proc_id, processor] : cluster->getProcessors()) {
        checkIfPendingMemoryCorrect(processor);
    }
}

void evictAccordingToBestDecision(int& numberWithEvictedCases, SchedulingResult& bestSchedulingResult, const vertex_t* pVertex, bool isHeft)
{

    const bool canAlreadyBeEvicted = !isHeft;
    edge_t* edgeToKick = bestSchedulingResult.edgeToKick;
    switch (bestSchedulingResult.resultingVar) {
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

        isHeft?
         bestSchedulingResult.processorOfAssignment->delocateToDiskOptionally(edgeToKick,
                                  true, findEdgeInChanges->newLocation.afterWhen.value()):
                           bestSchedulingResult.processorOfAssignment->delocateToDisk(
                                  edgeToKick,
                                  true, findEdgeInChanges->newLocation.afterWhen.value());
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


///////////////////////////////////////////////////FOR HEFT ONLY///////////////////////////////////////////////////
///
/// if (resultCorrect.processorOfAssignment->getAvailableMemory() < sumOut) {
        // only the correct result knows about kicking
        // std::shared_ptr<Event> lastEvictionEvent = nullptr;
        // double stillNeedsToBeEvictedToRun = sumOut - resultCorrect.processorOfAssignment->getAvailableMemory();
        // double writeTime = resultCorrect.startTime;
        //
        // for (auto it = resultCorrect.processorOfAssignment->getPendingMemories().begin();
        //     it != resultCorrect.processorOfAssignment->getPendingMemories().end() && stillNeedsToBeEvictedToRun > 0;) {
        //     //  print_edge(*it);
        //     if ((*it)->head->name != v->name) {
        //         stillNeedsToBeEvictedToRun -= (*it)->weight;
        //         double startWriteTime = std::max(writeTime,  (*it)->tail->makespanPerceived);
        //         auto location_on_processor = getLocationOnProcessor((*it), resultCorrect.processorOfAssignment->id, true);
        //         assert(location_on_processor!=nullptr);
        //         startWriteTime = std::max( startWriteTime, (*location_on_processor).afterWhen.value());
        //
        //         writeTime = startWriteTime + (*it)->weight / resultCorrect.processorOfAssignment->writeSpeedDisk;
        //         //   cout<<"tent on proc "<<resultCorrect.processorOfAssignment->id<<" ";
        //         resultCorrect.edgesToChangeStatus.emplace_back((*it), Location(LocationType::OnDisk, std::nullopt, writeTime));
        //         it = resultCorrect.processorOfAssignment->removePendingMemory(*it);
        //
        //         auto writeStart= Event::createEvent(nullptr, (*it), OnWriteStart, result.processorOfAssignment,startWriteTime, startWriteTime, false, buildEdgeName((*it)) + "-w-s");
        //         auto writeFinish= Event::createEvent(nullptr, (*it), OnWriteFinish, result.processorOfAssignment,writeTime, writeTime, false, buildEdgeName((*it)) + "-w-f");
        //         writeFinish->addPredecessorInPlanning(writeStart);
        //
        //         const std::shared_ptr<Event>& eventFinishPredecessorComputing = events.find((*it)->tail->name + "-f");
        //         if (eventFinishPredecessorComputing != nullptr) {
        //             const double prev = eventFinishPredecessorComputing->getActualTimeFire();
        //             writeStart->addPredecessorInPlanning(eventFinishPredecessorComputing);
        //             assert(prev == eventFinishPredecessorComputing->getActualTimeFire());
        //         } else {
        //             if ((*it)->tail->status == Status::Finished) {
        //                 std::cout << "no event finish predecessor - because tail is finished" << '\n';
        //             } else {
        //                 std::cout << "no event finish predecessor - AND THE TAIL IS NOT FINISHED" << '\n';
        //             }
        //         }
        //
        //         if (lastEvictionEvent) {
        //             writeStart->addPredecessorInPlanning(lastEvictionEvent);
        //         }
        //         lastEvictionEvent = writeFinish;
        //
        //
        //         createdEvents.emplace_back(writeStart);
        //         createdEvents.emplace_back(writeFinish);
        //
        //
        //     } else {
        //         ++it;
        //     }
        // }



/*
 *
 *
 *
*

 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *void processIncomingEdgesByNotGoingIntoPast(const vertex_t* v, const bool useDeviatedTimes,
    const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs,
    double& earliestStartingTimeToComputeVertex){
    //This is always baseline HEFT

    const bool shouldUseImaginary = !useDeviatedTimes;
    earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeCompute();
    for (const auto incomingEdge : v->in_edges) {
        const vertex_t* predecessor = incomingEdge->tail;

        const double edgeWeightToUse = useDeviatedTimes ? incomingEdge->weight * incomingEdge->factorForRealExecution
                                                          : incomingEdge->weight;

        if (predecessor->assignedProcessorId == ourModifiedProc->id) {
            if (!isLocatedOnThisProcessor(incomingEdge, ourModifiedProc->id, shouldUseImaginary)) {
                assert(isLocatedOnDisk(incomingEdge, shouldUseImaginary));
                double startOfRead = std::max(std::max(ourModifiedProc->getReadyTimeRead(), (*getLocationOnDisk(incomingEdge, shouldUseImaginary)).afterWhen.value()), earliestStartingTimeToComputeVertex);

                if (startOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startOfRead = ourModifiedProc->getReadyTimeCompute();
                }

                ourModifiedProc->setReadyTimeRead(
                    startOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk);
                earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeRead() > earliestStartingTimeToComputeVertex ? ourModifiedProc->getReadyTimeRead() : earliestStartingTimeToComputeVertex;
            }

        } else {
            if (isLocatedOnDisk(incomingEdge, shouldUseImaginary)) {
                // we need to schedule read
                double startOfRead = std::max(std::max(ourModifiedProc->getReadyTimeRead(), (*getLocationOnDisk(incomingEdge, shouldUseImaginary)).afterWhen.value()), earliestStartingTimeToComputeVertex);

                if (startOfRead < ourModifiedProc->getReadyTimeCompute() && ourModifiedProc->getAvailableMemoryDuringPreviousTask() < incomingEdge->weight) {
                    startOfRead = ourModifiedProc->getReadyTimeCompute();
                }
                ourModifiedProc->setReadyTimeRead(
                    startOfRead + edgeWeightToUse / ourModifiedProc->readSpeedDisk);
                earliestStartingTimeToComputeVertex = ourModifiedProc->getReadyTimeRead() > earliestStartingTimeToComputeVertex ? ourModifiedProc->getReadyTimeRead() : earliestStartingTimeToComputeVertex;

            } else {
                auto predecessorsProcessorsId = predecessor->assignedProcessorId;
                assert(isLocatedOnThisProcessor(incomingEdge, predecessorsProcessorsId, shouldUseImaginary));
                std::shared_ptr<Processor> addedProc;
                auto it = // modifiedProcs.size()==1?
                          //   modifiedProcs.begin():
                    std::find_if(modifiedProcs.begin(), modifiedProcs.end(),
                        [predecessorsProcessorsId](const std::shared_ptr<Processor>& p) {
                            return p->id == predecessorsProcessorsId;
                        });

                if (it == modifiedProcs.end()) {
                    Cluster* cluster = useDeviatedTimes ? actualCluster : imaginedCluster;
                    addedProc = std::make_shared<Processor>(*cluster->getProcessorById(predecessorsProcessorsId));
                    // cout<<"adding modified proc "<<addedProc->id<<endl;
                    modifiedProcs.emplace_back(addedProc);
                    checkIfPendingMemoryCorrect(addedProc);
                } else {
                    addedProc = *it;
                }

                assert(!hasDuplicates(modifiedProcs));

                double whichMakespan = useDeviatedTimes ? predecessor->makespan : predecessor->makespanPerceived;
                const double timeToStartWriting = std::max(std::max(whichMakespan, addedProc->getReadyTimeWrite()), earliestStartingTimeToComputeVertex);
                addedProc->setReadyTimeWrite(timeToStartWriting + edgeWeightToUse / addedProc->writeSpeedDisk);

                double startTimeOfRead = std::max(addedProc->getReadyTimeWrite(), ourModifiedProc->getReadyTimeRead());
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
            }
        }
    }
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
    //    cout<<"rank for "<<task->name<<" ";

    double maxCost = 0.0;
    for (const auto& out_edge : task->out_edges) {
        const double communicationCost = out_edge->weight;
        // cout<<communicationCost<<" ";
        if (out_edge->head->bottom_level == -1) {
            // cout<<"-1"<<endl;
            out_edge->head->bottom_level = calculateSimpleBottomUpRank(out_edge->head);
            // cout<<"then "<<task->out_edges[j]->head->bottom_level<<endl;
        }
        const double successorCost = out_edge->head->bottom_level; // calculateSimpleBottomUpRank(task->out_edges[j]->head);
        double cost = communicationCost + successorCost;
        maxCost = std::max(maxCost, cost);
    }
    // cout<<endl;
    const double retur = (task->time + maxCost);
    task->bottom_level = retur;
    task->rank= retur;
    // cout<<"result "<<retur<<endl;
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

std::vector<std::pair<vertex_t*, double>> buildRanksWalkOver(graph_t* graph)
{
    std::vector<std::pair<vertex_t*, double>> ranks;
    enforce_single_source_and_target(graph);
    int rank = 0;
    vertex_t* vertex = graph->first_vertex;
    while (vertex != nullptr) {
        if (vertex->in_edges.empty()) {
            ranks.emplace_back(vertex, rank);
        }
    }
    return ranks;
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
    default:
        throw std::runtime_error("unknown algorithm");
    }
    return ranks;
}

[[maybe_unused]] inline void checkIfPendingMemoryCorrect(const std::shared_ptr<Processor>& p)
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

[[maybe_unused]] inline bool hasDuplicates(const std::vector<std::shared_ptr<Processor>>& vec)
{
    //std::unordered_set<int> seenIds;
    //for (const auto& obj : vec) {
    //    if (!seenIds.insert(obj->id).second) {
            // Insert returns {iterator, false} if the value already exists
   //         return true;
    //    }
   // }
   // return false;
    return false;
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
double CVOfProcessorLoads( const std::map<int, double>& processorLoads)
{
    if (processorLoads.size() <= 1) {
        return 0.0;
    }

    std::vector<double> loads;
    for (const auto& pair : processorLoads) {
        loads.push_back(pair.second);
    }

    size_t N = loads.size();

    // Calculate the Mean (Average Load)
    double sum = std::accumulate(loads.begin(), loads.end(), 0.0);
    double mean = sum / N;

    // Avoid division by zero if all tasks somehow had 0 compute time (not typical)
    if (mean < 1e-9) {
        return 0.0;
    }

    // 3. CALCULATE VARIANCE AND STANDARD DEVIATION (σ)
    double variance = 0.0;
    for (double load : loads) {
        variance += std::pow(load - mean, 2);
    }
    // Population variance (divided by N) is appropriate for this descriptive statistic
    variance /= N;

    double stdDev = std::sqrt(variance);

    // 4. CALCULATE COEFFICIENT OF VARIATION (CV)
    double cv = stdDev / mean;

    return cv;

}

double idleTimePercentage( const std::map<int, std::vector<std::tuple<double, double>>> &processorsWorkTimes)
{

    double averageRatio=0;

    for (std::pair processor_work_times : processorsWorkTimes) {

        double allWorkTime=0;
        for (std::tuple interval : processor_work_times.second) {
            allWorkTime += std::get<1>(interval) - std::get<0>(interval);
        }

        double allTime = std::get<1>(processor_work_times.second.back());
        double allIdleTime = allTime - allWorkTime;
        averageRatio += allIdleTime/allWorkTime;
    }
    averageRatio/=processorsWorkTimes.size();
    return averageRatio;

}

double idleTimePercentageOn30s( const std::map<int, std::vector<std::tuple<double, double>>> &processorsWorkTimes)
{
    double averageRatio=0;

    for (std::pair processor_work_times : processorsWorkTimes) {
        if (processor_work_times.first>29) {
            double allWorkTime=0;
            for (std::tuple interval : processor_work_times.second) {
                allWorkTime += std::get<1>(interval) - std::get<0>(interval);
            }

            double allIdleTime = std::get<1>(processor_work_times.second.back()) - allWorkTime;
          //  assert(allIdleTime>0);
            averageRatio +=  allIdleTime/allWorkTime;
        }
    }
    averageRatio/=processorsWorkTimes.size();
    return averageRatio;

}


void putChangeOnCluster(vertex_t* vertex, SchedulingResult& schedulingResult, Cluster* cluster, int& numberWithEvictedCases,
    const bool real, const bool isHeft)
{
    checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);
    const bool shouldUseImaginary = !real;

    schedulingResult.processorOfAssignment->setAvailableMemoryDuringPreviousTask( schedulingResult.shouldBeFreeOnProcessorDuringTask);

    evictAccordingToBestDecision(numberWithEvictedCases, schedulingResult, vertex, isHeft, real);

    for (auto& modifiedProc : schedulingResult.modifiedProcs) {
        checkIfPendingMemoryCorrect(modifiedProc);
        const auto procInClusterWithId = cluster->getProcessorById(modifiedProc->id);
        procInClusterWithId->updateFrom(*modifiedProc);
    }

    for (auto e : schedulingResult.edgesToChangeStatus) {
        //  cout<<"change status "<<buildEdgeName(e.edge)<<endl;

        if (isLocatedOnThisProcessor(e.edge, schedulingResult.processorOfAssignment->id, shouldUseImaginary)) {
            delocateFromThisProcessorToDisk(e.edge, schedulingResult.processorOfAssignment->id, shouldUseImaginary,
                e.newLocation.afterWhen.value());
        }
    }

    assert(schedulingResult.processorOfAssignment->getReadyTimeCompute() < std::numeric_limits<double>::max());
    vertex->assignedProcessorId = schedulingResult.processorOfAssignment->id;

    for (const auto ine : vertex->in_edges) {
        const int onWhichProcessor = whatProcessorIsLocatedOn(ine, shouldUseImaginary);
        assert(onWhichProcessor == -1 || onWhichProcessor == schedulingResult.processorOfAssignment->id || cluster->getProcessorById(onWhichProcessor)->getPendingMemories().find(ine) == cluster->getProcessorById(onWhichProcessor)->getPendingMemories().end());

        if (onWhichProcessor == schedulingResult.processorOfAssignment->id) {
            // optionally, because edge could have been force removed during calculation of caorrect result in HEFT
            schedulingResult.processorOfAssignment->delocateToNowhereOptionally(ine, shouldUseImaginary);
        } else {
            if (onWhichProcessor != -1) {
                cluster->getProcessorById(onWhichProcessor)->delocateToNowhereOptionally(ine, shouldUseImaginary);
            } else {
                // edge has been read
                // cout<<"bla"<<endl;
                // cout << "NOWHERE! " << buildEdgeName(ine) << endl;
                if (const auto proc = findProcessorThatHoldsEdge(ine, cluster); proc != nullptr) {
                    if (proc->id == schedulingResult.processorOfAssignment->id)
                        schedulingResult.processorOfAssignment->delocateToNowhereOptionally(ine, shouldUseImaginary);
                    else
                        proc->delocateToNowhereOptionally(ine, shouldUseImaginary);
                }

                // assert(proc == nullptr);
            }
        }
        if (shouldUseImaginary)
            ine->imaginedLocations.clear();
        else
            ine->locations.clear();
    }

    checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);

    for (const auto v1 : vertex->out_edges) {
        schedulingResult.processorOfAssignment->loadFromNowhere(v1, shouldUseImaginary, schedulingResult.finishTime);
        checkIfPendingMemoryCorrect(schedulingResult.processorOfAssignment);
        if (schedulingResult.processorOfAssignment->getAvailableMemory() < 0) {
            std::cout << "";
        }
    }
    cluster->getProcessorById(schedulingResult.processorOfAssignment->id)->updateFrom(*schedulingResult.processorOfAssignment);
    for (const auto& [proc_id, processor] : cluster->getProcessors()) {
        checkIfPendingMemoryCorrect(processor);
    }
}



*/