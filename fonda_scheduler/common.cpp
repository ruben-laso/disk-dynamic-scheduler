//
// Created by kulagins on 26.05.23.
//

#include "../include/fonda_scheduler/common.hpp"

#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "graph.hpp"

bool Debug;

void Cluster::printAssignment()
{
    int counter = 0;
    for (const auto& [proc_id, processor] : this->getProcessors()) {
        if (processor->isBusy) {
            counter++;
            std::cout << "Processor" << counter << "." << '\n';
            std::cout << "\tMem: " << processor->getMemorySize() << ", Proc: " << processor->getProcessorSpeed() << '\n';

            const vertex_t* assignedVertex = processor->getAssignedTask();
            if (assignedVertex == nullptr)
                std::cout << "No assignment." << '\n';
            else {
                std::cout << "Assigned subtree, id " << assignedVertex->id << ", leader " << -1 << ", memReq " << assignedVertex->memoryRequirement << '\n';
                for (vertex_t* u = assignedVertex->subgraph->source; u; u = next_vertex_in_topological_order(assignedVertex->subgraph, u)) {
                    std::cout << u->name << ", ";
                }
                std::cout << '\n';
            }
        }
    }
}

void printDebug(const std::string& str)
{
    if (Debug) {
        std::cout << str << '\n';
    }
}
void printInlineDebug(const std::string& str)
{
    if (Debug) {
        std::cout << str;
    }
}

void checkForZeroMemories(graph_t* graph)
{
    for (vertex_t* vertex = graph->source; vertex; vertex = next_vertex_in_topological_order(graph, vertex)) {
        if (vertex->memoryRequirement == 0) {
            printDebug("Found a vertex with 0 memory requirement, name: " + vertex->name);
        }
    }
}

void removeSourceAndTarget(graph_t* graph, std::vector<std::pair<vertex_t*, double>>& ranks)
{

    auto iterator = std::find_if(ranks.begin(), ranks.end(),
        [](const std::pair<vertex_t*, int>& pair1) { return pair1.first->name == "GRAPH_SOURCE"; });
    if (iterator != ranks.end()) {
        ranks.erase(iterator);
    }
    iterator = find_if(ranks.begin(), ranks.end(),
        [](const std::pair<vertex_t*, int>& pair1) { return pair1.first->name == "GRAPH_TARGET"; });
    if (iterator != ranks.end()) {
        ranks.erase(iterator);
    }

    const vertex_t* startV = findVertexByName(graph, "GRAPH_SOURCE");
    const vertex_t* targetV = findVertexByName(graph, "GRAPH_TARGET");
    if (startV != nullptr)
        remove_vertex(graph, startV);
    if (targetV != nullptr)
        remove_vertex(graph, targetV);
}

bool isLocatedNowhere(edge_t* edge, const bool imaginary)
{

    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    const auto it = std::find_if(locations.begin(), locations.end(),
        [](const Location& location) {
            return location.locationType == LocationType::Nowhere;
        });
    return locations.empty() || it != locations.end();
}

bool isLocatedOnDisk(edge_t* edge, const bool imaginary)
{

    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    return std::find_if(locations.begin(), locations.end(),
               [](const Location& location) {
                   return location.locationType == LocationType::OnDisk;
               })
        != locations.end();
}

bool isLocatedOnThisProcessor(edge_t* edge, int id, const bool imaginary)
{

    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    return std::find_if(locations.begin(), locations.end(),
               [id](const Location& location) {
                   return location.locationType == LocationType::OnProcessor && location.processorId == id;
               })
        != locations.end();
}

bool isLocatedOnAnyProcessor(edge_t* edge, const bool imaginary)
{

    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    return std::find_if(locations.begin(), locations.end(),
               [](const Location& location) {
                   return location.locationType == LocationType::OnProcessor;
               })
        != locations.end();
}

int whatProcessorIsLocatedOn(edge_t* edge, const bool imaginary)
{

    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    const auto locationOnProcessor = std::find_if(locations.begin(), locations.end(),
        [](const Location& location) {
            return location.locationType == LocationType::OnProcessor;
        });
    return (locationOnProcessor != locations.end()) ? locationOnProcessor->processorId.value() : -1;
}

std::string buildEdgeName(const edge_t* edge)
{
    return edge->tail->name + "-" + edge->head->name;
}

void delocateFromThisProcessorToDisk(edge_t* edge, int id, const bool imaginary, double afterWhen)
{
    //std::cout << buildEdgeName(edge)<<" delocate from proc "<<id<<" to disk imagine? "<<(imaginary?"yes":"no")<<std::endl;

    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    auto it = std::find_if(
        locations.begin(),
        locations.end(),
        [id](const Location& location) {
            return location.locationType == LocationType::OnProcessor
                && location.processorId == id;
        });
    if (it == locations.end()) {
        throw std::runtime_error(
            "Edge " + buildEdgeName(edge) + " not located on processor " + std::to_string(id));
    }

    locations.erase(it);

    if (!isLocatedOnDisk(edge, imaginary)) {
        locations.emplace_back(LocationType::OnDisk, std::nullopt, afterWhen);
    }
}

void delocateFromThisProcessorToNowhere(edge_t* edge, int id, const bool imaginary)
{
    // std::cout << buildEdgeName(edge)<<" delocate from proc "<<id<<" to nowhere imagine? "<<(imaginary?"yes":"no")<<std::endl;
    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    auto it = std::find_if(
        locations.begin(),
        locations.end(),
        [id](const Location& loc) {
            return loc.locationType == LocationType::OnProcessor
                && loc.processorId == id;
        });

    if (it == locations.end()) {
        throw std::runtime_error(
            "Edge " + buildEdgeName(edge) + " not located on processor " + std::to_string(id));
    }

    locations.erase(it);
}

void locateToThisProcessorFromDisk(edge_t* edge, int id, const bool imaginary, double afterWhen)
{

    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    if (!isLocatedOnDisk(edge, imaginary)) {
        throw std::runtime_error(
            "Cannot locate edge " + buildEdgeName(edge) + " to processor " + std::to_string(id) + " because it is not located on disk");
    }

    if (!isLocatedOnThisProcessor(edge, id, imaginary)) {
        locations.emplace_back(LocationType::OnProcessor, id, afterWhen);
    }
}

Location* getLocationOnProcessor(edge_t* edge, int id, const bool imaginary)
{
    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    auto it = std::find_if(locations.begin(), locations.end(),
        [id](const Location& location) {
            return location.locationType == LocationType::OnProcessor && location.processorId == id;
        });

    if (it == locations.end())
        return nullptr;

    return &*it;
}

Location* getLocationOnAnyProcessor(edge_t* edge, bool imaginary)
{
    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    auto it = std::find_if(
        locations.begin(),
        locations.end(),
        [](const Location& location) {
            return location.locationType == LocationType::OnProcessor;
        });

    if (it == locations.end())
        return nullptr;

    return &*it; // pointer to Location inside vector
}

Location* getLocationOnDisk(edge_t* edge, bool imaginary)
{
    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    auto it = std::find_if(
        locations.begin(),
        locations.end(),
        [](const Location& location) {
            return location.locationType == LocationType::OnDisk;
        });

    if (it == locations.end())
        return nullptr;

    return &*it;
}

void locateToThisProcessorFromNowhere(edge_t* edge, int id, const bool imaginary, double afterWhen)
{

    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    //  cout<<"locating from nowhere to proc "<<id <<" edge "; print_edge(edge);
    if (!isLocatedOnThisProcessor(edge, id, imaginary))
        locations.emplace_back(LocationType::OnProcessor, id, afterWhen);
}
void locateToDisk(edge_t* edge, const bool imaginary, double afterWhen)
{
    std::vector<Location>& locations = imaginary ? edge->imaginedLocations : edge->locations;
    if (!isLocatedOnDisk(edge, imaginary))
        locations.emplace_back(LocationType::OnDisk, std::nullopt, afterWhen);
}

double getSumOut(const vertex_t* v)
{
    double sumOut = 0;
    for (auto& out_edge : v->out_edges) {
        sumOut += out_edge->weight;
        //      cout<<sumOut<<" by "<<v->out_edges[i]->weight<<endl;
    }
    return sumOut;
}

double getSumIn(const vertex_t* v)
{
    double sumIn = 0;
    for (auto& in_edge : v->in_edges) {
        sumIn += in_edge->weight;
        //      cout<<sumOut<<" by "<<v->out_edges[i]->weight<<endl;
    }
    return sumIn;
}

void Event::fire()
{
   //std::cout<<"fire event "<<this->id<<" at " <<this->getActualTimeFire()<<" planned at "<<this->getExpectedTimeFire()<<" on proc "<<this->processor->id <<std::endl;
    if (this->edge) {
        assert(this->edge->locations.size() >= 0);
        assert(this->edge->imaginedLocations.size() >= 0);
    }

    if (cluster->getProcessorById(this->processor->id).use_count() != this->processor.use_count()) {
        assert(this->timesFired == 0); // jsut came in with a processor from a wrong cluster
        this->processor = cluster->getProcessorById(this->processor->id);
    }
    assert(cluster->getProcessorById(this->processor->id).use_count() == this->processor.use_count());

    assert(this->actualTimeFire >= runtimeNow() || abs(this->actualTimeFire - runtimeNow()) < 0.01);

    this->timesFired++;

    if (this->type == OnTaskFinish && !this->processor->getLastComputeEvent().expired() && this->processor->getLastComputeEvent().lock()->id == this->id) {
        // we are last compute event
        this->processor->setReadyTimeCompute(this->getActualTimeFire());
    }
    if (this->type == OnReadFinish && !this->processor->getLastReadEvent().expired() && this->processor->getLastReadEvent().lock()->id == this->id) {
        // we are last read event
        this->processor->setReadyTimeRead(this->getActualTimeFire());
    }
    if (this->type == OnWriteFinish && !this->processor->getLastWriteEvent().expired() && this->processor->getLastWriteEvent().lock()->id == this->id) {
        // we are last write event
        this->processor->setReadyTimeWrite(this->getActualTimeFire());
    }

    switch (this->type) {
    case eventType::OnTaskStart:
        fireTaskStart();
        break;
    case eventType::OnTaskFinish:
        fireTaskFinish();
        break;
    case eventType::OnReadStart:
        fireReadStart();
        break;
    case eventType::OnReadFinish:
        fireReadFinish();
        break;
    case eventType::OnWriteStart:
        fireWriteStart();
        break;
    case eventType::OnWriteFinish:
        fireWriteFinish();
        break;
    }
    this->timesFired++;
}

void Processor::updateFrom(const Processor& other)
{

    assert(this->assignedTask == nullptr || this->assignedTask->id == other.assignedTask->id);

    std::unordered_map<std::string, std::weak_ptr<Event>> updatedEvents;

    this->readyTimeCompute = other.readyTimeCompute;
    this->readyTimeRead = other.readyTimeRead;
    this->readyTimeWrite = other.readyTimeWrite;

    assert(other.availableMemory <= other.getMemorySize() || std::abs(other.availableMemory - other.getMemorySize()) < 1);
    this->availableMemory = other.availableMemory;
    this->availableMemoryDuringPreviousTask = other.availableMemoryDuringPreviousTask;
    this->startOfLastTask = other.startOfLastTask;
    std::set<edge_t*, std::function<bool(edge_t*, edge_t*)>> updatedMemories(comparePendingMemories);
    // First, add elements that exist in both and new ones from 'other'
    for (auto* mem : other.pendingMemories) {
        updatedMemories.insert(mem); // Only inserts new ones, duplicates are ignored
    }
    // Swap the updated set into place
    pendingMemories.swap(updatedMemories);

    assert(other.afterAvailableMemory < other.getMemorySize() || std::abs(other.afterAvailableMemory - other.getMemorySize()) < 0.01);
    this->afterAvailableMemory = other.afterAvailableMemory;
    updatedMemories.clear();
    // First, add elements that exist in both and new ones from 'other'
    for (auto* mem : other.afterPendingMemories) {
        updatedMemories.insert(mem); // Only inserts new ones, duplicates are ignored
    }
    // Swap the updated set into place
    afterPendingMemories.swap(updatedMemories);

    this->lastReadEvent = other.lastReadEvent;
    this->lastWriteEvent = other.lastWriteEvent;
    this->lastComputeEvent = other.lastComputeEvent;

    this->writingQueue.clear();
    for (edge_t* e : other.writingQueue) {
        this->writingQueue.emplace_back(e);
    }
}

void clearGraph(const graph_t* graphMemTopology)
{
    vertex_t* vertex = graphMemTopology->first_vertex;
    while (vertex != nullptr) {
        vertex->makespan = vertex->makespanPerceived = -1;
        vertex->visited = false;
        vertex->status = Status::Unscheduled;
        vertex->actuallyUsedMemory = -1;
        vertex->rank = -1;
        vertex->bottom_level = -1;
        vertex->assignedProcessorId = -1;
        vertex = vertex->next;
    }

    edge_t* edge = graphMemTopology->first_edge;
    while (edge != nullptr) {
        edge->locations.clear();
        edge->imaginedLocations.clear();
        edge = edge->next;
    }
}


double computeOutDegreeVariance(graph_t* dag) {
    std::vector<int> outDegrees;
    int totalNodes = 0;
    double sum = 0;

    // 1. Collect all out-degrees
    vertex_t* vertex = dag->first_vertex;
    while (vertex != nullptr) {
        int degree = vertex->out_edges.size();
        outDegrees.push_back(degree);
        sum += degree;
        totalNodes++;
        vertex = vertex->next;
    }

    if (totalNodes == 0) return 0.0;

    // 2. Calculate Mean
    double mean = sum / totalNodes;

    // 3. Calculate Variance (The Average of squared differences)
    double varianceSum = 0;
    for (int degree : outDegrees) {
        varianceSum += std::pow(degree - mean, 2);
    }

    // !!! FIX: You must divide by totalNodes to get the actual Variance
    double actualVariance = varianceSum / totalNodes;

    // 4. Calculate Scaled Hub Score (Coefficient of Variation squared)
    if (mean == 0) return 0.0;

    // This is now scale-resistant
    double scaledHubScore = actualVariance / (mean * mean);

    return scaledHubScore;
}
/*
 *
 * 1 - offline BL
 * 2 - offline BLC
 * 3 - offline MM
 * 4 - offline L
 * 5 - offline BL-R
 * 6 - online BL
 * 7 - online BLC
 * 8 - online MM
 * 9 - online L
 * 10 - online BL-R
 *
 */
 int findBestAlgorithmForDag(graph_t* dag, bool deviationsExist)
{
    bool existsBypassBranch=false;
    vertex_t* vertex = dag->first_vertex;
    while (vertex != nullptr) {
        if (vertex->in_edges.size()==1 && vertex->out_edges.size()==1
        && vertex->in_edges.at(0)->tail->in_edges.empty()  && vertex->out_edges.at(0)->head->out_edges.empty()
        ) {
            existsBypassBranch=true;
            break;
        }
        vertex = vertex->next;
    }


    std::set<vertex_t*> firstLevelChildren;
    std::set<vertex_t*> secondLevelChildren;

    double avgEdgeWeightOnFirstTwoLevels= 0;
    int numEdgesOnFirst2Levels=0;

    double averageEdgeWeight=0;
    float allTaskMemoryRequirements=0;
    float allEdgeWeights=0;
    float allComputations=0;

    vertex = dag->first_vertex;
    while (vertex != nullptr) {
        allTaskMemoryRequirements+= vertex->memoryRequirement;
        allComputations+= vertex->time;

        bool isChildOfSource=false;
        for (auto in_edge : vertex->in_edges) {
            if (in_edge->tail->in_edges.size()==0) {
               firstLevelChildren.insert(vertex);
               isChildOfSource=true;
                avgEdgeWeightOnFirstTwoLevels+= in_edge->weight;
                numEdgesOnFirst2Levels++;
            }
            allEdgeWeights+= in_edge->weight;
        }
        if (isChildOfSource) {
            //all its children are second children of source
            for (auto out_edge : vertex->out_edges) {
                secondLevelChildren.insert(out_edge->head);
                avgEdgeWeightOnFirstTwoLevels+= out_edge->weight;
                numEdgesOnFirst2Levels++;
            }
        }
        vertex = vertex->next;
    }

    avgEdgeWeightOnFirstTwoLevels/=numEdgesOnFirst2Levels;
    averageEdgeWeight= allEdgeWeights / dag->number_of_edges;



    int singleChildNodes = 0;
    vertex = dag->first_vertex;
    while (vertex != nullptr) {
        if (vertex->out_edges.size() == 1) {
            singleChildNodes++;
        }
        vertex = vertex->next;
    }

    double chainProbability = (double)singleChildNodes / dag->vertices_by_id.size();

    double hubScore = computeOutDegreeVariance(dag);

    if (chainProbability>0.45) {
        //thin workflows, need online methods
        return 6; //any online method will do
    }

    if (hubScore<0.87) {
        if (chainProbability<0.2) {
            return 1;
        }
        return 3;   //offline MM
    }

    if (existsBypassBranch && avgEdgeWeightOnFirstTwoLevels>averageEdgeWeight*2) {
        //large weights on top and a bypass branch
        if (dag->vertices_by_id.size()<200) {
            //for very small graphs, online is better
            return 9;
        }
        return 5; // offline BLR
    }

    if (allTaskMemoryRequirements > allComputations * 100) {
        // memory-heavy
        return 4; // offline L
    }

    if (allEdgeWeights > allTaskMemoryRequirements * 10) {
        // io-heavy
        return 3; // offline MM
    }
    if (deviationsExist) {
        return 6; // fallback - online BL
    }
    return 1; // fallback - offline BL
}