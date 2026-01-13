//
// Created by kulagins on 26.05.23.
//

#include "../include/fonda_scheduler/common.hpp"

#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include "graph.hpp"

bool Debug;

void delayEverythingBy(const std::vector<Assignment*>& assignments, const Assignment* startingPoint, const double delayTime)
{

    for (const auto& assignment : assignments) {
        if (assignment->startTime >= startingPoint->startTime) {
            assignment->startTime += delayTime;
            assignment->finishTime += delayTime;
        }
    }
}

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
            "Edge " + buildEdgeName(edge) +
            " not located on processor " + std::to_string(id)
        );
    }

    locations.erase(it);

    if (!isLocatedOnDisk(edge, imaginary)) {
        locations.emplace_back(LocationType::OnDisk, std::nullopt, afterWhen);
    }
}

void delocateFromThisProcessorToNowhere(edge_t* edge, int id, const bool imaginary)
{
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
            "Edge " + buildEdgeName(edge) +
            " not located on processor " + std::to_string(id)
        );
    }

    locations.erase(it);
}

void locateToThisProcessorFromDisk(edge_t* edge, int id, const bool imaginary, double afterWhen)
{

    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    if (!isLocatedOnDisk(edge, imaginary)) {
        throw std::runtime_error(
            "Cannot locate edge " + buildEdgeName(edge) +
            " to processor " + std::to_string(id) +
            " because it is not located on disk"
        );
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
            return location.locationType == LocationType::OnProcessor &&
                   location.processorId == id;
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
        }
    );

    if (it == locations.end())
        return nullptr;

    return &*it;   // pointer to Location inside vector
}


Location* getLocationOnDisk(edge_t* edge, bool imaginary)
{
    auto& locations = imaginary ? edge->imaginedLocations : edge->locations;

    auto it = std::find_if(
        locations.begin(),
        locations.end(),
        [](const Location& location) {
            return location.locationType == LocationType::OnDisk;
        }
    );

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
        locations.emplace_back(LocationType::OnDisk, afterWhen);
}

double getSumOut(const vertex_t* v)
{
    double sumOut = 0;
    for (auto & out_edge : v->out_edges) {
        sumOut += out_edge->weight;
        //      cout<<sumOut<<" by "<<v->out_edges[i]->weight<<endl;
    }
    return sumOut;
}

double getSumIn(const vertex_t* v)
{
    double sumIn = 0;
    for (auto & in_edge : v->in_edges) {
        sumIn += in_edge->weight;
        //      cout<<sumOut<<" by "<<v->out_edges[i]->weight<<endl;
    }
    return sumIn;
}

void Event::fire()
{
    if (this->edge) {
        assert(this->edge->locations.size()>=0);
        assert(this->edge->imaginedLocations.size()>=0);
    }

    // PASS 1: absolute repair
    std::vector<TimeShift> repair;
    this->enforceSuccessorConstraints(repair);
    // Apply any repairs
    for (auto& s : repair) {
        events.reschedulePure(s.ev->id, s.newTime);
    }

    this->timesFired++;

    if (this->type==OnTaskFinish && !this->processor->getLastComputeEvent().expired() && this->processor->getLastComputeEvent().lock()->id==this->id) {
        //we are last compute event
        this->processor->setReadyTimeCompute(this->getActualTimeFire());
    }
    if (this->type==OnReadFinish && !this->processor->getLastReadEvent().expired() && this->processor->getLastReadEvent().lock()->id==this->id) {
        //we are last read event
        this->processor->setReadyTimeRead(this->getActualTimeFire());
    }
    if (this->type==OnWriteFinish && !this->processor->getLastWriteEvent().expired() && this->processor->getLastWriteEvent().lock()->id==this->id) {
        //we are last write event
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
        vertex = vertex->next;
    }

    edge_t* edge = graphMemTopology->first_edge;
    while (edge != nullptr) {
        edge->locations.clear();
        edge->imaginedLocations.clear();
        edge = edge->next;
    }
}