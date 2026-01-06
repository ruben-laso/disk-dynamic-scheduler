
#ifndef FONDA_SCHED_COMMON_HPP
#define FONDA_SCHED_COMMON_HPP

#include "../../extlibs/memdag/src/graph.hpp"
#include "cluster.hpp"
#include "json.hpp"

#include <regex>
#include <unordered_set>
#include <utility>

class Assignment {

public:
    vertex_t* task;
    Processor* processor;
    double startTime;
    double finishTime;

    Assignment(vertex_t* t, Processor* p, const double st, const double ft)
    {
        this->task = t;
        this->processor = p;
        this->startTime = st;
        this->finishTime = ft;
    }

    [[nodiscard]] nlohmann::json toJson() const
    {
        std::string tn = task->name;
        std::transform(tn.begin(), tn.end(), tn.begin(),
            [](const auto c) { return std::tolower(c); });
        return nlohmann::json {
            { "task", tn },
            { "start", startTime },
            { "machine", processor->id },
            { "finish", finishTime }
        };
    }
};

void printDebug(const std::string& str);

void printInlineDebug(const std::string& str);

void checkForZeroMemories(graph_t* graph);

////void completeRecomputationOfSchedule(Http::ResponseWriter &resp, const json &bodyjson, double timestamp, vertex_t * vertexThatHasAProblem);
void removeSourceAndTarget(graph_t* graph, std::vector<std::pair<vertex_t*, double>>& ranks);

void clearGraph(const graph_t* graphMemTopology);

Cluster*
prepareClusterWithChangesAtTimestamp(const nlohmann::json& bodyjson, double timestamp, std::vector<Assignment*>& tempAssignments);

// void delayOneTask(Http::ResponseWriter &resp, const json &bodyjson, string &nameOfTaskWithProblem, double newStartTime,
//                  Assignment *assignmOfProblem);
void delayEverythingBy(const std::vector<Assignment*>& assignments, const Assignment* startingPoint, double delayTime);

void takeOverChangesFromRunningTasks(const nlohmann::json& bodyjson, graph_t* currentWorkflow, std::vector<Assignment*>& assignments);

class EventManager;

enum eventType {
    OnWriteStart = 0,
    OnWriteFinish = 1,
    OnReadStart = 2,
    OnReadFinish = 3,
    OnTaskStart = 4,
    OnTaskFinish = 5
};

class Event : public std::enable_shared_from_this<Event> {
public:
    std::string id;
    vertex_t* task = nullptr;
    edge_t* edge = nullptr;
    eventType type;
    std::shared_ptr<Processor> processor;

    bool onlyPreemptive = false;
    bool isDone = false;
    int timesFired = 0;

private:
    double expectedTimeFire = -1.0;
    double actualTimeFire   = -1.0;

    // true once inserted into EventManager
    bool isManaged = false;
    // Only EventManager may change event times
    friend class EventManager;


    void setActualTimeFireInternal(double t) noexcept {
        if (isManaged)
            throw std::logic_error("Cannot modify actualTimeFire after insertion into EventManager");

        actualTimeFire = t;
    }

    void setExpectedTimeFireInternal(double t) noexcept {
        if (isManaged)
            throw std::logic_error("Cannot modify expectedTimeFire after insertion into EventManager");

        expectedTimeFire = t;
    }

    void forceUpdateTimeFromManager(double t) noexcept
    {
        actualTimeFire = t;
        expectedTimeFire = t;
    }

public:
    double getActualTimeFire() const noexcept { return actualTimeFire; }
    double getExpectedTimeFire() const noexcept { return expectedTimeFire; }

    // combined logical view
    double getVisibleTimeFireForPlanning() const noexcept {
        return isDone ? actualTimeFire : expectedTimeFire;
    }

    std::vector<std::weak_ptr<Event>> successors;
    std::vector<std::shared_ptr<Event>> predecessors;

    void initialize(const std::vector<std::shared_ptr<Event>>& predecessors,
        const std::vector<std::weak_ptr<Event>>& successors)
    {
        for (auto& pred : predecessors) {
            this->addPredecessorInPlanning(pred);
        }
        for (auto& succ : successors) {
            this->addSuccessorInPlanning(succ);
        }
    }

    Event(vertex_t* task, edge_t* edge,
        const eventType type, const std::shared_ptr<Processor>& processor,
        const double expectedTimeFire, const double actualTimeFire,
        const bool isEviction, std::string idN,
        const std::vector<std::shared_ptr<Event>>& predecessors = {},
        const std::vector<std::weak_ptr<Event>>& successors = {})
        : id(std::move(idN))
        , task(task)
        , edge(edge)
        , type(type)
        , processor(processor) // shared ownership
        , onlyPreemptive(isEviction)
        , expectedTimeFire(expectedTimeFire)
        , actualTimeFire(actualTimeFire)
    {
        initialize(predecessors, successors);
    }

     static std::shared_ptr<Event> createEvent(vertex_t* task, edge_t* edge,
        eventType type, const std::shared_ptr<Processor>& processor,
        double expectedTimeFire, double actualTimeFire,
        const std::vector<std::shared_ptr<Event>>& predecessors,
        const std::vector<std::weak_ptr<Event>>& successors,
        bool isEviction, const std::string& id)
    {
        return std::make_shared<Event>(task, edge, type, processor, expectedTimeFire,
            actualTimeFire, isEviction, id, predecessors, successors);
    }

    static std::shared_ptr<Event> createEvent(vertex_t* task, edge_t* edge,
        eventType type, const std::shared_ptr<Processor>& processor,
        double expectedTimeFire, double actualTimeFire,
        bool isEviction, const std::string& id)
    {
        return std::make_shared<Event>(task, edge, type, processor, expectedTimeFire,
            actualTimeFire, isEviction, id);
    }

    void cleanupSuccessors()
    {
        successors.erase(
            std::remove_if(
                successors.begin(),
                successors.end(),
                [](const std::weak_ptr<Event>& w) {
                    return w.expired();
                }),
            successors.end());
    }

    void removeSuccessorById(const std::string& idToRemove)
    {
        successors.erase(
            std::remove_if(
                successors.begin(),
                successors.end(),
                [&](const std::weak_ptr<Event>& w) {
                    auto s = w.lock();
                    return !s || s->id == idToRemove;
                }),
            successors.end());
    }

    void removePredecessorById(const std::string& idToRemove)
    {
        predecessors.erase(
            std::remove_if(
                predecessors.begin(),
                predecessors.end(),
                [&](const std::shared_ptr<Event>& w) {
                    return w->id == idToRemove;
                }),
            predecessors.end());
    }

    void cleanupDependencies()
    {
        removeFromDependencies();
    }


    static void propagateChainInPlanning(const std::shared_ptr<Event>& event, const double add, std::unordered_set<std::shared_ptr<Event>>& visited)
    {
        if (visited.count(event)) {
            return;
        }

        event->cleanupSuccessors();
        visited.insert(event);

        for (auto& succ_ : event->successors) {
            const auto& successor = succ_.lock();
            if (!successor) {
                throw std::runtime_error("Successor is expired or null in propagateChainInPlanning");
            }
            const double newTime = successor->getVisibleTimeFireForPlanning() + add;
            successor->setActualTimeFireInternal(newTime);
            successor->setExpectedTimeFireInternal(newTime);

            propagateChainInPlanning(successor, add, visited);
        }
    }

    void addPredecessorInPlanning(const std::shared_ptr<Event>& pred)
    {
        pred->cleanupSuccessors();
        if (pred->id == this->id) {
            throw std::runtime_error("ADDING OURSELVES AS PREDECESSOR!");
        }

        if (std::find(predecessors.begin(), predecessors.end(), pred) != predecessors.end()) {

            // Already a predecessor, no need to add again
            return;
        }

        this->predecessors.emplace_back(pred);

        const double predsVisibleTime = pred->getVisibleTimeFireForPlanning();
        if (predsVisibleTime > this->actualTimeFire) {
            const double diff = predsVisibleTime - this->actualTimeFire;
            this->setActualTimeFireInternal(predsVisibleTime);
            this->setExpectedTimeFireInternal(predsVisibleTime);

            std::unordered_set<std::shared_ptr<Event>> visited;
            propagateChainInPlanning(shared_from_this(), diff, visited);
        }

        pred->addSuccessorInPlanning(shared_from_this());
    }

    // Modified addSuccessorInPlanning to use unordered_set
    void addSuccessorInPlanning(const std::weak_ptr<Event>& succ_)
    {
        const auto& succ = succ_.lock();
        if (!succ) {
            throw std::runtime_error("Successor is expired or null in addSuccessorInPlanning");
        }

        if (succ->id == this->id) {
            throw std::runtime_error("ADDING OURSELVES AS SUCCESSOR!");
        }
        this->cleanupSuccessors();
        this->successors.emplace_back(succ); // Always insert (either first time or replacing one with same ID)

        // Adjust successor timing if needed
        const double succsVisibleTime = succ->getVisibleTimeFireForPlanning();
        if (succsVisibleTime < this->expectedTimeFire) {
            const double diff = this->expectedTimeFire - succsVisibleTime;
            succ->setActualTimeFireInternal(this->expectedTimeFire);
            succ->setExpectedTimeFireInternal(this->expectedTimeFire);

            std::unordered_set<std::shared_ptr<Event>> visited;
            propagateChainInPlanning(succ, diff, visited);
        }

        // Add this as a predecessor of succ if not already present
        const bool alreadyPredecessor = std::find(succ->predecessors.begin(), succ->predecessors.end(), shared_from_this()) != succ->predecessors.end();
        if (!alreadyPredecessor) {
            succ->addPredecessorInPlanning(shared_from_this());
        }
    }


    static bool hasCycleFrom(const std::shared_ptr<Event>& event, std::unordered_set<std::string>& visited, std::unordered_set<std::string>& recStack,
        const bool checkPredecessors)
    {
        if (recStack.find(event->id) != recStack.end()) {
            std::cout << "Cycle detected at event: " << event->id << '\n';
            return true; // Cycle detected!
        }

        if (visited.find(event->id) != visited.end()) {
            return false; // Already checked, no cycle found
        }

        visited.insert(event->id);
        recStack.insert(event->id);

        // Choose to check either predecessors or successors
        if (checkPredecessors) {
            for (const auto& predecessor : event->predecessors) {
                if (hasCycleFrom(predecessor, visited, recStack, checkPredecessors)) {
                    // If a cycle is detected, remove the event from the predecessor/successor list
                    predecessor->removeSuccessorById(event->id);
                    event->removePredecessorById(predecessor->id);
                    return true;
                }
            }
        } else {
            for (const auto& successor : event->successors) {
                if (hasCycleFrom(successor.lock(), visited, recStack, checkPredecessors)) {
                    // If a cycle is detected, remove the event from the predecessor/successor list
                    successor.lock()->removePredecessorById(event->id);
                    event->removeSuccessorById(successor.lock()->id);
                    return true;
                }
            }
        }

        recStack.erase(event->id); // Remove from recursion stack after processing
        return false;
    }

    bool checkCycleFromEvent()
    {
        std::unordered_set<std::string> visited;
        std::unordered_set<std::string> recStack; // Tracks the current path

        return hasCycleFrom(shared_from_this(), visited, recStack, true) || hasCycleFrom(shared_from_this(), visited, recStack, false);
    }

    void adjustBothPlannedFireTimes(double t)
    {
        if (isManaged)
            throw std::logic_error("Cannot modify after insertion into EventManager");

        setExpectedTimeFireInternal(t);
        setActualTimeFireInternal(t);

        // also update affected successors if needed
        std::unordered_set<std::shared_ptr<Event>> visited;
        propagateChainInPlanning(shared_from_this(), 0.0, visited);
    }



    auto& getPredecessors()
    {
        return predecessors;
    }

    const auto& getPredecessors() const
    {
        return predecessors;
    }

    auto& getSuccessors()
    {
        return successors;
    }

    const auto& getSuccessors() const
    {
        return successors;
    }

    void fire();

    void fireTaskStart();

    void fireTaskFinish();

    void fireReadStart();

    void fireReadFinish();

    void fireWriteStart();

    void fireWriteFinish();

    void removeFromSuccessors();

    void removeFromPredecessors();

    void removeFromDependencies();

};

struct CompareByTimestamp {
    bool operator()(const std::shared_ptr<Event>& a,
                    const std::shared_ptr<Event>& b) const
    {
        if (a->getActualTimeFire() != b->getActualTimeFire())
            return a->getActualTimeFire() < b->getActualTimeFire();
        return a->id < b->id;
    }
};

class EventManager {
public:
    using EventPtr = std::shared_ptr<Event>;
    using EventSet = std::set<EventPtr, CompareByTimestamp>;

private:
    EventSet eventSet;  // ordered by timestamp
    std::unordered_map<std::string, EventSet::iterator> eventById;
    std::unordered_map<int, EventSet> eventsByProcessor;

public:
    EventManager() = default;

    // ---------------------------
    // INSERT
    // ---------------------------
    bool insert(const EventPtr& ev)
    {
        // Remove existing event with same ID
        auto it = eventById.find(ev->id);
        if (it != eventById.end()) {
            eraseInternal(it); // remove from all structures
        }

        auto [setIt, ok] = eventSet.insert(ev);
        if (!ok) return false;

        eventById[ev->id] = setIt;
        eventsByProcessor[ev->processor->id].insert(ev);

        // 🔒 Freeze mutation after insertion
        ev->isManaged = true;


        // maintain dependency cross-links
        for (auto& p : ev->getPredecessors())
            p->addSuccessorInPlanning(ev);

        for (auto& s : ev->getSuccessors())
            if (auto sp = s.lock())
                sp->addPredecessorInPlanning(ev);

        return true;
    }

    // ---------------------------
    // RESCHEDULE (safe timestamp change)
    // ---------------------------
    bool reschedule(const std::string& id, double newTime)
    {

        auto it = eventById.find(id);
        if (it == eventById.end()) return false;

        EventPtr ev = *(it->second);

        eraseInternal(it);

        // bypass flag because manager is allowed
        ev->forceUpdateTimeFromManager(newTime);

        return insert(ev);

    }

    bool remove(const std::string& id)
    {
        auto it = eventById.find(id);
        if (it == eventById.end()) return false;

        eraseInternal(it);
        return true;
    }

    EventPtr find(const std::string& id) const
    {
        auto it = eventById.find(id);
        return (it == eventById.end()) ? nullptr : *(it->second);
    }

    const EventSet& eventsForProcessor(int pid) const
    {
        static EventSet empty;
        auto it = eventsByProcessor.find(pid);
        return (it != eventsByProcessor.end()) ? it->second : empty;
    }


    EventPtr earliestReady() const
    {
        for (const auto& e : eventSet)
            if (e->getPredecessors().empty())
                return e;

        return nullptr;
    }


    bool empty() const { return eventSet.empty(); }

    void clear()
    {
        eventSet.clear();
        eventById.clear();
        eventsByProcessor.clear();
    }

private:

    void eraseInternal(std::unordered_map<std::string, EventSet::iterator>::iterator it)
    {

        EventPtr ev = *(it->second);
        int pid = ev->processor->id;


        auto pit = eventsByProcessor.find(pid);
        if (pit != eventsByProcessor.end()) {
            pit->second.erase(ev);
            if (pit->second.empty())
                eventsByProcessor.erase(pit);
        }


        eventSet.erase(it->second);


        eventById.erase(it);

        // maintain dependency structure
        ev->removeFromDependencies();
    }
};


struct CompareByRank {

    bool operator()(const vertex_t* a, const vertex_t* b) const {
        assert(a->rank!=-1);
        assert(b->rank!=-1);

        if (a->rank != b->rank)
            return a->rank < b->rank; // max-heap

        // tie-breakers
        if (a->time != b->time)
            return a->time < b->time;

        if (a->out_edges.size() != b->out_edges.size())
            return a->out_edges.size() < b->out_edges.size();

        return a->id > b->id;
    }

};

class ReadyQueue {
public:
    std::set<vertex_t*, CompareByRank> readyTasks;
};

#endif