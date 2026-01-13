
#ifndef FONDA_SCHED_COMMON_HPP
#define FONDA_SCHED_COMMON_HPP

#include "../../extlibs/memdag/src/graph.hpp"
#include "cluster.hpp"
#include "json.hpp"

#include <regex>
#include <unordered_set>
#include <utility>

extern double timeInSystem;

inline double runtimeNow()
{
    return timeInSystem;
}

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

struct TimeShift {
    Event* ev;
    double newTime;
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
    double actualTimeFire = -1.0;

    // true once inserted into EventManager
    bool isManaged = false;
    // Only EventManager may change event times
    friend class EventManager;

    void setActualTimeFireInternal(double t) noexcept
    {
        // std::cout << "set actual time of "<<this->id<< " to "<<t<<std::endl;;
        assert(!isManaged && "Cannot mutate topology of live event");

        actualTimeFire = t;
    }

    void setExpectedTimeFireInternal(double t) noexcept
    {
        if (isManaged)
            throw std::logic_error("Cannot modify expectedTimeFire after insertion into EventManager");

        expectedTimeFire = t;
    }

    void forceUpdateTimeFromManager(double t) noexcept
    {
        actualTimeFire = t;
    }

public:
    double getActualTimeFire() const noexcept { return actualTimeFire; }
    double getExpectedTimeFire() const noexcept { return expectedTimeFire; }

    // combined logical view
    double getVisibleTimeFireForPlanning() const noexcept
    {
        return isDone ? actualTimeFire : expectedTimeFire;
    }

    std::vector<std::weak_ptr<Event>> successors;
    std::vector<std::shared_ptr<Event>> predecessors;

    std::vector<std::shared_ptr<Event>> getPredecessors() { return predecessors; }
    std::vector<std::weak_ptr<Event>> getSuccessors() { return successors; }

    void initialize(const std::vector<std::shared_ptr<Event>>& predecessors,
        const std::vector<std::weak_ptr<Event>>& successors)
    {
        for (auto& pred : predecessors) {
            this->addPredecessorInPlanning(pred);
        }
        for (auto& succ : successors) {
            succ.lock()->addPredecessorInPlanning(shared_from_this());
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

    bool isStart() const
    {
        return type == OnTaskStart || type == OnReadStart || type == OnWriteStart;
    }

    bool isFinish() const
    {
        return type == OnTaskFinish || type == OnReadFinish || type == OnWriteFinish;
    }

    double earliestAllowedTimeNoPlanning() const
    {
        double t = -std::numeric_limits<double>::infinity();
        for (auto& p : predecessors) {
            t = std::max(t, p->getActualTimeFire());
        }
        return t;
    }

    double earliestAllowedTime(
        const std::vector<TimeShift>& shifts)
    {
        // std::cout << "earliest allowed time for "<<this->id<<" ";
        double t = -std::numeric_limits<double>::infinity();
        for (auto p : predecessors) {
            t = std::max(t, plannedTime(p, shifts));
        }
        // std::cout << " is "<<t<<std::endl;;
        return t;
    }

    double plannedTime(std::shared_ptr<Event> e,
        const std::vector<TimeShift>& shifts)
    {
        for (auto it = shifts.rbegin(); it != shifts.rend(); ++it) {
            if (it->ev->id == e->id) {
                //      std::cout << "found time in shifts for "<<e->id<<std::endl;
                return it->newTime;
            }
        }
        return e->getActualTimeFire();
    }

    double effectiveTime(std::shared_ptr<Event> e,
        const std::vector<TimeShift>& shifts)
    {
        for (auto it = shifts.rbegin(); it != shifts.rend(); ++it) {
            if (it->ev->id == e->id)
                return it->newTime;
        }
        return e->getActualTimeFire();
    }

    void pushAllSuccessorsLaterRuntime( std::vector<TimeShift>& shifts)
    {
        std::unordered_set<Event*> visited;
        propagatePushLaterRuntime(visited, shifts);
    }

    void propagatePushLaterRuntime(
        std::unordered_set<Event*>& visited,
        std::vector<TimeShift>& shifts)
    {
        if (!visited.insert(this).second)
            return;

        const double myTime = effectiveTime(shared_from_this(), shifts);

        for (auto& sw : successors) {
            auto s = sw.lock();
            if (!s)
                continue;

            const double sTime = effectiveTime(s, shifts);

            if (sTime < myTime) {
                shifts.push_back({ s.get(), myTime });

                s->propagatePushLaterRuntime(visited, shifts);
            }
        }
    }

    void pullAllSuccessorsEarlierByRuntime( std::vector<TimeShift>& shifts)
    {

        std::unordered_set<Event*> visited;
        propagatePullEarlierRuntime( visited, shifts);
    }

    void propagatePullEarlierRuntime( std::unordered_set<Event*>& visited,
    std::vector<TimeShift>& shifts)
    {
        if (!visited.insert(this).second)
            return;

        const double limiting = earliestAllowedTime(shifts);
        const double myTime   = effectiveTime(shared_from_this(), shifts);

        // Pull ONLY if legal
        if (myTime > limiting) {
            shifts.push_back({ this, limiting });
        }

        for (auto& sw : successors) {
            auto s = sw.lock();
            if (!s) continue;

            s->propagatePullEarlierRuntime(visited, shifts);
        }
    }


    void enforceSuccessorConstraints(std::vector<TimeShift>& shifts)
    {
        std::unordered_set<Event*> visited;
        enforceSuccessorConstraintsImpl(visited, shifts);
    }

    void enforceSuccessorConstraintsImpl(
        std::unordered_set<Event*>& visited,
        std::vector<TimeShift>& shifts)
    {
        if (!visited.insert(this).second)
            return;

        cleanupSuccessors();

        for (auto& sw : successors) {
            auto s = sw.lock();
            if (!s)
                continue;

            double earliestAllowed = s->earliestAllowedTime(shifts);

            // ONLY fix illegal successors
            if (plannedTime(s, shifts) < earliestAllowed) {

                shifts.push_back({ s.get(), earliestAllowed });

                // Continue repairing downstream
                s->enforceSuccessorConstraintsImpl(visited, shifts);
            }
        }
    }

    void addPredecessorPure(const std::shared_ptr<Event>& pred)
    {
        assert(!isManaged && "Cannot mutate topology of live event");
        if (pred->id == this->id) {
            throw std::runtime_error("ADDING OURSELVES AS PREDECESSOR!");
        }

        if (std::find(predecessors.begin(), predecessors.end(), pred) != predecessors.end()) {
            // Already a predecessor, no need to add again
            return;
        }

        this->predecessors.emplace_back(pred);

        pred->addSuccessorPure(shared_from_this());
    }

    void addSuccessorPure(const std::weak_ptr<Event>& succ_)
    {
        const auto& succ = succ_.lock();
        if (!succ) {
            throw std::runtime_error("Successor is expired or null in addSuccessorInPlanning");
        }
        if (succ->id == this->id) {
            throw std::runtime_error("ADDING OURSELVES AS SUCCESSOR!");
        }
        this->successors.emplace_back(succ); // Always insert (either first time or replacing one with same ID)
        // Add this as a predecessor of succ if not already present
        const bool alreadyPredecessor = std::find(succ->predecessors.begin(), succ->predecessors.end(), shared_from_this()) != succ->predecessors.end();
        if (!alreadyPredecessor) {
            succ->addPredecessorPure(shared_from_this());
        }
    }

    void addPredecessorInPlanning(std::shared_ptr<Event> predecessor)
    {
        // 1. structural edge
        addPredecessorPure(predecessor);

        // 2. compute latest allowed start
        double predVisible = predecessor->getVisibleTimeFireForPlanning();
        double succVisible = this->getVisibleTimeFireForPlanning();

        if (predVisible <= succVisible)
            return; // already valid, nothing to do

        double diff = predVisible - succVisible;

        // 3. shift THIS event
        this->setActualTimeFireInternal(this->getActualTimeFire() + diff);
        this->setExpectedTimeFireInternal(this->getExpectedTimeFire() + diff);

        // 4. and shift ALL successors
        propagateAllSuccessorsForward(diff, /*inPlanning=*/true);
    }

    void propagateAllSuccessorsForward(double diff, bool inPlanning)
    {
        if (diff <= 0.0)
            return;

        this->cleanupSuccessors();

        // visited prevents cycles
        std::unordered_set<Event*> visited;
        std::deque<std::shared_ptr<Event>> worklist;

        visited.insert(this);
        worklist.push_back(shared_from_this());

        while (!worklist.empty()) {
            auto current = worklist.front();
            worklist.pop_front();

            current->cleanupSuccessors();

            for (auto& succWeak : current->successors) {
                auto succ = succWeak.lock();
                if (!succ)
                    continue;

                if (visited.insert(succ.get()).second) {
                    // not visited before → schedule traversal
                    worklist.push_back(succ);
                }

                // shift the successor time
                double newTime = succ->getActualTimeFire() + diff;
                succ->setActualTimeFireInternal(newTime);

                if (inPlanning) {
                    succ->setExpectedTimeFireInternal(newTime);
                    // invariants you're aiming for
                    assert(succ->getExpectedTimeFire() == succ->getActualTimeFire());
                }
            }
        }
    }

    void adjustBothPlannedFireTimes(double t)
    {
        if (isManaged)
            throw std::logic_error("Cannot modify after insertion into EventManager");

        assert(t >= this->getExpectedTimeFire() || this->getExpectedTimeFire() - t < 0.1);
        assert(this->getExpectedTimeFire() == this->getActualTimeFire());

        setExpectedTimeFireInternal(t);
        setActualTimeFireInternal(t);

        double diff = t - this->getExpectedTimeFire();
        propagateAllSuccessorsForward(diff, true);
    }

    //////////////////////// CHECK FOR CYCLES AND HELPERS ///////////////////////////////////
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

    bool cleanupPredecessors();
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
    double lastFiredTime = -std::numeric_limits<double>::infinity();

private:
    EventSet eventSet; // ordered by timestamp
    std::unordered_map<std::string, EventSet::iterator> eventById;
    std::unordered_map<int, EventSet> eventsByProcessor;

public:
    EventManager() = default;

    bool insert(const EventPtr& ev)
    {
        // First insertion sanity checks
        if (!ev->isManaged) {

        }

        // Remove old instance if present
        auto it = eventById.find(ev->id);
        if (it != eventById.end()) {
            eraseFromQueueOnly(it);
        }
        auto [setIt, ok] = eventSet.insert(ev);
        if (!ok)
            return false;

        eventById[ev->id] = setIt;
        eventsByProcessor[ev->processor->id].insert(ev);

        ev->isManaged = true;

        return true;
    }
    bool reschedulePure(const std::string& id, double newTime)
    {    //  std::cout<<"reschedule pure "<<id<<" to "<<newTime<<std::endl;

        auto it = eventById.find(id);
        if (it == eventById.end())
            return false;

        EventPtr ev = *(it->second);
        const double oldTime = ev->actualTimeFire;

        if (oldTime == newTime)
            return true;

        // Runtime monotonicity enforcement
        if (newTime < runtimeNow()) {
            std::string error = " Illegal backward runtime reschedule\n event: " + ev->id + "\n"
                + "   requested: " + std::to_string(newTime) + "\n"
                + "   runtimeNow: " + std::to_string(runtimeNow()) + "\n";
            std::cout
                << " Illegal backward runtime reschedule\n"
                << "   event: " << ev->id << "\n"
                << "   requested: " << newTime << "\n"
                << "   runtimeNow: " << runtimeNow() << "\n";

            newTime = runtimeNow(); // OR: return false / throw
            throw new std::runtime_error(error);
        }


        eraseFromQueueOnly(it);
        ev->actualTimeFire = newTime;
        // Reinsert at correct position
        insert(ev);

        return true;
    }

    bool reschedule(const std::string& id, double newTime)
    {
        auto it = eventById.find(id);
        if (it == eventById.end())
            return false;

        EventPtr ev = *(it->second);
        //  std::cout <<" from "<<ev->getActualTimeFire()<<std::endl;;

        const double oldTime = ev->actualTimeFire;

        if (oldTime == newTime)
            return true;

        reschedulePure(id, newTime);

        std::vector<TimeShift> shifts;

        if (newTime > oldTime) {
            ev->pushAllSuccessorsLaterRuntime(shifts);
        } else {
            ev->pullAllSuccessorsEarlierByRuntime(shifts);
        }

        for (auto& s : shifts) {
            //     std::cout << "move "<<s.ev->id<<" from "<<s. ev->getActualTimeFire()<< " to "<<s.newTime<<", ";
            reschedulePure(s.ev->id, s.newTime);
        }
        //std::cout << std::endl;
        return true;
    }

    bool remove(const std::string& id)
    {
        auto it = eventById.find(id);
        if (it == eventById.end())
            return false;

        eraseCompletely(it);
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

    EventPtr earliest() const
    {
        return (*eventSet.begin());
    }

    bool empty() const { return eventSet.empty(); }

    void clear()
    {
        eventSet.clear();
        eventById.clear();
        eventsByProcessor.clear();
    }

    void assertQueueSorted(const std::string& where) const
    {
        if (eventSet.size() < 2)
            return;

        auto it = eventSet.begin();
        auto prev = it;
        ++it;

        for (; it != eventSet.end(); ++it, ++prev) {
            double t_prev = (*prev)->getActualTimeFire();
            double t_curr = (*it)->getActualTimeFire();

            if (t_curr < t_prev) {
                std::cerr << "\n❌ EVENT QUEUE CORRUPTION DETECTED\n";
                std::cerr << "Location: " << where << "\n";
                std::cerr << "Prev event: " << (*prev)->id
                          << " time=" << t_prev << "\n";
                std::cerr << "Curr event: " << (*it)->id
                          << " time=" << t_curr << "\n";
                std::cerr << "Queue dump:\n";

                for (const auto& e : eventSet) {
                    std::cerr << "  " << e->id
                              << " @ " << e->getActualTimeFire()
                              << "\n";
                }

                std::abort(); // hard stop — corruption must not continue
            }
        }
    }

private:
    void eraseFromQueueOnly(std::unordered_map<std::string, EventSet::iterator>::iterator it)
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
    }

    void eraseCompletely(std::unordered_map<std::string, EventSet::iterator>::iterator it)
    {
        EventPtr ev = *(it->second);
        eraseFromQueueOnly(it);

        // now topology change
        ev->removeFromDependencies();
    }
};

struct CompareByRank {

    bool operator()(const vertex_t* a, const vertex_t* b) const
    {
        assert(a->rank != -1);
        assert(b->rank != -1);

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

