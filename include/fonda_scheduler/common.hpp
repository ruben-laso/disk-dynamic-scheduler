
#ifndef FONDA_SCHED_COMMON_HPP
#define FONDA_SCHED_COMMON_HPP

#include "../../extlibs/memdag/src/graph.hpp"
#include "cluster.hpp"
#include "json.hpp"

#include <regex>
#include <unordered_set>
#include <utility>
#include <queue>

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
    int memoryVariant = -1;

private:
    double expectedTimeFire = -1.0;
    double actualTimeFire = -1.0;

    // true once inserted into EventManager
    bool isManaged = false;
    // Only EventManager may change event times
    friend class EventManager;

    void setActualTimeFireInternal(double t) noexcept
    {
        assert(!isManaged && "Cannot mutate topology of live event");

        actualTimeFire = t;
    }

    void setExpectedTimeFireInternal(double t) noexcept
    {
        // std::cout << "set expected time of "<<this->id<< " to "<<t<<std::endl;
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

    void initialize(const std::vector<std::shared_ptr<Event>>& preds,
        const std::vector<std::weak_ptr<Event>>& succs)
    {
        for (auto& pred : preds) {
            this->addPredecessorInPlanning(pred);
        }
        for (auto& succ : succs) {
            succ.lock()->addPredecessorInPlanning(shared_from_this());
        }
    }

    Event(vertex_t* task, edge_t* edge,
        const eventType type, const std::shared_ptr<Processor>& processor,
        const double expectedTimeFire, const double actualTimeFire,
        const bool isPreemptive, std::string idN,
        const std::vector<std::shared_ptr<Event>>& predecessors = { },
        const std::vector<std::weak_ptr<Event>>& successors = { })
        : id(std::move(idN))
        , task(task)
        , edge(edge)
        , type(type)
        , processor(processor) // shared ownership
        , onlyPreemptive(isPreemptive)
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
        bool isPreemptive, const std::string& id)
    {
        return std::make_shared<Event>(task, edge, type, processor, expectedTimeFire,
            actualTimeFire, isPreemptive, id, predecessors, successors);
    }

    static std::shared_ptr<Event> createEvent(vertex_t* task, edge_t* edge,
        eventType type, const std::shared_ptr<Processor>& processor,
        double expectedTimeFire, double actualTimeFire,
        bool isPreemptive, const std::string& id)
    {
        return std::make_shared<Event>(task, edge, type, processor, expectedTimeFire,
            actualTimeFire, isPreemptive, id);
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

    std::shared_ptr<Event> getCorrespondingStart()
    {
        assert(this->isFinish());
        for (auto predecessor : this->getPredecessors()) {
            if (predecessor->isStart()) {
                if (this->task != nullptr) {
                    if (predecessor->task != nullptr && this->task->name == predecessor->task->name) {
                        return predecessor;
                    }
                } else {
                    if (predecessor->edge != nullptr && buildEdgeName(this->edge) == buildEdgeName(predecessor->edge)) {
                        return predecessor;
                    }
                }
            }
        }
        throw new std::runtime_error("no corresponding start event found for " + this->id);
    }

    std::shared_ptr<Event> getCorrespondingFinish()
    {
        assert(this->isStart());
        for (auto successpr : this->getSuccessors()) {
            auto succ = successpr.lock();
            if (succ->isFinish()) {
                if (this->task != nullptr) {
                    if (succ->task != nullptr && this->task->name == succ->task->name) {
                        return succ;
                    }
                } else {
                    if (succ->edge != nullptr && buildEdgeName(this->edge) == buildEdgeName(succ->edge)) {
                        return succ;
                    }
                }
            }
        }
        throw new std::runtime_error("no corresponding finish event found for " + this->id);
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
        double t = 0;
        for (auto p : predecessors) {
            t = std::max(t, plannedTime(p, shifts));
        }
        // std::cout << " is "<<t<<std::endl;;
        return t;
    }

    double earliestAllowedTimeFast(const std::unordered_map<Event*, double>& fastShifts)
    {
        double t = 0;
        for (auto p : predecessors) {
            // Look in the map first (O(1) lookup)
            auto it = fastShifts.find(p.get());

            // If it's in the map, use the new shifted time; otherwise, use the current time
            double pTime = (it != fastShifts.end()) ? it->second : p->getActualTimeFire();

            t = std::max(t, pTime);
        }
        return t;
    }

    double getOldOrNewTime(Event* ev, const std::unordered_map<Event*, double>& fastShifts)
    {
        auto it = fastShifts.find(ev);
        if (it != fastShifts.end()) {
            return it->second; // Return the new proposed time
        }
        return ev->getActualTimeFire(); // Return the original time
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

    void pullAllSuccessorsEarlierByRuntime(std::vector<TimeShift>& shifts)
    {

        std::unordered_set<Event*> visited;
        propagatePullEarlierRuntime(shifts);
    }

    void propagatePullEarlierRuntime(std::vector<TimeShift>& shifts)
    {
        std::unordered_set<Event*> visited;
        std::deque<std::shared_ptr<Event>> worklist;

        // We start with the successors of the event that just moved earlier
        for (auto& sw : successors) {
            if (auto s = sw.lock())
                worklist.push_back(s);
        }

        while (!worklist.empty()) {
            auto s = worklist.front();
            worklist.pop_front();
            if (visited.count(s.get()))
                continue;

            double oldTime = s->getActualTimeFire();

            if (s->isStart()) {
                double limiting = s->earliestAllowedTime(shifts);
                if (oldTime > limiting) {
                    // We can pull it!
                    shifts.push_back({ s.get(), limiting });
                    visited.insert(s.get());

                    for (auto& sw : s->successors) {
                        if (auto next = sw.lock())
                            worklist.push_back(next);
                    }
                }
            } else {
                // s is finish
                assert(s->getPredecessors().size() == 1);
                auto startEv = s->getCorrespondingStart();
                auto fireTimeOfStart = plannedTime(startEv, shifts);
                double durationOriginal = s->getActualTimeFire() - startEv->getActualTimeFire();
                shifts.push_back({ s.get(), fireTimeOfStart + durationOriginal });

                for (auto& sw : s->successors) {
                    if (auto next = sw.lock())
                        worklist.push_back(next);
                }
            }
        }
    }

    void enforceSuccessorConstraints(std::vector<TimeShift>& outShifts)
    {
        std::deque<std::shared_ptr<Event>> worklist;
        // Map of Event* -> NewTime (O(1) lookups)
        std::unordered_map<Event*, double> fastShifts;

        // 1. Initial Worklist seed: Check everyone who depends on 'this'
        for (auto& sw : successors) {
            if (auto s = sw.lock())
                worklist.push_back(s);
        }

        while (!worklist.empty()) {
            auto current = worklist.front();
            worklist.pop_front();

            // Helper to find the "current" time in this transaction
            double currentTime = getOldOrNewTime(current.get(), fastShifts);

            if (current->isStart()) {
                // RULE: Start must wait for predecessors
                double limiting = current->earliestAllowedTimeFast(fastShifts);

                if (std::abs(limiting - currentTime) > 1e-7) {
                    double diff = limiting - current->getActualTimeFire();
                    fastShifts[current.get()] = limiting;

                    // ATOMIC MOVE: If Start moves, Finish MUST move by same diff
                    auto f = current->getCorrespondingFinish();
                    if (f) {
                        double newFinTime = f->getActualTimeFire() + diff;
                        fastShifts[f.get()] = newFinTime;

                        // Only queue successors of the Finish
                        for (auto& sw : f->getSuccessors()) {
                            if (auto next = sw.lock())
                                worklist.push_back(next);
                        }
                    }
                }
            } else if (current->isFinish()) {
                // RULE: Finish must wait for data predecessors AND its own Start
                auto s = current->getCorrespondingStart();
                double sTime = getOldOrNewTime(s.get(), fastShifts);
                double duration = current->getActualTimeFire() - s->getActualTimeFire();

                // It must be at least (Start + Duration) AND >= any other data dependencies
                double limiting = std::max(current->earliestAllowedTimeFast(fastShifts), sTime + duration);

                double difference = limiting - currentTime;
                if (std::abs(difference) > 1e-7) {
                    fastShifts[current.get()] = limiting;
                    // Check downstream tasks
                    for (auto& sw : current->getSuccessors()) {
                        if (auto next = sw.lock())
                            worklist.push_back(next);
                    }
                }
            } else {
                // RULE: Generic I/O or other events
                double limiting = current->earliestAllowedTimeFast(fastShifts);
                double x = limiting - currentTime;
                if (std::abs(x) > 1e-7) {
                    fastShifts[current.get()] = limiting;
                    for (auto& sw : current->getSuccessors()) {
                        if (auto next = sw.lock())
                            worklist.push_back(next);
                    }
                }
            }
        }

        // 2. Final Step: Convert our fast map to the output vector
        for (auto const& [ev, time] : fastShifts) {
            outShifts.push_back({ ev, time });
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
        // 1. structural edge, no time change happens here
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

        // 4. and shift all successors
        propagateAllSuccessorsForwardInPlanning(diff);
    }

    void propagateAllSuccessorsForwardInPlanning(double diff)
    {
        // std::cout << "propapgaget "<<diff<<" from task "<<this->id<<std::endl;;
        if (diff <= 0.0)
            return;

        this->cleanupSuccessors();

        // visited prevents cycles and tracks by how much they have already been shifted, so we only re-visit if we have to shift more than before
        std::unordered_map<Event*, double> visitedShift;
        std::deque<std::pair<std::shared_ptr<Event>, double>> worklist;

        visitedShift[this] = diff;
        worklist.push_back({ shared_from_this(), diff });

        while (!worklist.empty()) {
            auto currentPair = worklist.front();
            worklist.pop_front();
            // a start always has only one successor, its own finish

            auto current = currentPair.first;
            double currentDiff = currentPair.second;
            current->cleanupSuccessors();

            if (current->isFinish()) {

                for (auto& succWeak : current->successors) {
                    auto succ = succWeak.lock();
                    if (!succ)
                        continue;
                    assert(succ->isStart());

                    double oldSuccTime = succ->getVisibleTimeFireForPlanning();
                    double newSuccTime = oldSuccTime;

                    double oldCurrTime = current->getVisibleTimeFireForPlanning();
                    // shifting starts happens only if we are the limiting predecesoor
                    if (oldCurrTime > oldSuccTime) {
                        // current is the limiter, move the start successor forward to our time
                        newSuccTime = oldSuccTime + (oldCurrTime - oldSuccTime);
                        double localDiff = newSuccTime - oldSuccTime;

                        succ->setActualTimeFireInternal(newSuccTime);
                        succ->setExpectedTimeFireInternal(newSuccTime);
                        assert(succ->getExpectedTimeFire() == succ->getActualTimeFire());

                        if (visitedShift[succ.get()] < localDiff) {
                            visitedShift[succ.get()] = localDiff;
                            worklist.push_back({ succ, localDiff });
                        }
                    }
                }
            } else {
                // curr is start, can have only one finish - its own; unless we are still planning and have not yet added the finish. But no 2 or more finishes
                assert(current->getSuccessors().size() <= 1);
                if (current->getSuccessors().size() > 0) {
                    std::shared_ptr<Event> succFinish = current->getCorrespondingFinish();

                    double oldFinishTime = succFinish->getVisibleTimeFireForPlanning();
                    double newFinishTime = oldFinishTime + currentDiff;

                    succFinish->setActualTimeFireInternal(newFinishTime);
                    succFinish->setExpectedTimeFireInternal(newFinishTime);
                    assert(succFinish->getExpectedTimeFire() == succFinish->getActualTimeFire());

                    if (visitedShift[succFinish.get()] < currentDiff) {
                        visitedShift[succFinish.get()] = currentDiff;
                        worklist.push_back({ succFinish, currentDiff });
                    }
                }
            }
        }
    }
    void updateOrAddShift(std::vector<TimeShift>& shifts, Event* ev, double newTime)
    {
        //  std::cout << "update shift "<<ev->id<<" to "<<newTime<<std::endl;
        for (auto& s : shifts) {
            if (s.ev == ev) {
                s.newTime = newTime;
                return;
            }
        }
        shifts.push_back({ ev, newTime });
    }

    void propagateAllSuccessorsForwardInExecution(double initialDiff, std::unordered_map<Event*, double>& fastShifts)
    {
        if (initialDiff <= 1e-7)
            return;

        // We use visitedShift to track the max displacement applied to each event
        std::unordered_map<Event*, double> visitedShift;
        std::deque<std::shared_ptr<Event>> worklist;

        visitedShift[this] = initialDiff;

        for (auto& sw : successors) {
            if (auto s = sw.lock())
                worklist.push_back(s);
        }

        while (!worklist.empty()) {
            auto current = worklist.front();
            worklist.pop_front();

            double oldFireTime = current->getActualTimeFire();
            double newLimitingTime = current->earliestAllowedTimeFast(fastShifts);

            if (current->isFinish()) {
                auto startEv = current->getCorrespondingStart();
                double startDiff = visitedShift[startEv.get()];

                if (startDiff > visitedShift[current.get()]) {
                    double newFinishTime = oldFireTime + startDiff;
                    fastShifts[current.get()] = newFinishTime; // O(1)
                    visitedShift[current.get()] = startDiff;

                    for (auto& sw : current->successors) {
                        if (auto next = sw.lock())
                            worklist.push_back(next);
                    }
                }
            } else {
                if (newLimitingTime > oldFireTime + 1e-7) {
                    double localDiff = newLimitingTime - oldFireTime;
                    if (localDiff > visitedShift[current.get()]) {
                        fastShifts[current.get()] = newLimitingTime; // O(1)
                        visitedShift[current.get()] = localDiff;

                        for (auto& sw : current->successors) {
                            if (auto next = sw.lock())
                                worklist.push_back(next);
                        }
                    }
                }
            }
        }
    }

    void propagatePullEarlierRuntimeFast(std::unordered_map<Event*, double>& fastShifts)
    {
        std::unordered_set<Event*> visited;
        std::deque<std::shared_ptr<Event>> worklist;

        // Start with the successors of the event that just moved earlier
        for (auto& sw : successors) {
            if (auto s = sw.lock())
                worklist.push_back(s);
        }

        while (!worklist.empty()) {
            auto s = worklist.front();
            worklist.pop_front();

            // In pulling, we can use a simple visited set because we are
            // pulling to the "Earliest" possible, which is usually
            // determined by the 'latest' parent.
            if (visited.count(s.get()))
                continue;

            double oldTime = s->getActualTimeFire();

            if (s->isStart()) {
                // How early CAN it fire based on the new positions of predecessors?
                double limiting = s->earliestAllowedTimeFast(fastShifts);

                // If the current scheduled time is later than the limiting time, pull it back
                if (oldTime > limiting) {
                    fastShifts[s.get()] = limiting;
                    visited.insert(s.get());

                    for (auto& sw : s->successors) {
                        if (auto next = sw.lock())
                            worklist.push_back(next);
                    }
                }
            } else {
                // It's a Finish event
                assert(s->getPredecessors().size() == 1);
                auto startEv = s->getCorrespondingStart();

                // Find the NEW time of the start event from our map
                auto it = fastShifts.find(startEv.get());
                double fireTimeOfStart = (it != fastShifts.end()) ? it->second : startEv->getActualTimeFire();

                double durationOriginal = s->getActualTimeFire() - startEv->getActualTimeFire();
                double newFinishTime = fireTimeOfStart + durationOriginal;

                if (newFinishTime < oldTime - 1e-7) {
                    fastShifts[s.get()] = newFinishTime;
                    visited.insert(s.get());

                    for (auto& sw : s->successors) {
                        if (auto next = sw.lock())
                            worklist.push_back(next);
                    }
                }
            }
        }
    }

    void adjustBothPlannedFireTimes(double newTime)
    {
        if (isManaged)
            throw std::logic_error("Cannot modify after insertion into EventManager");

        assert(newTime >= this->getExpectedTimeFire() || this->getExpectedTimeFire() - newTime < 0.1);
        assert(this->getExpectedTimeFire() == this->getActualTimeFire());

        double oldTime = this->getExpectedTimeFire();
        setExpectedTimeFireInternal(newTime);
        setActualTimeFireInternal(newTime);

        double diff = newTime - oldTime;
        propagateAllSuccessorsForwardInPlanning(diff);
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
    void scheduleTasksUntilFoundForThisProc();

    void fireTaskFinish();

    void fireReadStart();

    void fireReadFinish();

    void fireWriteStart();

    void fireWriteFinish();

    void removeFromSuccessors();

    void removeFromPredecessors();

    void removeFromDependencies();

    bool cleanupPredecessors();

    void printEventDetailed() const
    {

        std::cout << "--- Event [" << id << "] ---\n";

        // Type and State
        std::cout
            << " | Status: " << (isDone ? "DONE" : "PENDING")
            << " | Fired: " << timesFired << "x\n";

        // Pointers (Task, Edge, Processor)
        std::cout << " | Processor: " << processor->id << "\n";

        // Timing (Formatted to 4 decimal places)
        std::cout << "  Expected Fire: " << (expectedTimeFire < 0 ? "N/A" : std::to_string(expectedTimeFire)) << "\n";
        std::cout << "  Actual Fire:   " << (actualTimeFire < 0 ? "N/A" : std::to_string(actualTimeFire)) << "\n";

        // Relationships (Predecessors and Successors)
        std::cout << "  Predecessors: [ ";
        for (const auto& swp : predecessors) {
            std::cout << swp->id << " ";
        }
        std::cout << "]\n";

        std::cout << "  Successors:   [ ";
        for (const auto& swp : successors) {
            if (auto s = swp.lock()) { // Lock weak_ptr to access the object
                std::cout << s->id << " ";
            }
        }
        std::cout << "]\n";

        // Internal Flags
        std::cout << "  Flags: [Managed: " << (isManaged ? "Yes" : "No")
                  << "] [Preemptive Only: " << (onlyPreemptive ? "Yes" : "No") << "]\n";

        std::cout << "--------------------------" << std::endl;
    }

    void printEventShort() const
    {
        std::cout << id << ": ";

        std::cout << " Processor: " << processor->id;
        std::cout << "  Expected Fire: " << expectedTimeFire;
        //std::cout << "  Actual Fire:   " << actualTimeFire;
        std::cout << std::endl;
    }
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
    int deviationVariant = 1; // default is no devations

private:
    EventSet eventSet; // ordered by timestamp
    std::unordered_map<std::string, EventSet::iterator> eventById;
    std::unordered_map<int, EventSet> eventsByProcessor;

public:
    EventManager() = default;

    unsigned int size()
    {
        return eventSet.size();
    }

    bool insert(const EventPtr& ev)
    {
        bool firstInsertion = !ev->isManaged;

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

        // First insertion sanity checks
        if (firstInsertion) {
            // PASS 1: absolute repair, cannot be needed without deviations
            if (deviationVariant != 1  ) {
                if (ev->isStart()) {

                }
                else {
                    auto ourStart = ev->getCorrespondingStart();
                    // inserting finish, so start is already inserted. we can try to move the pair of them
                    for (auto predecessor : ourStart->predecessors) {
                        if (predecessor->isManaged && predecessor->getActualTimeFire() != predecessor->getExpectedTimeFire() &&
                            predecessor->getActualTimeFire() != ourStart->getActualTimeFire()) {
                            std::vector<TimeShift> repair;
                            predecessor->enforceSuccessorConstraints(repair);
                            // Apply any repairs
                            for (auto& s : repair) {
                                this->reschedulePure(s.ev->id, s.newTime);
                            }
                        }
                    }
                }

            }
        }

        return true;
    }
    bool reschedulePure(const std::string& id, double newTime)
    {
        auto it = eventById.find(id);
        if (it == eventById.end())
            return false;

        EventPtr ev = *(it->second);
        const double oldTime = ev->actualTimeFire;

        if (oldTime == newTime)
            return true;

        //std::cout << "reschedule pure " << id << " to " << newTime << " from " << ev->getActualTimeFire() << std::endl;
        // Runtime monotonicity enforcement
        if (newTime < runtimeNow()) { //&& std::abs(newTime - runtimeNow()) > 1e-7) {
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
        ev->cleanupSuccessors();
        const double oldTime = ev->actualTimeFire;

        // 1. Initial constraint check
        double earliest = ev->earliestAllowedTimeFast({ });
        if (newTime < earliest)
            newTime = earliest;

        if (std::abs(newTime - oldTime) < 1e-7)
            return true;

        // Use a map for all propagation calculations
        std::unordered_map<Event*, double> fastShifts;
        fastShifts[ev.get()] = newTime;

        double diff = newTime - oldTime;
        EventPtr eventToPropagateFrom = ev;

        // 2. Handle Start-Finish lockstep immediately in the map
        if (ev->isStart()) {
            auto myFinish = ev->getCorrespondingFinish();
            if (myFinish) {
                double finishOldTime = myFinish->getActualTimeFire();
                double finishNewTime = finishOldTime + diff;
                fastShifts[myFinish.get()] = finishNewTime;
                eventToPropagateFrom = myFinish;
            }
        }

        // 3. Propagate
        if (diff > 0) {
            eventToPropagateFrom->propagateAllSuccessorsForwardInExecution(diff, fastShifts);
        } else {
            // You should refactor propagatePullEarlierRuntime to use the map similarly
            eventToPropagateFrom->propagatePullEarlierRuntimeFast(fastShifts);
        }

        // 4. Batch Update (The Performance Winner)
        for (auto& s : fastShifts) {
            //std::cout << "shifts: move "<<s.first->id<<" from "<<s. first->getActualTimeFire()<< " to "<<s.second<<", ";
            reschedulePure(s.first->id, s.second);
        }

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

    void queueDump()
    {
        for (const auto& e : eventSet) {
            std::cout << "  " << e->id
                      << " @ " << e->getActualTimeFire()
                      << "\n";
        }
    }

    void verifySchedule(std::unordered_map<int, vertex_t*> allVertices)
    {
        // std::cout << "--- Starting Event-Based Verification ---" << std::endl;
        bool violationFound = false;

        for (auto pain : allVertices) {
            auto* v = pain.second;
            // 1. Recover the Start and Finish events for this vertex
            auto startEv = this->find(v->name + "-s");
            auto finishEv = this->find(v->name + "-f");

            if (startEv == nullptr || finishEv == nullptr) {
                std::cerr << "[ERROR] Missing events for vertex: " << v->name << std::endl;
                violationFound = true;
                continue;
            }

            double vStart = startEv->getVisibleTimeFireForPlanning();
            double vFinish = finishEv->getVisibleTimeFireForPlanning();

            // 2. Check Data Dependencies
            for (auto* in_edge : v->in_edges) {
                auto predFinishEv = this->find(in_edge->tail->name + "-f");
                if (predFinishEv != nullptr) {
                    double predFinish = predFinishEv->getVisibleTimeFireForPlanning();
                    if (vStart < predFinish - 1e-7) {
                        std::cerr << "[ERROR] Data Dependency Violation: " << v->name
                                  << " starts at " << vStart << " but predecessor "
                                  << in_edge->tail->name << " finishes at " << predFinish << std::endl;
                        violationFound = true;
                    }
                }
            }

            // 3. Check Processor Sequentiality (The "Last Event" Chain)
            // Find if this Start Event has a predecessor on the same processor
            for (auto const& pred : startEv->getPredecessors()) {

                if (pred && pred->processor->id == startEv->processor->id) {
                    // This is a compute-to-compute or write-to-compute dependency on the same proc
                    double predTime = pred->getVisibleTimeFireForPlanning();
                    if (vStart < predTime - 1e-7) {
                        std::cerr << "[ERROR] Processor Resource Violation: " << v->name
                                  << " starts at " << vStart << " before same-processor event "
                                  << pred->id << " finished at " << predTime << std::endl;
                        violationFound = true;
                    }
                }
            }
        }

        if (!violationFound) {
            //  std::cout << "Verification Successful: All event timings are consistent." << std::endl;
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


struct PriorityRankComparator {
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
   // std::set<vertex_t*, CompareByRank> readyTasks;
    std::priority_queue<
        vertex_t*,
        std::vector<vertex_t*>,
       PriorityRankComparator> readyTasks;
};

std::shared_ptr<Event> findTaskStart(const std::vector<std::shared_ptr<Event>>& someEvents);

std::shared_ptr<Event> findLatest(const std::vector<std::shared_ptr<Event>>& someEvents);

#endif
