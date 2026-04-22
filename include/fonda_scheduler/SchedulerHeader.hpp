//
// Created by kulagins on 11.03.24.
//

#ifndef RESHI_TXT_DYNSCHED_HPP
#define RESHI_TXT_DYNSCHED_HPP

#include "cluster.hpp"
#include "common.hpp"
#include "graph.hpp"
#include "json.hpp"
#include "sp-graph.hpp"

extern Cluster* imaginedCluster;
extern Cluster* imaginedClusterIncorrect;
extern Cluster* actualCluster;

struct EdgeChange {

    edge_t* edge;
    Location newLocation;

    EdgeChange(edge_t* e, Location nl)
        : edge(e)
        , newLocation(nl)
    {
    }
};

class SchedulingResult {
public:
    std::shared_ptr<Processor> processorOfAssignment;
    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    double finishTime;
    double startTime;
    int resultingVar;
    edge_t* edgeToKick;
    double peakMem;
    double shouldBeFreeOnProcessorDuringTask=-1;
    std::vector<EdgeChange> edgesToChangeStatus;

    explicit SchedulingResult(const std::shared_ptr<Processor>& proc)
        : processorOfAssignment(proc ? std::make_shared<Processor>(*proc) : nullptr)
        , modifiedProcs {}
        , finishTime(0)
        , startTime(0)
        , resultingVar(-1)
        , edgeToKick(nullptr)
        , peakMem(0)
    {
    }
};



double calculateSimpleBottomUpRank(vertex_t* task);

double calculateBLCBottomUpRank( vertex_t* task);

std::vector<std::pair<vertex_t*, double>> calculateMMBottomUpRank(graph_t* graphWMems);
double calculateLRank(vertex_t* task);


std::vector<std::shared_ptr<Event>> medih2(graph_t* graph, fonda::Options options, double& runtime);

std::vector<std::pair<vertex_t*, double>> calculateBottomLevels(graph_t* graph, int algoNum);

double howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(const vertex_t* v, const std::shared_ptr<Processor>& pj);


std::vector<std::shared_ptr<Event>>  tentativeAssignment(vertex_t* v, SchedulingResult& result, fonda::Options options);
std::vector<std::shared_ptr<Event>>
tentativeAssignmentHEFT_withCorrectionAndEvents(
    vertex_t* v,
    SchedulingResult& resultIncorrect,
    SchedulingResult& resultCorrect, fonda::Options options);

graph_t* convertToNonMemRepresentation(graph_t* withMemories, std::map<int, int>& noMemToWithMem);

void processIncomingEdges(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc, std::vector<std::shared_ptr<Processor>>& modifiedProcs,
    double& earliestStartingTimeToComputeVertex, std::vector<std::shared_ptr<Event>>& createdEvents, bool forbidLookingIntoPast);
void processIncomingEdgesDisregardingMemorySizes(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs, double& earliestStartingTimeToComputeVertex);

void processIncomingEdgesByNotGoingIntoPast(const vertex_t* v, const bool useDeviatedTimes,
    const std::shared_ptr<Processor>& ourModifiedProc,
    std::vector<std::shared_ptr<Processor>>& modifiedProcs,
    double& earliestStartingTimeToComputeVertex);

void checkIfPendingMemoryCorrect(const std::shared_ptr<Processor>& p);

std::vector<std::shared_ptr<Event>>  bestTentativeAssignment(bool isHeft, vertex_t* vertex, SchedulingResult& result, SchedulingResult& incorrectResultForHeftOnly, fonda::Options options);

void realSurplusOfOutgoingEdges(const vertex_t* v, const std::shared_ptr<Processor>& ourModifiedProc, double& sumOut);

void evictAccordingToBestDecision(int& numberWithEvictedCases, SchedulingResult& bestSchedulingResult, const vertex_t* pVertex, bool isHeft);

//void putChangeOnCluster(vertex_t* vertex, SchedulingResult& schedulingResult, Cluster* cluster, int& numberWithEvictedCases, bool isHeft = false);
void applySchedulingResultToImaginedCluster(vertex_t* vertex, SchedulingResult& schedulingResult, Cluster* ourCluster, int& numberWithEvictedCases,  bool isHeft);

std::shared_ptr<Processor> findProcessorThatHoldsEdge(edge_t* incomingEdge, Cluster* clusterToLookIn);

void handleBiggestEvict(bool real, SchedulingResult& result, const std::vector<EdgeChange>& changedEdgesOne,
    double startTimeForTask, edge_t* biggestPendingEdge, double readyTimeCompute, double readyTimeWrite);
void emulateBiggestEvict2( SchedulingResult& result, const std::vector<EdgeChange>& changedEdgesOne,
    double startTimeForTask, edge_t* biggestPendingEdge, double readyTimeCompute, double readyTimeWrite);

void handleAllEvict(SchedulingResult& result, double timeToWriteAllPending, const std::vector<EdgeChange>& changedEdgesAll,
    double startTimeForAllEvicted, double readyTimeCompute, double readyTimeWrite);
void emulateAllEvict2(SchedulingResult& result, double timeToWriteAllPending, const std::vector<EdgeChange>& changedEdgesAll,
    double startTimeForAllEvicted, double readyTimeCompute, double readyTimeWrite);


double finishTimeWithMemorySwapping(double startTime, double amountToOffload, double timeToRun, const vertex_t* task, const std::shared_ptr<Processor>& p, fonda::Options options);


double uniquePredecessorProcs(vertex_t* vertex);

double CVOfProcessorLoads( const std::map<int, double>& processorLoads);

double idleTimePercentage(const std::map<int, std::vector<std::tuple<double, double>>> &processorsWorkTimes);

double idleTimePercentageOn30s(const std::map<int, std::vector<std::tuple<double, double>>> &processorsWorkTimes);


#endif // RESHI_TXT_DYNSCHED_HPP
