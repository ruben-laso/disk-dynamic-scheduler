#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>

#include <csignal>

#include "csv/single_include/csv2/csv2.hpp"
#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"

#include "fonda_scheduler/io/graphWeightsBuilder.hpp"
#include "fonda_scheduler/options.hpp"
#include "fonda_scheduler/utils.hpp"
#include "memdag/src/graph.hpp"

#include <complex>
bool checkCycleInGraph(graph_t* dag);
/*
 *
 *  Call: memoryMultiplicator speedMultiplicator readWritePenalty offloadPenalty,workflow, inputSize, algorithmNumber, isBaseline, root directory, machines file, number of deviation function, yes/no for preemptive writes
 *  1000000, 100, 1, 0.001, true ../ machines.csv 1
 *
 * algos with  memory awareness: 1 - HEFT-BL, 2- HEFT-BL, 3- HEFT-MM
 * HEFT (no memory awareness) : yes at isBaseline, algoNum is irrelevant then
 *  deviations :  1 - normal deviation function around historical value with 10% deviation
 *  2 - normal deviation function around historical value with 50% deviation
 *  3 - no deviation
 *  4 - everything x2
 *  5 - 30% deviation
 */
// 1 1 0.1 0.01 debug 10 1 no ../ machines_debug.csv 3 -> gives evictions
// 1000000 100 100 0.1 chipseq_200 41366257414 1 yes ../ machines.csv 3
// 1000000 100 1 0.001 methylseq_200 110641579976 1 yes ../ machines.csv
// 100000000 100 1 1 methylseq_2000 110641579976 1 yes ../ machines.csv
// 1000000 100 1 1 bacass 3637252230 1 yes ../
// 100000000 100 1 1 chipseq 3793245764 1 yes ../ machines.csv
// 1 1 1 1 debug 10 1 yes ../ machines_debug.csv
// 1000000 100 1 0.001 chipseq_1000 3793245764 1 no ../ machines.csv -> für beide gültig
// 100000000 100 1 1 chipseq_2000 3793245764 1 yes ../ machines.csv
// 1000000 100 1 0.001 eager_2000 25705994498 1 no ../ machines.csv
// 100000000 100 1 0.001 eager 8330435694 1 no ../ machines.csv 3
//-m 100000000 -s 100 -r 10 -w atacseq -i 14091675276 -a heft-bl -p ../ -d 1 -q 3 -S -f input/machines.csv -E -M

//-m 100000000 -s 100 -r 10 -w eager -i 19132169434 -a heft-mm -p ../ -d 1 -q 3 -S -f input/machines.csv
int main(const int argc, char* argv[])
{
    // for(int i = 1; i < argc; i++)
    //     printf("%s\n", argv[i]);

    // std::cout<<std::endl;

    fonda::Options options = fonda::parseOptions(argc, argv);
    std::cout << "algo_nr " << options.algoNumber << " " << options.workflowName << " " << "input_size " << options.inputSize << " ";

    const auto workflow_rows = fonda_scheduler::loadTracesFile(options.pathPrefix + options.tracesFile);

    imaginedCluster = Fonda::buildClusterFromCsv(options.pathPrefix + options.machinesFile, options);
    imaginedClusterIncorrect = Fonda::buildClusterFromCsv(options.pathPrefix + options.machinesFile, options);

    // With deviations
    actualCluster = Fonda::buildClusterFromCsv(options.pathPrefix + options.machinesFile, options);

    // actualCluster->printProcessors();

    double biggestMem = imaginedCluster->getMemBiggestFreeProcessor()->getMemorySize();

    // QUESTION: Why not reading directly from the options.workflowName?
    std::string filename;
    if (options.workflowName.rfind("/home", 0) == 0 || options.workflowName.rfind("/work", 0) == 0) {
        filename = options.workflowName.substr(0, options.workflowName.find("//") + 1) + options.workflowName.substr(options.workflowName.find("//") + 2, options.workflowName.size());
    } else {
        filename = options.pathPrefix + "input/";
        // string suffix = "00";
        //  bool isGenerated = workflowName.substr(workflowName.size() - suffix.size()) == suffix;
        // if (isGenerated) {
        filename += "generated/"; //+filename;
        //  }
        filename += options.workflowName;

        if (const size_t pos = filename.find(".dot"); pos == std::string::npos) {
            filename += ".dot";
        }
    }
    graph_t* graphMemTopology;
    if (filename.find("dag") != std::string::npos) {
        graphMemTopology = read_dot_graph(filename.c_str(), "weight", "C", "M", "swaps");
        checkForZeroMemories(graphMemTopology);
        //  print_graph_to_cout(graphMemTopology);
    } else {
        graphMemTopology = read_dot_graph(filename.c_str(), nullptr, nullptr, nullptr, "swaps");
        checkForZeroMemories(graphMemTopology);

        const auto i1 = options.workflowName.find("//");
        options.workflowName = i1 == std::string::npos ? options.workflowName : options.workflowName.substr(i1 + 2, options.workflowName.size());
        // remove the size from name: atacseq_2000 -> atacseq
        const auto n4 = options.workflowName.find('_');
        options.workflowName = options.workflowName.substr(0, n4);

        // 10, 100                                                               memShorteningDivision, ioShorteningCoef
        Fonda::fillGraphWeightsFromExternalSource(graphMemTopology, workflow_rows, imaginedCluster, 1, 1, options);
        // print_graph_to_cout(graphMemTopology);
    }
    if (
        checkCycleInGraph(graphMemTopology)) {
        return -1;
    };

    if (options.autoChoice) {
        const auto start = std::chrono::system_clock::now();
        enforce_single_source_and_target_with_minimal_weights(graphMemTopology);
        compute_bottom_and_top_levels(graphMemTopology);
        printGraphAndWeightProperties(graphMemTopology, options.deviationModel != 1);
        const auto end = std::chrono::system_clock::now();
        auto elapsed_seconds = end - start;
        long count = elapsed_seconds.count();
        std::cout<< count<<std::endl;

    } else {
        if (options.scaleToFit) {
            fonda_scheduler::scaleToFit(graphMemTopology, biggestMem);
        }

        std::cout << std::setprecision(15);
        std::clog << std::setprecision(15);

        double runtimeDynamic = 0;
        for (vertex_t* u = graphMemTopology->first_vertex; u; u = u->next) {
            assert(u->status == Unscheduled);
        }
        double onlineMakespan = 0;

        assert(options.algoNumber != 0); // never use just heft, it is attached to each execution as a third option now.
        onlineMakespan = onlineMedih(graphMemTopology, actualCluster, options, runtimeDynamic);

        for (vertex_t* u = graphMemTopology->first_vertex; u; u = u->next) {
            assert(u->status == Finished);
        }
        for (const edge_t* e = graphMemTopology->first_edge; e; e = e->next) {
            if (e->accountedFor) {
                //   std::cout<<" accounted for!  ";print_edge(e);
            } else {
                if (e->locations.size() > 0 && e->locations.at(0).locationType == LocationType::OnProcessor && e->locations.at(0).processorId == e->head->assignedProcessorId) {
                } else {
                    std::cout << " unaccounted for ";
                    print_edge(e);
                    if (e->locations.at(0).locationType == LocationType::OnDisk) {
                        std::cout << "on disk" << std::endl;
                    }
                    if (e->locations.at(0).locationType == LocationType::OnProcessor) {
                        std::cout << "on proc " << e->locations.at(0).processorId.value() << std::endl;
                    }
                    if (e->locations.at(0).locationType == LocationType::Nowhere) {
                        std::cout << "on nowhere " << std::endl;
                    }

                    std::cout << e->locations.size() << std::endl;
                }
            }
        }
        events.clear();
        std::cout << " duration_of_algorithm " << runtimeDynamic << " "; // << endl;
        std::cout << "makespan_online " << onlineMakespan << "\t";

        delete actualCluster;
        actualCluster = Fonda::buildClusterFromCsv(options.pathPrefix + options.machinesFile, options);
        timeInSystem = 0;
        events.clear();
        clearGraph(graphMemTopology);
        enforce_single_source_and_target_with_minimal_weights(graphMemTopology);
        compute_bottom_and_top_levels(graphMemTopology);

        double runtimOffline = 0;
        double offline = correctOflineMedihWithEvents(graphMemTopology, actualCluster, options, runtimOffline);

        std::cout << " duration_of_algorithm " << runtimOffline << " "; // << endl;
        std::cout << "makespan_offline " << offline << ' ';
        delete graphMemTopology;
        delete imaginedCluster;
        delete actualCluster;
    }
}

static bool hasCycleFrom(vertex_t* task, std::unordered_set<std::string>& visited, std::unordered_set<std::string>& recStack,
    const bool checkPredecessors)
{
    if (recStack.find(task->name) != recStack.end()) {
        std::cout << "Cycle detected at event: " << task->name << '\n';
        return true; // Cycle detected!
    }

    if (visited.find(task->name) != visited.end()) {
        return false; // Already checked, no cycle found
    }

    visited.insert(task->name);
    recStack.insert(task->name);

    // Choose to check either predecessors or successors
    if (checkPredecessors) {
        for (const auto& predecessor : task->in_edges) {
            if (hasCycleFrom(predecessor->tail, visited, recStack, checkPredecessors)) {
                std::cout << "found cycle from " << predecessor->tail->name << " to " << task->name << std::endl;
                return true;
            }
        }
    } else {
        for (const auto& successor : task->out_edges) {
            if (hasCycleFrom(successor->head, visited, recStack, checkPredecessors)) {
                std::cout << "found cycle from " << task->name << " to " << successor->head->name << std::endl;
                return true;
            }
        }
    }

    recStack.erase(task->name); // Remove from recursion stack after processing
    return false;
}

bool checkCycleInGraph(graph_t* dag)
{
    std::unordered_set<std::string> visited;
    std::unordered_set<std::string> recStack; // Tracks the current path

    return hasCycleFrom(dag->first_vertex, visited, recStack, true) || hasCycleFrom(dag->first_vertex, visited, recStack, false);
}
