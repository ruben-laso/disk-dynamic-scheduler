#include "ClusterFactory.cpp"
#include "WorkflowFactory.cpp"
#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include <gtest/gtest.h>

TEST(SchedulerLogicTest, DoesNotExceedMemoryLimit)
{

    graph_t* dag = WorkflowFactory::CreateChain(3, 100.0, 49.9, 10.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 50.0, 1.0);
    double runtime = 0;

    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, 1, runtime);
        ASSERT_EQ(events.size(), 6); // only task start and finish events for 3 tasks
        for (auto e : eventsMedih) {
            e->printEventShort();
        }

        ASSERT_EQ(eventsMedih.at(1)->getExpectedTimeFire(), eventsMedih.at(0)->getExpectedTimeFire() + 100.0); // first task finishes after 100
        ASSERT_EQ(eventsMedih.at(3)->getExpectedTimeFire(), eventsMedih.at(2)->getExpectedTimeFire() + 100.0); // second task finishes after 100
        ASSERT_EQ(eventsMedih.at(5)->getExpectedTimeFire(), eventsMedih.at(4)->getExpectedTimeFire() + 100.0); // third task finishes after 100
    });

    free_graph(dag);
}

TEST(SchedulerLogicTest, ExceedsMemory)
{

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides(100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateBottleneckCluster();

    double runtime = 0;
    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, 1, runtime);
        ASSERT_EQ(eventsMedih.size(), 16);
    });

    free_graph(dag);
}

TEST(BaselineTest, ExceedsMemory)
{

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides(100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateBottleneckCluster();

    double runtime = 0;
    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, 1, runtime);
        ASSERT_EQ(eventsMedih.size(), 16);
        for (auto e : eventsMedih) {
            e->printEventShort();
        }
        std::cout << "-------------------\n";
        imaginedCluster = ClusterFactory::CreateBottleneckCluster();
        imaginedClusterIncorrect = ClusterFactory::CreateBottleneckCluster();

        std::vector<std::shared_ptr<Event>> eventsHeft = medih2(dag, 0, runtime);

        for (auto e : eventsHeft) {
            e->printEventShort();
        }
    });

    free_graph(dag);
}

TEST(TestWithRuntime, ExceedsMemory)
{

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides(100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateMidBottleneckCluster();
    actualCluster = ClusterFactory::CreateMidBottleneckCluster();

    double runtime = 0;
    EXPECT_NO_THROW({
        // std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, 1, runtime);
        double runtimeHeft;
        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, 1, 3, runtimeHeft);

        clearGraph(dag);
        // dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
        imaginedCluster = ClusterFactory::CreateBottleneckCluster();
        imaginedClusterIncorrect = ClusterFactory::CreateBottleneckCluster();
        timeInSystem = 0;

        double msHeft = correctOflineMedihWithEvents(dag, actualCluster, 0, 3, runtimeHeft);
    });

    free_graph(dag);
}

TEST(TestWithDeviations, SpreadForkNoDeviations)
{
    graph_t* dag = WorkflowFactory::CreateFork(4, 100.0, 100.0, 100.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
    actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

    EXPECT_NO_THROW({
        double runtime;
        auto eventsBL = medih2(dag, 2, runtime);

        ASSERT_EQ(eventsBL.size(), 14); //
        ASSERT_EQ(eventsBL.at(2)->processor->id, 0);
        ASSERT_EQ(eventsBL.at(4)->processor->id, 0);
        ASSERT_EQ(eventsBL.at(10)->processor->id, 1); // last task and his read are on processor 1
        ASSERT_EQ(eventsBL.at(11)->processor->id, 1);
        ASSERT_EQ(eventsBL.at(12)->processor->id, 1);
        ASSERT_EQ(eventsBL.at(13)->processor->id, 1);
    });

    free_graph(dag);
}

TEST(TestWithDeviations, ChainSpreadDueToDeviation)
{
    graph_t* dag = WorkflowFactory::CreateFork(4, 100.0, 100.0, 50.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
    actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

    EXPECT_NO_THROW({
        double runtime;

        // no deviations, all on one processor except for 3
        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, 2, 1, runtime);
        ASSERT_EQ(msOffline, 40); // 3 times runtime of 100/10 = 10 (no memory effects, no communication)

        dag = WorkflowFactory::CreateFork(4, 100.0, 100.0, 50.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        timeInSystem = 0;
        events.clear();

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(1)->factorForRealExecution = 10;
        dag->vertices_by_id.at(0)->factorForRealExecution = 1;
        dag->vertices_by_id.at(2)->factorForRealExecution = 1;
        dag->vertices_by_id.at(3)->factorForRealExecution = 1;
        dag->vertices_by_id.at(4)->factorForRealExecution = 1;

        dag->first_edge->factorForRealExecution = 1;
        dag->first_edge->next->factorForRealExecution = 1;
        dag->first_edge->next->next->factorForRealExecution = 1;
        dag->first_edge->next->next->next->factorForRealExecution = 1;

        // we still put all tasks on the same processor and wait
        msOffline = correctOflineMedihWithEvents(dag, actualCluster, 2, 1, runtime);
        ASSERT_EQ(msOffline, 130); //

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateFork(4, 100.0, 100.0, 50.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        timeInSystem = 0;

        dag->vertices_by_id.at(1)->factorForRealExecution = 10;
        dag->vertices_by_id.at(0)->factorForRealExecution = 1;
        dag->vertices_by_id.at(2)->factorForRealExecution = 1;
        dag->vertices_by_id.at(3)->factorForRealExecution = 1;
        dag->vertices_by_id.at(4)->factorForRealExecution = 1;
        dag->first_edge->factorForRealExecution = 1;
        dag->first_edge->next->factorForRealExecution = 1;
        dag->first_edge->next->next->factorForRealExecution = 1;
        dag->first_edge->next->next->next->factorForRealExecution = 1;

        fonda::Options options;
        options.usePreemptiveWrites = true;
        options.algoNumber = 2;
        options.deviationModel = 2;

        double msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 131); // online scheduler adds the graph-target, but its generally the same results
    });

    free_graph(dag);
}