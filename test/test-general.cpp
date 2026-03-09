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
        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, options, runtime);
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
        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, options, runtime);
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
        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, options, runtime);
        ASSERT_EQ(eventsMedih.size(), 16);
        for (auto e : eventsMedih) {
            e->printEventShort();
        }
        std::cout << "-------------------\n";
        imaginedCluster = ClusterFactory::CreateBottleneckCluster();
        imaginedClusterIncorrect = ClusterFactory::CreateBottleneckCluster();

        options.algoNumber = 0;
        std::vector<std::shared_ptr<Event>> eventsHeft = medih2(dag, options, runtime);

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
        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;
        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, options, runtimeHeft);

        clearGraph(dag);
        // dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
        imaginedCluster = ClusterFactory::CreateBottleneckCluster();
        imaginedClusterIncorrect = ClusterFactory::CreateBottleneckCluster();
        timeInSystem = 0;

        options.algoNumber = 0; // HEFT
        double msHeft = correctOflineMedihWithEvents(dag, actualCluster, options, runtimeHeft);
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
        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;
        auto eventsBL = medih2(dag, options, runtime);

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

TEST(TestWithDeviations, ForkNoDeviation)
{
    graph_t* dag = WorkflowFactory::CreateFork(4, 100.0, 100.0, 50.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
    actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

    EXPECT_NO_THROW({
        double runtime;

        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;

        // no deviations, all on one processor except for 3
        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOffline, 40); // 3 times runtime of 100/10 = 10 (no memory effects, no communication)

        dag = WorkflowFactory::CreateFork(4, 100.0, 100.0, 50.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        timeInSystem = 0;
        events.clear();
    });

    free_graph(dag);
}

TEST(TestWithDeviations, ForkTriesSpreadWithDeviationsButCannot)
{
    graph_t* dag = WorkflowFactory::CreateFork(3, 100.0, 100.0, 20.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
    actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

    // left task (task_0) is seriously delayed
    dag->vertices_by_id.at(1)->factorForRealExecution = 10;
    dag->vertices_by_id.at(0)->factorForRealExecution = 1;
    dag->vertices_by_id.at(2)->factorForRealExecution = 1;
    dag->vertices_by_id.at(3)->factorForRealExecution = 1;

    dag->first_edge->factorForRealExecutionRead = 1;
    dag->first_edge->factorForRealExecutionWrite = 1;
    dag->first_edge->next->factorForRealExecutionRead = 1;
    dag->first_edge->next->factorForRealExecutionWrite = 1;
    dag->first_edge->next->next->factorForRealExecutionRead = 1;
    dag->first_edge->next->next->factorForRealExecutionWrite = 1;

    EXPECT_NO_THROW({
        double runtime;

        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;

        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOffline, 120); //

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateFork(3, 100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(1)->factorForRealExecution = 10;
        dag->vertices_by_id.at(0)->factorForRealExecution = 1;
        dag->vertices_by_id.at(2)->factorForRealExecution = 1;
        dag->vertices_by_id.at(3)->factorForRealExecution = 1;

        dag->first_edge->factorForRealExecutionRead = 1;
        dag->first_edge->factorForRealExecutionWrite = 1;
        dag->first_edge->next->factorForRealExecutionRead = 1;
        dag->first_edge->next->factorForRealExecutionWrite = 1;
        dag->first_edge->next->next->factorForRealExecutionRead = 1;
        dag->first_edge->next->next->factorForRealExecutionWrite = 1;

        options.algoNumber = 2;
        options.taskReleasePolicy = 2; // only schedulet asks until we find one for our processor
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        double msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 121); // online scheduler adds the graph-target, but its generally the same results
    });

    free_graph(dag);
}

TEST(TestWithDeviations, ThickDiamondTriesSpreadWithDeviationsButCannot)
{
    graph_t* dag = WorkflowFactory::CreateThickDiamond(3, 100.0, 100.0, 20.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
    actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

    // left task (task_0) is seriously delayed
    dag->vertices_by_id.at(3)->factorForRealExecution = 10;
    dag->vertices_by_id.at(0)->factorForRealExecution = 1;
    dag->vertices_by_id.at(2)->factorForRealExecution = 1;
    dag->vertices_by_id.at(1)->factorForRealExecution = 1;

    auto e = dag->first_edge;

    while (e != nullptr) {
        e->factorForRealExecutionRead = 1;
        e->factorForRealExecutionWrite = 1;
        e = e->next;
    }

    EXPECT_NO_THROW({
        double runtime;

        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;

        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOffline, 128); //

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateThickDiamond(3, 100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(3)->factorForRealExecution = 10;
        dag->vertices_by_id.at(0)->factorForRealExecution = 1;
        dag->vertices_by_id.at(2)->factorForRealExecution = 1;
        dag->vertices_by_id.at(1)->factorForRealExecution = 1;
        dag->vertices_by_id.at(4)->factorForRealExecution = 1;

        e = dag->first_edge;
        while (e != nullptr) {
            e->factorForRealExecutionRead = 1;
            e->factorForRealExecutionWrite = 1;
            e = e->next;
        }

        options.algoNumber = 1;
        options.taskReleasePolicy = 3; // only schedulet asks until we find one for our processor
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        double msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 140); // online scheduler is worse, because it schedules one task at a atime and puts all on the same processor

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateThickDiamond(3, 100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(3)->factorForRealExecution = 10;
        dag->vertices_by_id.at(0)->factorForRealExecution = 1;
        dag->vertices_by_id.at(2)->factorForRealExecution = 1;
        dag->vertices_by_id.at(1)->factorForRealExecution = 1;
        dag->vertices_by_id.at(4)->factorForRealExecution = 1;

        e = dag->first_edge;
        while (e != nullptr) {
            e->factorForRealExecutionRead = 1;
            e->factorForRealExecutionWrite = 1;
            e = e->next;
        }

        options.algoNumber = 2;
        options.taskReleasePolicy = 1; // releases all tasks
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 128); // releases all at once, schedules all at once - like offline scheduler

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateThickDiamond(3, 100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(3)->factorForRealExecution = 10;
        dag->vertices_by_id.at(0)->factorForRealExecution = 1;
        dag->vertices_by_id.at(2)->factorForRealExecution = 1;
        dag->vertices_by_id.at(1)->factorForRealExecution = 1;
        dag->vertices_by_id.at(4)->factorForRealExecution = 1;

        e = dag->first_edge;
        while (e != nullptr) {
            e->factorForRealExecutionRead = 1;
           e->factorForRealExecutionWrite = 1;
            e = e->next;
        }

        options.algoNumber = 2;
        options.taskReleasePolicy = 2; // releases as many as processors (2)
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 128); // also like offline scheduler
    });

    free_graph(dag);
}

TEST(TestWithDeviations, LongDiamondTriesSpreadWithDeviationsButCannot)
{
    graph_t* dag = WorkflowFactory::CreateLongDiamond(100.0, 100.0, 20.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
    actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

    print_graph_to_cout(dag);

    dag->vertices_by_id.at(3)->factorForRealExecution = 10;

    EXPECT_NO_THROW({
        double runtime;

        fonda::Options options;
        options.algoNumber = 1; // HEFT-BL
        options.reverseOrdering = false;
        options.deviationModel = 1;

        double msOffline = correctOflineMedihWithEvents(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOffline, 144); //

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateLongDiamond(100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(3)->factorForRealExecution = 10;

        options.algoNumber = 2;
        options.taskReleasePolicy = 3; // only schedulet asks until we find one for our processor
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        double msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 170); // online scheduler is worse, because it schedules one task at a atime and puts all on the same processor

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateLongDiamond(100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(3)->factorForRealExecution = 10;

        options.algoNumber = 2;
        options.taskReleasePolicy = 1; // releases all tasks
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 148); // releases all at once, schedules all at once - like offline scheduler

        // with deviations, online scheduler spreads tasks to the second processor
        dag = WorkflowFactory::CreateLongDiamond(100.0, 100.0, 20.0);
        imaginedCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);
        actualCluster = ClusterFactory::CreateHomogeneous(2, 420, 10);

        // left task (task_0) is seriously delayed
        dag->vertices_by_id.at(3)->factorForRealExecution = 10;

        options.algoNumber = 2;
        options.taskReleasePolicy = 2; // releases as many as processors (2)
        options.deviationModel = 2; // doesnt matter, we fixated the deviations directly on the graph

        msOnline = onlineMedih(dag, actualCluster, options, runtime);
        ASSERT_EQ(msOnline, 148); // also like offline scheduler
    });

    free_graph(dag);
}
