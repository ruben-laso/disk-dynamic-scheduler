#include <gtest/gtest.h>
#include "WorkflowFactory.cpp"
#include "ClusterFactory.cpp"
#include "fonda_scheduler/SchedulerHeader.hpp"

TEST(SchedulerLogicTest, DoesNotExceedMemoryLimit) {

    graph_t* dag = WorkflowFactory::CreateChain(3, 100.0, 49.9, 10.0);
    imaginedCluster = ClusterFactory::CreateHomogeneous(2, 50.0, 1.0);
    double runtime =0;

    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Event>> events = medih2(dag, 1, runtime);
        assert(events.size()==6); // only task start and finish events for 3 tasks
        assert(events.at(1)->getExpectedTimeFire()==events.at(0)->getExpectedTimeFire()+100.0); // first task finishes after 100
        assert(events.at(3)->getExpectedTimeFire()==events.at(2)->getExpectedTimeFire()+100.0); // second task finishes after 100
        assert(events.at(5)->getExpectedTimeFire()==events.at(4)->getExpectedTimeFire()+100.0); // third task finishes after 100

    });

    free_graph(dag);
}

TEST(SchedulerLogicTest, ExceedsMemory) {

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateBottleneckCluster();

    double runtime =0;
    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Event>> events = medih2(dag, 1, runtime);
        assert(events.size()==16);

    });

    free_graph(dag);
}