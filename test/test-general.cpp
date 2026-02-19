#include "ClusterFactory.cpp"
#include "WorkflowFactory.cpp"
#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include <gtest/gtest.h>

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

TEST(BaselineTest, ExceedsMemory) {

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateBottleneckCluster();

    double runtime =0;
    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, 1, runtime);
        assert(eventsMedih.size()==16);
        for (auto e: eventsMedih) {
           e->printEventShort();
        }
        std::cout << "-------------------\n";
        imaginedCluster = ClusterFactory::CreateBottleneckCluster();
        imaginedClusterIncorrect = ClusterFactory::CreateBottleneckCluster();

        std::vector<std::shared_ptr<Event>> eventsHeft = medih2(dag, 0, runtime);
        //assert(eventsHeft.size()==16);

        for (auto e: eventsHeft) {
            e->printEventShort();
        }

    });

    free_graph(dag);
}



TEST(TestWithRuntime, ExceedsMemory) {

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateBottleneckCluster();
    actualCluster = ClusterFactory::CreateBottleneckCluster();

    double runtime =0;
    EXPECT_NO_THROW({
        //std::vector<std::shared_ptr<Event>> eventsMedih = medih2(dag, 1, runtime);
        double runtimeHeft;
        double msOffline  = correctOflineMedihWithEvents(dag, actualCluster, 1, 4, runtimeHeft);

        std::cout << "-------------------\n";
        clearGraph(dag);
        //dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
        imaginedCluster = ClusterFactory::CreateBottleneckCluster();
        imaginedClusterIncorrect = ClusterFactory::CreateBottleneckCluster();
        timeInSystem=0;

        double msHeft  = correctOflineMedihWithEvents(dag, actualCluster, 0, 4, runtimeHeft);


    });

    free_graph(dag);
}