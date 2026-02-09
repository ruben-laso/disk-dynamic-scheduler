#include "ClusterFactory.cpp"
#include "EventFactory.cpp"
#include "WorkflowFactory.cpp"
#include "fonda_scheduler/DynamicSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include <gtest/gtest.h>

TEST(ProcessIncomingEdgesTest, CanLogin) {


}

TEST(FinishTimeWithMemorySwappingTest, ZeroTimeParallelism) {

    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 100.0);//task time,task memory, edgeWeight - all edges same
    task->swapRate=0.25;
    std::shared_ptr<Processor> p0 = ClusterFactory::createSingleProcessor(100, 1.0,0);
    std::shared_ptr<Processor> p1 = ClusterFactory::createSingleProcessor(100, 1.0,1);
    std::shared_ptr<Processor> p2 = ClusterFactory::createSingleProcessor(100, 1.0,2);
    std::shared_ptr<Processor> p3 = ClusterFactory::createSingleProcessor(100, 1.0,3);
    imaginedCluster =  new Cluster();
    imaginedCluster->addProcessor(p0);
    imaginedCluster->addProcessor(p1);
    imaginedCluster->addProcessor(p2);
    imaginedCluster->addProcessor(p3);

    locateToThisProcessorFromNowhere(task->in_edges.at(0), 1,true, 100);
    locateToThisProcessorFromNowhere(task->in_edges.at(1), 2,true, 100);
    locateToThisProcessorFromNowhere(task->in_edges.at(2), 3,true, 100);

    p1->addPendingMemory(task->in_edges.at(0));
    p2->addPendingMemory(task->in_edges.at(1));
    p3->addPendingMemory(task->in_edges.at(2));

    task->in_edges.at(0)->tail->assignedProcessorId=1;
    task->in_edges.at(1)->tail->assignedProcessorId=2;
    task->in_edges.at(2)->tail->assignedProcessorId=3;

    auto e1 = EventFactory::createWriteFinishEvent( task->in_edges.at(0), p1, 100);
    auto e2 = EventFactory::createWriteFinishEvent( task->in_edges.at(1), p2, 100);
    auto e3 = EventFactory::createWriteFinishEvent( task->in_edges.at(2), p3, 100);

    events.insert(e1);
    events.insert(e2);
    events.insert(e3);

    e1 = EventFactory::createTaskFinishEvent(task->in_edges.at(0)->tail, p1, 50);
    e2 = EventFactory::createTaskFinishEvent(task->in_edges.at(1)->tail, p2, 50);
    e3 = EventFactory::createTaskFinishEvent(task->in_edges.at(2)->tail, p3, 50);

    events.insert(e1);
    events.insert(e2);
    events.insert(e3);


    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Processor>> modifiedProcs={};
        std::vector<std::shared_ptr<Event>> createdEvents;
        double earliestStartComputeVertex=0;
        processIncomingEdges2(task,p0, modifiedProcs,earliestStartComputeVertex,createdEvents, false);
        assert(createdEvents.size()==12);
        assert(earliestStartComputeVertex==140);

        assert(createdEvents.at(0)->getExpectedTimeFire()==100); // first write start
        assert(createdEvents.at(3)->getExpectedTimeFire()==120); // first read finish
        assert(createdEvents.at(4)->getExpectedTimeFire()==100); // second write start, on independent processor
        assert(createdEvents.at(7)->getExpectedTimeFire()==130); // second read finish, after first read finishes
        assert(createdEvents.at(8)->getExpectedTimeFire()==100); // third write start, on independent processor
        assert(createdEvents.at(11)->getExpectedTimeFire()==140); // third read finish, after second read finishes

    });

    delete task;
}

TEST(FinishTimeWithMemorySwappingTest, MemoryInducedDelay) {
    // Edge weight 80, Task memory 50
    vertex_t* task = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 80.0);
    std::shared_ptr<Processor> p0 = ClusterFactory::createSingleProcessor(100, 1.0, 0);
    std::shared_ptr<Processor> p1 = ClusterFactory::createSingleProcessor(100, 1.0, 1);

    imaginedCluster = new Cluster();
    imaginedCluster->addProcessor(p0);
    imaginedCluster->addProcessor(p1);

    // Simulate a previous task still running on p0 until T=500, using 60MB
    // Total 100 - Used 60 = 40 Available. Our edge needs 80.
    p0->setReadyTimeCompute(500.0);
    p0->setAvailableMemoryDuringPreviousTask(40.0);

    // Predecessor on p1 finishes writing at 100
    locateToThisProcessorFromNowhere(task->in_edges.at(0), 1, true, 100);
    task->in_edges.at(0)->tail->assignedProcessorId = 1;
    p1->addPendingMemory(task->in_edges.at(0));


    auto wf = EventFactory::createWriteFinishEvent(task->in_edges.at(0), p1, 100);
    auto tf = EventFactory::createTaskFinishEvent(task->in_edges.at(0)->tail, p1, 50);
    events.insert(wf);
    events.insert(tf);

    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    std::vector<std::shared_ptr<Event>> createdEvents;
    double earliestStartComputeVertex = 0;

    processIncomingEdges2(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);

    // Find the Read Start event
    auto readStart = *std::find_if(createdEvents.begin(), createdEvents.end(),
        [](auto e){ return e->type == OnReadStart; });

    // ASSERTION: Read must wait for T=500 (Compute Ready) because 40MB < 80MB
    EXPECT_GE(readStart->getExpectedTimeFire(), 500.0);
    EXPECT_EQ(earliestStartComputeVertex, 500.0 + (80.0 / p0->readSpeedDisk));

    delete task;
}

TEST(FinishTimeWithMemorySwappingTest, PreExistingDiskData) {
    vertex_t* task = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 50.0);
    std::shared_ptr<Processor> p0 = ClusterFactory::createSingleProcessor(100, 1.0, 0);

    imaginedCluster = new Cluster();
    imaginedCluster->addProcessor(p0);

    auto edge = task->in_edges.at(0);
    locateToDisk(edge, true, 1000.0);

    auto dummyWF = Event::createEvent(nullptr, edge, OnWriteFinish, p0, 0, 0, false, buildEdgeName(edge) + "-w-f");
    events.insert(dummyWF);

    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    std::vector<std::shared_ptr<Event>> createdEvents;
    double earliestStartComputeVertex = 0;

    processIncomingEdges2(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);


    auto readStart = *std::find_if(createdEvents.begin(), createdEvents.end(),
        [](auto e){ return e->type == OnReadStart; });


    EXPECT_EQ(readStart->getExpectedTimeFire(), 1000.0);

    int writeEvents = std::count_if(createdEvents.begin(), createdEvents.end(),
        [](auto e){ return e->type == OnWriteStart || e->type == OnWriteFinish; });
    EXPECT_EQ(writeEvents, 0);

    delete task;
}

TEST(FinishTimeWithMemorySwappingTest, LocalEdgeReuse) {
    vertex_t* task = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 50.0);
    std::shared_ptr<Processor> p0 = ClusterFactory::createSingleProcessor(100, 1.0, 0);

    imaginedCluster = new Cluster();
    imaginedCluster->addProcessor(p0);

    task->in_edges.at(0)->tail->assignedProcessorId = 0;
    locateToThisProcessorFromNowhere(task->in_edges.at(0), 0, true, 200.0);

    auto tf = EventFactory::createTaskFinishEvent(task->in_edges.at(0)->tail, p0, 250.0);
    events.insert(tf);

    std::vector<std::shared_ptr<Processor>> modifiedProcs;
    std::vector<std::shared_ptr<Event>> createdEvents;
    double earliestStartComputeVertex = 0;

    processIncomingEdges2(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);


    EXPECT_EQ(createdEvents.size(), 0);

    delete task;
}

TEST(howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere, HasIncomingInPending) {
    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    vertex_t* otherTask = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    task->swapRate=0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);
    p->addPendingMemory(task->in_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(1));

    EXPECT_NO_THROW({
        double result = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(task, p);
        assert(result== 30); // 100 total - 50 for task - 10 for otherTask out edge1 - 10 for otherTask out edge2 + 10 for task in edge that will not take space because it will be consumed
    });

    delete task;
}

TEST(realSurplusOfOutgoingEdges, HasIncomingInPending) {
    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    vertex_t* otherTask = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    task->swapRate=0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);
    p->addPendingMemory(task->in_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(1));

    EXPECT_NO_THROW({
        double result = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(task, p);
        assert(result== 30); // 100 total - 50 for task - 10 for otherTask out edge1 - 10 for otherTask out edge2 + 10 for task in edge that will not take space because it will be consumed
    });

    delete task;
}