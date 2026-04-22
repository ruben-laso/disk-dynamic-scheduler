#include "ClusterFactory.cpp"
#include "EventFactory.cpp"
#include "WorkflowFactory.cpp"
#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include <gtest/gtest.h>

TEST(ProcessIncomingEdgesTest, CanLogin)
{
}

TEST(FinishTimeWithMemorySwappingTest, ZeroTimeParallelism)
{

    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 100.0); // task time,task memory, edgeWeight - all edges same
    task->swapRate = 0.25;
    std::shared_ptr<Processor> p0 = ClusterFactory::createSingleProcessor(100, 1.0, 0);
    std::shared_ptr<Processor> p1 = ClusterFactory::createSingleProcessor(100, 1.0, 1);
    std::shared_ptr<Processor> p2 = ClusterFactory::createSingleProcessor(100, 1.0, 2);
    std::shared_ptr<Processor> p3 = ClusterFactory::createSingleProcessor(100, 1.0, 3);
    imaginedCluster = new Cluster();
    imaginedCluster->addProcessor(p0);
    imaginedCluster->addProcessor(p1);
    imaginedCluster->addProcessor(p2);
    imaginedCluster->addProcessor(p3);

    locateToThisProcessorFromNowhere(task->in_edges.at(0), 1, true, 100);
    locateToThisProcessorFromNowhere(task->in_edges.at(1), 2, true, 100);
    locateToThisProcessorFromNowhere(task->in_edges.at(2), 3, true, 100);

    p1->addPendingMemory(task->in_edges.at(0));
    p2->addPendingMemory(task->in_edges.at(1));
    p3->addPendingMemory(task->in_edges.at(2));

    task->in_edges.at(0)->tail->assignedProcessorId = 1;
    task->in_edges.at(1)->tail->assignedProcessorId = 2;
    task->in_edges.at(2)->tail->assignedProcessorId = 3;

    auto e1 = EventFactory::createWriteFinishEvent(task->in_edges.at(0), p1, 100);
    auto e2 = EventFactory::createWriteFinishEvent(task->in_edges.at(1), p2, 100);
    auto e3 = EventFactory::createWriteFinishEvent(task->in_edges.at(2), p3, 100);

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
        std::vector<std::shared_ptr<Processor>> modifiedProcs = { };
        std::vector<std::shared_ptr<Event>> createdEvents;
        double earliestStartComputeVertex = 0;
        processIncomingEdges(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);
        assert(createdEvents.size() == 12);
        assert(earliestStartComputeVertex == 140);

        assert(createdEvents.at(0)->getExpectedTimeFire() == 100); // first write start
        assert(createdEvents.at(3)->getExpectedTimeFire() == 120); // first read finish
        assert(createdEvents.at(4)->getExpectedTimeFire() == 100); // second write start, on independent processor
        assert(createdEvents.at(7)->getExpectedTimeFire() == 130); // second read finish, after first read finishes
        assert(createdEvents.at(8)->getExpectedTimeFire() == 100); // third write start, on independent processor
        assert(createdEvents.at(11)->getExpectedTimeFire() == 140); // third read finish, after second read finishes
    });

    delete task;
}

TEST(FinishTimeWithMemorySwappingTest, MemoryInducedDelay)
{
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

    processIncomingEdges(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);

    // Find the Read Start event
    auto readStart = *std::find_if(createdEvents.begin(), createdEvents.end(),
        [](auto e) { return e->type == OnReadStart; });

    // ASSERTION: Read must wait for T=500 (Compute Ready) because 40MB < 80MB
    EXPECT_GE(readStart->getExpectedTimeFire(), 500.0);
    EXPECT_EQ(earliestStartComputeVertex, 500.0 + (80.0 / p0->readSpeedDisk));

    delete task;
}

TEST(FinishTimeWithMemorySwappingTest, PreExistingDiskData)
{
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

    processIncomingEdges(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);

    auto readStart = *std::find_if(createdEvents.begin(), createdEvents.end(),
        [](auto e) { return e->type == OnReadStart; });

    EXPECT_EQ(readStart->getExpectedTimeFire(), 1000.0);

    int writeEvents = std::count_if(createdEvents.begin(), createdEvents.end(),
        [](auto e) { return e->type == OnWriteStart || e->type == OnWriteFinish; });
    EXPECT_EQ(writeEvents, 0);

    delete task;
}

TEST(FinishTimeWithMemorySwappingTest, LocalEdgeReuse)
{
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

    processIncomingEdges(task, p0, modifiedProcs, earliestStartComputeVertex, createdEvents, false);

    EXPECT_EQ(createdEvents.size(), 0);

    delete task;
}

TEST(howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere, HasIncomingInPending)
{
    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    vertex_t* otherTask = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    task->swapRate = 0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);
    p->addPendingMemory(task->in_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(1));

    EXPECT_NO_THROW({
        double result = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(task, p);
        assert(result == 30); // 100 total - 50 for task - 10 for otherTask out edge1 - 10 for otherTask out edge2 + 10 for task in edge that will not take space because it will be consumed
    });

    delete task;
}

TEST(realSurplusOfOutgoingEdges, HasIncomingInPending)
{
    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    vertex_t* otherTask = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    task->swapRate = 0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);
    p->addPendingMemory(task->in_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(0));
    p->addPendingMemory(otherTask->out_edges.at(1));

    EXPECT_NO_THROW({
        double result = howMuchMemoryIsStillAvailableOnProcIfTaskScheduledThere(task, p);
        assert(result == 30); // 100 total - 50 for task - 10 for otherTask out edge1 - 10 for otherTask out edge2 + 10 for task in edge that will not take space because it will be consumed
    });

    delete task;
}

TEST(processIncomingEdges, CannotBePLacedBeforeLastEvent)
{
    vertex_t* task1 = WorkflowFactory::CreateOneSimpleTaskNoEdges("1", 100.0, 50);
    vertex_t* task2 = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 10);
    vertex_t* task3 = WorkflowFactory::CreateOneSimpleTaskNoEdges("3", 100.0, 50);
    vertex_t* task4 = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 10.0);

    task4->swapRate = 0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);
    std::shared_ptr<Processor> p2 = ClusterFactory::createSingleProcessor(100, 1.0);
    p2->id = 2;
    imaginedCluster= new Cluster();
    imaginedCluster->addProcessor(p);
    imaginedCluster->addProcessor(p2);

    auto t1_s = EventFactory::createTaskStartEvent(task1, p, 10);
    auto t1_f = EventFactory::createTaskFinishEvent(task1, p, 100);
    t1_f->addPredecessorInPlanning(t1_s);
    auto t2_s = EventFactory::createTaskStartEvent(task2, p, 10);
    auto t2_f = EventFactory::createTaskFinishEvent(task2, p, 400);
    t2_f->addPredecessorInPlanning(t2_s);
    auto t3_s = EventFactory::createTaskStartEvent(task3, p, 410);
    auto t3_f = EventFactory::createTaskFinishEvent(task3, p, 500);
    t3_f->addPredecessorInPlanning(t3_s);

    auto read = EventFactory::createReadFinishEvent(task2->in_edges.at(0), p, 50);

    p->setReadyTimeCompute(500);
    p->setLastComputeEvent(t3_f);
    p->setLastReadEvent(read);
    p->setAvailableMemoryDuringPreviousTask(20);
    p2->setAvailableMemoryDuringPreviousTask(30);

    task4->in_edges.at(0)->tail->assignedProcessorId = 2;
    p2->addPendingMemory(task4->in_edges.at(0));
    locateToThisProcessorFromNowhere(task4->in_edges.at(0), 2, true, 20);

    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Processor>> modifiedProcs;
        double earliestStartingTimeToComputeVertex;
        std::vector<std::shared_ptr<Event>> createdEvents;
        processIncomingEdges(task4, p, modifiedProcs, earliestStartingTimeToComputeVertex, createdEvents, false);

        ASSERT_EQ(createdEvents.size(), 4);
        ASSERT_EQ(createdEvents.at(2)->getExpectedTimeFire(), 410); // read start must be after last compute event because we know memory size after then
    });

    delete task1;
    delete task2;
    delete task3;
    delete task4;
}

TEST(processIncomingEdgesWithoutTimes, CannotBePLacedBeforeLastEventButIs)
{
    vertex_t* task1 = WorkflowFactory::CreateOneSimpleTaskNoEdges("1", 100.0, 50);
    vertex_t* task2 = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 10);
    vertex_t* task3 = WorkflowFactory::CreateOneSimpleTaskNoEdges("3", 100.0, 50);
    vertex_t* task4 = WorkflowFactory::CreateOneTaskWith1Incoming1Outgoing(100.0, 50, 10.0);

    task4->swapRate = 0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);
    std::shared_ptr<Processor> p2 = ClusterFactory::createSingleProcessor(100, 1.0);
    p2->id = 2;
    p->setIsKeptValid(false);
    p2->setIsKeptValid(false);

    imaginedClusterIncorrect= new Cluster();
    imaginedClusterIncorrect->mayBecomeInvalid();
    imaginedClusterIncorrect->addProcessor(p);
    imaginedClusterIncorrect->addProcessor(p2);

    auto t1_s = EventFactory::createTaskStartEvent(task1, p, 10);
    auto t1_f = EventFactory::createTaskFinishEvent(task1, p, 100);
    t1_f->addPredecessorInPlanning(t1_s);
    auto t2_s = EventFactory::createTaskStartEvent(task2, p, 10);
    auto t2_f = EventFactory::createTaskFinishEvent(task2, p, 400);
    t2_f->addPredecessorInPlanning(t2_s);
    auto t3_s = EventFactory::createTaskStartEvent(task3, p, 410);
    auto t3_f = EventFactory::createTaskFinishEvent(task3, p, 500);
    t3_f->addPredecessorInPlanning(t3_s);

    auto read = EventFactory::createReadFinishEvent(task2->in_edges.at(0), p, 50);

    p->setReadyTimeCompute(500);
    p->setLastComputeEvent(t3_f);
    p->setLastReadEvent(read);
    p->setAvailableMemoryDuringPreviousTask(20);
    p2->setAvailableMemoryDuringPreviousTask(30);

    task4->in_edges.at(0)->tail->assignedProcessorId = 2;
    p2->addPendingMemory(task4->in_edges.at(0));
    locateToThisProcessorFromNowhere(task4->in_edges.at(0), 2, true, 20);

    EXPECT_NO_THROW({
        std::vector<std::shared_ptr<Processor>> modifiedProcs;
        double earliestStartingTimeToComputeVertex=0;
        std::vector<std::shared_ptr<Event>> createdEvents;
        processIncomingEdgesDisregardingMemorySizes(task4, p, modifiedProcs, earliestStartingTimeToComputeVertex);

        ASSERT_EQ(earliestStartingTimeToComputeVertex, 500);//we can compute after the last compute has finished
        ASSERT_EQ(p->getReadyTimeRead(), 51);//50 + 1 for read; place the read early, disregarding memory sizes
    });

    delete task1;
    delete task2;
    delete task3;
    delete task4;
}

TEST(computeSwapPenalty, PenaltyOnTwoProccessors)
{

    graph_t* dag = WorkflowFactory::CreateDiamondWithQuarterSides( 100.0, 100.0, 10.0);
    imaginedCluster = ClusterFactory::CreateMidBottleneckCluster();
    actualCluster = ClusterFactory::CreateMidBottleneckCluster();

    for (auto vertices_by_id : dag->vertices_by_id) {
        auto v  = vertices_by_id.second;
        v->swapRate = 0.1;
    }


    EXPECT_NO_THROW({

        //If place source on smallerProcessor
            fonda::Options opts { };

        double finishTime = finishTimeWithMemorySwapping(0, 80, 100, dag->vertices_by_id.at(0), imaginedCluster->processors.at(1), opts);
        ASSERT_EQ(abs(finishTime- 9.04)<1e-6, true);

        //If place left on smallerProcessor, while the right's file is there: from 20 memory 10 is occupied with a file, left's mem req is 22
        finishTime = finishTimeWithMemorySwapping(0, 12, 25, dag->vertices_by_id.at(1), imaginedCluster->processors.at(1), opts);
        ASSERT_EQ(abs(finishTime- 1.22)<1e-6, true);

    });


}