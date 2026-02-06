#include <gtest/gtest.h>
#include "WorkflowFactory.cpp"
#include "ClusterFactory.cpp"
#include "fonda_scheduler/SchedulerHeader.hpp"

TEST(ProcessIncomingEdgesTest, CanLogin) {


}

TEST(FinishTimeWithMemorySwappingTest, Success) {
    vertex_t* task = WorkflowFactory::CreateOneTaskWith3Incoming2Outgoing(100.0, 50, 10.0);
    task->swapRate=0.25;
    std::shared_ptr<Processor> p = ClusterFactory::createSingleProcessor(100, 1.0);

    EXPECT_NO_THROW({
        double result = finishTimeWithMemorySwapping(0.0, 20, 100, task, p);
        assert(result==101.0); // 100 for execution + 1.25*2/5 *2 = 1 for swapping
    });

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