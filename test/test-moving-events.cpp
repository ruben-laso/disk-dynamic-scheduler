#include "ClusterFactory.cpp"
#include "EventFactory.cpp"
#include "WorkflowFactory.cpp"
#include "fonda_scheduler/OnlineSchedulerHeader.hpp"
#include "fonda_scheduler/SchedulerHeader.hpp"
#include <gtest/gtest.h>

TEST(EventReschedulingTest, OneFinishNoChange) {

    auto first = WorkflowFactory::CreateOneSimpleTaskNoEdges("first", 100.0, 50);
    auto second = WorkflowFactory::CreateOneSimpleTaskNoEdges("second", 100.0, 50);

    auto p = ClusterFactory::createSingleProcessor(80, 1.0);

    auto first_f = EventFactory::createTaskFinishEvent(first, p, 5);
    auto second_s = EventFactory::createTaskStartEvent(second, p, 10);
    auto second_f = EventFactory::createTaskFinishEvent(second, p, 20);
    second_f->addPredecessorInPlanning(second_s);

    EXPECT_NO_THROW({
        second_s->addPredecessorInPlanning(first_f);
        assert(second_s->getExpectedTimeFire()==10);

    });

}

TEST(EventReschedulingTest, OneFinishPushForward) {

    auto first = WorkflowFactory::CreateOneSimpleTaskNoEdges("first", 100.0, 50);
    auto second = WorkflowFactory::CreateOneSimpleTaskNoEdges("second", 100.0, 50);

    auto p = ClusterFactory::createSingleProcessor(80, 1.0);

    auto first_f = EventFactory::createTaskFinishEvent(first, p, 11);
    auto second_s = EventFactory::createTaskStartEvent(second, p, 10);
    auto second_f = EventFactory::createTaskFinishEvent(second, p, 20);
    second_f->addPredecessorInPlanning(second_s);

    EXPECT_NO_THROW({
        assert(second_s->getExpectedTimeFire()==10);
        second_s->addPredecessorInPlanning(first_f);
        assert(second_s->getExpectedTimeFire()==11);
        assert(second_f->getExpectedTimeFire()==21);

    });

}

TEST(EventReschedulingTest, LongChainPushForward) {

    auto first = WorkflowFactory::CreateOneSimpleTaskNoEdges("first", 100.0, 50);
    auto second = WorkflowFactory::CreateOneSimpleTaskNoEdges("second", 100.0, 50);
    auto third = WorkflowFactory::CreateOneSimpleTaskNoEdges("third", 100.0, 50);

    auto p = ClusterFactory::createSingleProcessor(80, 1.0);

    auto first_f = EventFactory::createTaskFinishEvent(first, p, 11);
    auto second_s = EventFactory::createTaskStartEvent(second, p, 10);
    auto second_f = EventFactory::createTaskFinishEvent(second, p, 20);
    auto third_s = EventFactory::createTaskStartEvent(third, p, 20);
    auto third_f = EventFactory::createTaskFinishEvent(third, p, 30);
    second_f->addPredecessorInPlanning(second_s);
    third_f->addPredecessorInPlanning(third_s);


    EXPECT_NO_THROW({
        assert(third_f->getExpectedTimeFire()==30);
        third_s->addPredecessorInPlanning(second_f);
        assert(third_f->getExpectedTimeFire()==30);
        assert(second_s->getExpectedTimeFire()==10);
        second_s->addPredecessorInPlanning(first_f);
        assert(second_s->getExpectedTimeFire()==11);
        assert(second_f->getExpectedTimeFire()==21);
        assert(third_s->getExpectedTimeFire()==21);
        assert(third_f->getExpectedTimeFire()==31);
    });

}

TEST(EventReschedulingTest, DiamondOneBranchNoPushForwardOnEnd) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 25);

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_c_w_s->addPredecessorInPlanning(a_f);
    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);
    x_b_r_s->addPredecessorInPlanning(a_f);
    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);


    EXPECT_NO_THROW({
        a_f->adjustBothPlannedFireTimes(11);

        assert(a_c_w_s->getExpectedTimeFire()==11);
        assert(a_c_w_f->getExpectedTimeFire()==16);
        assert(a_c_r_s->getExpectedTimeFire()==16);
        assert(a_c_r_f->getExpectedTimeFire()==21);
        assert(c_s->getExpectedTimeFire()==21);
        assert(c_f->getExpectedTimeFire()==26);

        assert(x_b_r_s->getExpectedTimeFire()==20);
        assert(x_b_r_f->getExpectedTimeFire()==25);
        assert(y_b_r_s->getExpectedTimeFire()==25);
        assert(y_b_r_f->getExpectedTimeFire()==30);

        assert(b_s->getExpectedTimeFire()==30);
        assert(b_f->getExpectedTimeFire()==35);
        assert(b_m_w_s->getExpectedTimeFire()==35);
        assert(b_m_w_f->getExpectedTimeFire()==40);

        assert(m_s->getExpectedTimeFire()==40);
        assert(m_f->getExpectedTimeFire()==45);


    });

}

TEST(EventReschedulingTest, DiamondOneBranchPushForwardOnEnd) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);//THIS! both branches finish at the same time, so the earlier will push the diamond end

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_c_w_s->addPredecessorInPlanning(a_f);
    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);
    x_b_r_s->addPredecessorInPlanning(a_f);
    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);


    EXPECT_NO_THROW({
        a_f->adjustBothPlannedFireTimes(11);

        assert(a_c_w_s->getExpectedTimeFire()==11);
        assert(a_c_w_f->getExpectedTimeFire()==16);
        assert(a_c_r_s->getExpectedTimeFire()==16);
        assert(a_c_r_f->getExpectedTimeFire()==21);
        assert(c_s->getExpectedTimeFire()==21);
        assert(c_f->getExpectedTimeFire()==41);

        assert(x_b_r_s->getExpectedTimeFire()==20);
        assert(x_b_r_f->getExpectedTimeFire()==25);
        assert(y_b_r_s->getExpectedTimeFire()==25);
        assert(y_b_r_f->getExpectedTimeFire()==30);

        assert(b_s->getExpectedTimeFire()==30);
        assert(b_f->getExpectedTimeFire()==35);
        assert(b_m_w_s->getExpectedTimeFire()==35);
        assert(b_m_w_f->getExpectedTimeFire()==40);

        assert(m_s->getExpectedTimeFire()==41);
        assert(m_f->getExpectedTimeFire()==46);


    });

}

TEST(EventReschedulingTest, DiamondBothBranchesPushForwardOnEnd) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);//THIS! both branches finish at the same time, so the earlier will push the diamond end

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_c_w_s->addPredecessorInPlanning(a_f);
    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);
    x_b_r_s->addPredecessorInPlanning(a_f);
    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);


    EXPECT_NO_THROW({
        a_f->adjustBothPlannedFireTimes(21);

        EXPECT_EQ(a_c_w_s->getExpectedTimeFire(),21);
        EXPECT_EQ(a_c_w_f->getExpectedTimeFire(),26);
        EXPECT_EQ(a_c_r_s->getExpectedTimeFire(),26);
        EXPECT_EQ(a_c_r_f->getExpectedTimeFire(),31);
        EXPECT_EQ(c_s->getExpectedTimeFire(),31);
        EXPECT_EQ(c_f->getExpectedTimeFire(),51);

        EXPECT_EQ(x_b_r_s->getExpectedTimeFire(),21);
        EXPECT_EQ(x_b_r_f->getExpectedTimeFire(),26);
        EXPECT_EQ(y_b_r_s->getExpectedTimeFire(),26);
        EXPECT_EQ(y_b_r_f->getExpectedTimeFire(),31);

        EXPECT_EQ(b_s->getExpectedTimeFire(),31);
        EXPECT_EQ(b_f->getExpectedTimeFire(),36);
        EXPECT_EQ(b_m_w_s->getExpectedTimeFire(),36);
        EXPECT_EQ(b_m_w_f->getExpectedTimeFire(),41);

        EXPECT_EQ(m_s->getExpectedTimeFire(),51);
        EXPECT_EQ(m_f->getExpectedTimeFire(),56);


    });

}


TEST(AdjustBothTimesFire, OneFinishReschedulePushForward) {

    auto first = WorkflowFactory::CreateOneSimpleTaskNoEdges("first", 100.0, 50);
    auto second = WorkflowFactory::CreateOneSimpleTaskNoEdges("second", 100.0, 50);

    auto p = ClusterFactory::createSingleProcessor(80, 1.0);

    auto first_f = EventFactory::createTaskFinishEvent(first, p, 5);
    auto second_s = EventFactory::createTaskStartEvent(second, p, 10);
    auto second_f = EventFactory::createTaskFinishEvent(second, p, 20);
    second_f->addPredecessorInPlanning(second_s);

    EXPECT_NO_THROW({
        assert(second_s->getExpectedTimeFire()==10);
        second_s->addPredecessorInPlanning(first_f);
        assert(second_s->getExpectedTimeFire()==10);

        first_f->adjustBothPlannedFireTimes(11);
        assert(second_s->getExpectedTimeFire()==11);
        assert(second_f->getExpectedTimeFire()==21);

    });

}

TEST(Reschedule, RescheduleNoPredecessors)
{
    auto task = WorkflowFactory::CreateOneSimpleTaskNoEdges("A", 10, 50);
    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);

    auto task_s = EventFactory::createTaskStartEvent(task, p1, 5);
    auto task_f = EventFactory::createTaskFinishEvent(task, p1, 10);

    events.insert(task_s);
    events.insert(task_f);

    events.reschedule("A-f", 15);
    ASSERT_EQ(task_f->getActualTimeFire(),15);

    events.reschedule("A-f", 8);
    ASSERT_EQ(task_f->getActualTimeFire(),8);


}

TEST(Reschedule, RescheduleSimplePredecessor)
{
    auto task = WorkflowFactory::CreateOneSimpleTaskNoEdges("A", 10, 50);
    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);

    auto task_s = EventFactory::createTaskStartEvent(task, p1, 5);
    auto task_f = EventFactory::createTaskFinishEvent(task, p1, 10);
    task_f->addPredecessorInPlanning(task_s);

    events.insert(task_s);
    events.insert(task_f);

    events.reschedule("A-f", 15);
    ASSERT_EQ(task_f->getActualTimeFire(),15);
    ASSERT_EQ(task_s->getActualTimeFire(),5);

    events.reschedule("A-f", 8);
    ASSERT_EQ(task_f->getActualTimeFire(),8);

    events.reschedule("A-s", 2);
    ASSERT_EQ(task_f->getActualTimeFire(),5);

}

TEST(EventReschedulingTest, DiamondBothBranchesPullBackward) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 10);
    auto a_s = EventFactory::createTaskStartEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_f->addPredecessorInPlanning(a_s);
    a_c_w_s->addPredecessorInPlanning(a_f);
    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);
    x_b_r_s->addPredecessorInPlanning(a_f);
    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);

    events.insert(a_f); events.insert(a_s);
    events.insert(a_c_w_s);    events.insert(a_c_w_f);
    events.insert(a_c_r_s);    events.insert(a_c_r_f);
    events.insert(x_b_r_s);    events.insert(x_b_r_f);
    events.insert(y_b_r_s);    events.insert(y_b_r_f);
    events.insert(b_s);    events.insert(b_f);
    events.insert(b_m_w_s);    events.insert(b_m_w_f);
    events.insert(c_s);    events.insert(c_f);
    events.insert(m_s);    events.insert(m_f);


    EXPECT_NO_THROW({

        events.reschedule(a_f->id, 5);

        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),5);
        EXPECT_EQ(events.find("a-c-w-f").get()->getActualTimeFire(),10);
        EXPECT_EQ(events.find("a-c-r-s").get()->getActualTimeFire(),10);
        EXPECT_EQ(events.find("a-c-r-f").get()->getActualTimeFire(),15);
        EXPECT_EQ(events.find("c-s").get()->getActualTimeFire(),15);
        EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),35);

        EXPECT_EQ(events.find("x-b-r-s").get()->getActualTimeFire(),5);
        EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),10);
        EXPECT_EQ(events.find("y-b-r-s").get()->getActualTimeFire(),10);
        EXPECT_EQ(events.find("y-b-r-f").get()->getActualTimeFire(),15);

        EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),15);
        EXPECT_EQ(events.find("b-f").get()->getActualTimeFire(),20);
        EXPECT_EQ(events.find("b-m-w-s").get()->getActualTimeFire(),20);
        EXPECT_EQ(events.find("b-m-w-f").get()->getActualTimeFire(),25);

        EXPECT_EQ(events.find("m-s").get()->getActualTimeFire(),35);
        EXPECT_EQ(events.find("m-f").get()->getActualTimeFire(),40);

    });

}


TEST(EventReschedulingTest, DiamondOneBranchPushesForward) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 10);
    auto a_s = EventFactory::createTaskStartEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_f->addPredecessorInPlanning(a_s);
    a_c_w_s->addPredecessorInPlanning(a_f);
    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);
    x_b_r_s->addPredecessorInPlanning(a_f);
    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);

    events.insert(a_f); events.insert(a_s);
    events.insert(a_c_w_s);    events.insert(a_c_w_f);
    events.insert(a_c_r_s);    events.insert(a_c_r_f);
    events.insert(x_b_r_s);    events.insert(x_b_r_f);
    events.insert(y_b_r_s);    events.insert(y_b_r_f);
    events.insert(b_s);    events.insert(b_f);
    events.insert(b_m_w_s);    events.insert(b_m_w_f);
    events.insert(c_s);    events.insert(c_f);
    events.insert(m_s);    events.insert(m_f);


    EXPECT_NO_THROW({

        events.reschedule(a_f->id, 12);

        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),12);
        EXPECT_EQ(events.find("a-c-w-f").get()->getActualTimeFire(),17);
        EXPECT_EQ(events.find("a-c-r-s").get()->getActualTimeFire(),17);
        EXPECT_EQ(events.find("a-c-r-f").get()->getActualTimeFire(),22);
        EXPECT_EQ(events.find("c-s").get()->getActualTimeFire(),22);
        EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),42);

        EXPECT_EQ(events.find("x-b-r-s").get()->getActualTimeFire(),20);
        EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),25);
        EXPECT_EQ(events.find("y-b-r-s").get()->getActualTimeFire(),25);
        EXPECT_EQ(events.find("y-b-r-f").get()->getActualTimeFire(),30);

        EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),30);
        EXPECT_EQ(events.find("b-f").get()->getActualTimeFire(),35);
        EXPECT_EQ(events.find("b-m-w-s").get()->getActualTimeFire(),35);
        EXPECT_EQ(events.find("b-m-w-f").get()->getActualTimeFire(),40);

        EXPECT_EQ(events.find("m-s").get()->getActualTimeFire(),42);
        EXPECT_EQ(events.find("m-f").get()->getActualTimeFire(),47);

    });

}

TEST(EventReschedulingTest, DiamondBothBranchesPushForward) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 10);
    auto a_s = EventFactory::createTaskStartEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_f->addPredecessorInPlanning(a_s);
    a_c_w_s->addPredecessorInPlanning(a_f);
    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);
    x_b_r_s->addPredecessorInPlanning(a_f);
    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);

    events.insert(a_f); events.insert(a_s);
    events.insert(a_c_w_s);    events.insert(a_c_w_f);
    events.insert(a_c_r_s);    events.insert(a_c_r_f);
    events.insert(x_b_r_s);    events.insert(x_b_r_f);
    events.insert(y_b_r_s);    events.insert(y_b_r_f);
    events.insert(b_s);    events.insert(b_f);
    events.insert(b_m_w_s);    events.insert(b_m_w_f);
    events.insert(c_s);    events.insert(c_f);
    events.insert(m_s);    events.insert(m_f);


    EXPECT_NO_THROW({

        events.reschedule(a_f->id, 21);

        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),21);
        EXPECT_EQ(events.find("a-c-w-f").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("a-c-r-s").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("a-c-r-f").get()->getActualTimeFire(),31);
        EXPECT_EQ(events.find("c-s").get()->getActualTimeFire(),31);
        EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),51);

        EXPECT_EQ(events.find("x-b-r-s").get()->getActualTimeFire(),21);
        EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("y-b-r-s").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("y-b-r-f").get()->getActualTimeFire(),31);

        EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),31);
        EXPECT_EQ(events.find("b-f").get()->getActualTimeFire(),36);
        EXPECT_EQ(events.find("b-m-w-s").get()->getActualTimeFire(),36);
        EXPECT_EQ(events.find("b-m-w-f").get()->getActualTimeFire(),41);

        EXPECT_EQ(events.find("m-s").get()->getActualTimeFire(),51);
        EXPECT_EQ(events.find("m-f").get()->getActualTimeFire(),56);

    });

}


TEST(Reschedule, SlackSuccessorMovesMore) {
    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto taskA = WorkflowFactory::CreateOneSimpleTaskNoEdges("A", 5, 50);
    auto taskB = WorkflowFactory::CreateOneSimpleTaskNoEdges("B", 5, 50);

    auto af = EventFactory::createTaskFinishEvent(taskA, p1, 10);
    auto bs = EventFactory::createTaskStartEvent(taskB, p1, 20); // 10s Slack
    bs->addPredecessorInPlanning(af);

    events.insert(af); events.insert(bs);

    // Pull A-f back 5 seconds
    events.reschedule("A-f", 5);

    EXPECT_EQ(af->getActualTimeFire(), 5);
    EXPECT_EQ(bs->getActualTimeFire(), 5); //successor moves more than 5 seconds
}

TEST(EventReschedulingTest, ResourceChainSerialization) {
    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto t1 = WorkflowFactory::CreateOneSimpleTaskNoEdges("T1", 0, 0);
    auto t2 = WorkflowFactory::CreateOneSimpleTaskNoEdges("T2", 0, 0);
    graph_t* g = new_graph();
    new_edge(g,t1,t2,50,nullptr);
    new_edge(g,t2,t1,50,nullptr);

    // T1 Read: 10-20
    auto r1s = EventFactory::createReadStartEvent(t1->out_edges.at(0), p1, 10);
    auto r1f = EventFactory::createReadFinishEvent(t1->out_edges.at(0), p1, 20);
    r1f->addPredecessorInPlanning(r1s);

    // T2 Read: 20-30 (Serial link created by your processIncomingEdges2 logic)
    auto r2s = EventFactory::createReadStartEvent(t2->out_edges.at(0), p1, 20);
    auto r2f = EventFactory::createReadFinishEvent(t2->out_edges.at(0), p1, 30);
    r2f->addPredecessorInPlanning(r2s);
    r2s->addPredecessorInPlanning(r1f); // The resource dependency

    events.insert(r1s); events.insert(r1f);
    events.insert(r2s); events.insert(r2f);

    // Scenario A: T1 finishes early
    events.reschedule(r1f->id, 15);
    EXPECT_EQ(r2s->getActualTimeFire(), 15);
    EXPECT_EQ(r2f->getActualTimeFire(), 25);

    // Scenario B: T1 finishes late
    events.reschedule(r1f->id, 40);
    EXPECT_EQ(r2s->getActualTimeFire(), 40);
    EXPECT_EQ(r2f->getActualTimeFire(), 50);
}

TEST(EventReschedulingTest, SuccessorCannotPrecedePredecessor) {
    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto taskA = WorkflowFactory::CreateOneSimpleTaskNoEdges("A", 0, 0);
    auto taskB = WorkflowFactory::CreateOneSimpleTaskNoEdges("B", 0, 0);

    auto af = EventFactory::createTaskFinishEvent(taskA, p1, 10);
    auto bs = EventFactory::createTaskStartEvent(taskB, p1, 10);
    auto bf = EventFactory::createTaskFinishEvent(taskB, p1, 20);
    bs->addPredecessorInPlanning(af);
    bf->addPredecessorInPlanning(bs);

    events.insert(af); events.insert(bs);

    // Try to reschedule B-s to T=5, while A-f is still at T=10
    events.reschedule("B-s", 5);

    EXPECT_GE(bs->getActualTimeFire(), af->getActualTimeFire());
}

TEST(enforceSuccessorConstraintsTest, DiamondBothBranchesPushForward) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 10);
    auto a_s = EventFactory::createTaskStartEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_f->addPredecessorInPlanning(a_s);

    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);

    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);

    events.insert(a_f);
    //a-f is unattached
    events.reschedule(a_f->id, 21);

    EXPECT_EQ(a_c_w_s->getActualTimeFire(),10);
    EXPECT_EQ(x_b_r_f->getActualTimeFire(),25);
    EXPECT_EQ(b_s->getActualTimeFire(),30);

    a_c_w_s->addPredecessorInPlanning(a_f);
    x_b_r_s->addPredecessorInPlanning(a_f);

     events.insert(a_s);
    events.insert(a_c_w_s);    events.insert(a_c_w_f);
    events.insert(a_c_r_s);    events.insert(a_c_r_f);
    events.insert(x_b_r_s);    events.insert(x_b_r_f);
    events.insert(y_b_r_s);    events.insert(y_b_r_f);
    events.insert(b_s);    events.insert(b_f);
    events.insert(b_m_w_s);    events.insert(b_m_w_f);
    events.insert(c_s);    events.insert(c_f);
    events.insert(m_s);    events.insert(m_f);


    EXPECT_NO_THROW({

        //adding predecessors in planning after reschedule does propagate rescheduling
        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),10);
       EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),25);
       EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),30);
       EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),40);


        //we explicitly repair the constraints before firing a-f
        std::vector<TimeShift> repair;
        a_f->enforceSuccessorConstraints(repair);
        for (auto& s : repair) {
            events.reschedulePure(s.ev->id, s.newTime);
        }

        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),21);
        EXPECT_EQ(events.find("a-c-w-f").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("a-c-r-s").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("a-c-r-f").get()->getActualTimeFire(),31);
        EXPECT_EQ(events.find("c-s").get()->getActualTimeFire(),31);
        EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),51);

        EXPECT_EQ(events.find("x-b-r-s").get()->getActualTimeFire(),21);
        EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("y-b-r-s").get()->getActualTimeFire(),26);
        EXPECT_EQ(events.find("y-b-r-f").get()->getActualTimeFire(),31);

        EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),31);
        EXPECT_EQ(events.find("b-f").get()->getActualTimeFire(),36);
        EXPECT_EQ(events.find("b-m-w-s").get()->getActualTimeFire(),36);
        EXPECT_EQ(events.find("b-m-w-f").get()->getActualTimeFire(),41);

        EXPECT_EQ(events.find("m-s").get()->getActualTimeFire(),51);
        EXPECT_EQ(events.find("m-f").get()->getActualTimeFire(),56);

    });

}

TEST(enforceSuccessorConstraintsTest, DiamondPullsBack) {

    auto p1 = ClusterFactory::createSingleProcessor(80, 1.0);
    auto p2 = ClusterFactory::createSingleProcessor(80, 1.0);

     graph_t* g = new_graph();

    auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
    auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
    auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
    auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
    auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
    auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

    new_edge(g, a, c, 20, nullptr);
    new_edge(g, b, m, 20, nullptr);
    new_edge(g, c, m, 20,nullptr);
    new_edge(g, x, b, 20, nullptr);
    new_edge(g, y, b, 20, nullptr);

    auto a_f = EventFactory::createTaskFinishEvent(a, p1, 10);
    auto a_s = EventFactory::createTaskStartEvent(a, p1, 5);

    auto a_c_w_s = EventFactory::createWriteStartEvent(a->out_edges.at(0), p1, 10);
    auto a_c_w_f = EventFactory::createWriteFinishEvent(a->out_edges.at(0), p1, 15);
    auto a_c_r_s = EventFactory::createReadStartEvent(a->out_edges.at(0), p2, 15);
    auto a_c_r_f = EventFactory::createReadFinishEvent(a->out_edges.at(0), p2, 20);

    auto  x_b_r_s = EventFactory::createReadStartEvent(x->out_edges.at(0), p1, 20);
    auto  x_b_r_f = EventFactory::createReadFinishEvent(x->out_edges.at(0), p1, 25);
    auto  y_b_r_s = EventFactory::createReadStartEvent(y->out_edges.at(0), p1, 25);
    auto  y_b_r_f = EventFactory::createReadFinishEvent(y->out_edges.at(0), p1, 30);

    auto b_s = EventFactory::createTaskStartEvent(b, p1, 30);
    auto b_f = EventFactory::createTaskFinishEvent(b, p1, 35);
    auto b_m_w_s = EventFactory::createWriteStartEvent(b->out_edges.at(0), p1, 35);
    auto b_m_w_f = EventFactory::createWriteFinishEvent(b->out_edges.at(0), p1, 40);

    auto c_s = EventFactory::createTaskStartEvent(c, p2, 20);
    auto c_f = EventFactory::createTaskFinishEvent(c, p2, 40);

    auto m_s = EventFactory::createTaskStartEvent(m, p1, 40);
    auto m_f = EventFactory::createTaskFinishEvent(m, p1, 45);

    a_f->addPredecessorInPlanning(a_s);

    a_c_w_f->addPredecessorInPlanning(a_c_w_s);
    a_c_r_s->addPredecessorInPlanning(a_c_w_f);
    a_c_r_f->addPredecessorInPlanning(a_c_r_s);

    x_b_r_f->addPredecessorInPlanning(x_b_r_s);
    y_b_r_s->addPredecessorInPlanning(x_b_r_f);
    y_b_r_f->addPredecessorInPlanning(y_b_r_s);
    b_s->addPredecessorInPlanning(x_b_r_f);
    b_s->addPredecessorInPlanning(y_b_r_f);
    b_f->addPredecessorInPlanning(b_s);
    b_m_w_s->addPredecessorInPlanning(b_f);
    b_m_w_f->addPredecessorInPlanning(b_m_w_s);
    m_s->addPredecessorInPlanning(b_m_w_f);
    m_f->addPredecessorInPlanning(m_s);
    c_s->addPredecessorInPlanning(a_c_r_f);
    c_f->addPredecessorInPlanning(c_s);
    m_s->addPredecessorInPlanning(c_f);

    events.insert(a_f);
    //a-f is unattached
    events.reschedule(a_f->id, 7);

    EXPECT_EQ(a_c_w_s->getActualTimeFire(),10);
    EXPECT_EQ(x_b_r_f->getActualTimeFire(),25);
    EXPECT_EQ(b_s->getActualTimeFire(),30);

    a_c_w_s->addPredecessorInPlanning(a_f);
    x_b_r_s->addPredecessorInPlanning(a_f);

     events.insert(a_s);
    events.insert(a_c_w_s);    events.insert(a_c_w_f);
    events.insert(a_c_r_s);    events.insert(a_c_r_f);
    events.insert(x_b_r_s);    events.insert(x_b_r_f);
    events.insert(y_b_r_s);    events.insert(y_b_r_f);
    events.insert(b_s);    events.insert(b_f);
    events.insert(b_m_w_s);    events.insert(b_m_w_f);
    events.insert(c_s);    events.insert(c_f);
    events.insert(m_s);    events.insert(m_f);

    //events.canPullEarlier= true;

    EXPECT_NO_THROW({

        //adding predecessors in planning after reschedule does propagate rescheduling
        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),10);
       EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),25);
       EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),30);
       EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),40);


        //we explicitly repair the constraints before firing a-f
        std::vector<TimeShift> repair;
        a_f->enforceSuccessorConstraints(repair);
        for (auto& s : repair) {
            events.reschedulePure(s.ev->id, s.newTime);
        }

        EXPECT_EQ(events.find("a-c-w-s").get()->getActualTimeFire(),7);
        EXPECT_EQ(events.find("a-c-w-f").get()->getActualTimeFire(),12);
        EXPECT_EQ(events.find("a-c-r-s").get()->getActualTimeFire(),12);
        EXPECT_EQ(events.find("a-c-r-f").get()->getActualTimeFire(),17);
        EXPECT_EQ(events.find("c-s").get()->getActualTimeFire(),17);
        EXPECT_EQ(events.find("c-f").get()->getActualTimeFire(),37);

        EXPECT_EQ(events.find("x-b-r-s").get()->getActualTimeFire(),7);
        EXPECT_EQ(events.find("x-b-r-f").get()->getActualTimeFire(),12);
        EXPECT_EQ(events.find("y-b-r-s").get()->getActualTimeFire(),12);
        EXPECT_EQ(events.find("y-b-r-f").get()->getActualTimeFire(),17);

        EXPECT_EQ(events.find("b-s").get()->getActualTimeFire(),17);
        EXPECT_EQ(events.find("b-f").get()->getActualTimeFire(),22);
        EXPECT_EQ(events.find("b-m-w-s").get()->getActualTimeFire(),22);
        EXPECT_EQ(events.find("b-m-w-f").get()->getActualTimeFire(),27);

        EXPECT_EQ(events.find("m-s").get()->getActualTimeFire(),37);
        EXPECT_EQ(events.find("m-f").get()->getActualTimeFire(),42);

    });

}


TEST(enforceSuccessorConstraintsTest, SimpleSuccessorPushedForward)
{
    auto first = WorkflowFactory::CreateOneSimpleTaskNoEdges("first", 100.0, 50);
    auto second = WorkflowFactory::CreateOneSimpleTaskNoEdges("second", 100.0, 50);

    auto p = ClusterFactory::createSingleProcessor(80, 1.0);

    auto first_s = EventFactory::createTaskStartEvent(first, p, 5);
    auto first_f = EventFactory::createTaskFinishEvent(first, p, 10);
    auto second_s = EventFactory::createTaskStartEvent(second, p, 10);
    auto second_f = EventFactory::createTaskFinishEvent(second, p, 20);
    second_f->addPredecessorInPlanning(second_s);
    first_f->addPredecessorInPlanning(first_s);
    second_s->addPredecessorInPlanning(first_f);

    EXPECT_NO_THROW({
        std::vector<TimeShift> outShifts;
        second_s->enforceSuccessorConstraints(outShifts);
        EXPECT_EQ(outShifts.size(), 0);
    });
}