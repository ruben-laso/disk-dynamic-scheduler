#include <gtest/gtest.h>
#include "WorkflowFactory.cpp"
#include "ClusterFactory.cpp"
#include "EventFactory.cpp"
#include "fonda_scheduler/SchedulerHeader.hpp"

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

        assert(a_c_w_s->getExpectedTimeFire()==21);
        assert(a_c_w_f->getExpectedTimeFire()==26);
        assert(a_c_r_s->getExpectedTimeFire()==26);
        assert(a_c_r_f->getExpectedTimeFire()==31);
        assert(c_s->getExpectedTimeFire()==31);
        assert(c_f->getExpectedTimeFire()==51);

        assert(x_b_r_s->getExpectedTimeFire()==21);
        assert(x_b_r_f->getExpectedTimeFire()==26);
        assert(y_b_r_s->getExpectedTimeFire()==26);
        assert(y_b_r_f->getExpectedTimeFire()==31);

        assert(b_s->getExpectedTimeFire()==31);
        assert(b_f->getExpectedTimeFire()==36);
        assert(b_m_w_s->getExpectedTimeFire()==36);
        assert(b_m_w_f->getExpectedTimeFire()==41);

        assert(m_s->getExpectedTimeFire()==51);
        assert(m_f->getExpectedTimeFire()==56);


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
