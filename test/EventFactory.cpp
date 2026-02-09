#include "fonda_scheduler/common.hpp"


class EventFactory {
public:
    static std::shared_ptr<Event> createTaskStartEvent(vertex_t* task, const std::shared_ptr<Processor>& processor, double expectedTimeFire)
    {        return Event::createEvent(task, nullptr, OnTaskStart, processor, expectedTimeFire, expectedTimeFire, false, task->name+"-s");
    }
    static std::shared_ptr<Event> createTaskFinishEvent(vertex_t* task, const std::shared_ptr<Processor>& processor, double expectedTimeFire)
    {        return Event::createEvent(task, nullptr, OnTaskFinish, processor, expectedTimeFire, expectedTimeFire, false, task->name+"-f");
    }

    static std::shared_ptr<Event> createWriteStartEvent(edge_t* edge, const std::shared_ptr<Processor>& processor, double expectedTimeFire)
    {        return Event::createEvent(nullptr, edge, OnWriteStart, processor, expectedTimeFire, expectedTimeFire, false, buildEdgeName(edge)+"-w-s");
    }
    static std::shared_ptr<Event> createWriteFinishEvent(edge_t* edge, const std::shared_ptr<Processor>& processor, double expectedTimeFire)
    {        return Event::createEvent(nullptr, edge, OnWriteFinish, processor, expectedTimeFire, expectedTimeFire, false, buildEdgeName(edge)+"-w-f");
    }
    static std::shared_ptr<Event> createReadStartEvent(edge_t* edge, const std::shared_ptr<Processor>& processor, double expectedTimeFire)
    {        return Event::createEvent(nullptr, edge, OnReadStart, processor, expectedTimeFire, expectedTimeFire, false, buildEdgeName(edge)+"-r-s");
    }
    static std::shared_ptr<Event> createReadFinishEvent(edge_t* edge, const std::shared_ptr<Processor>& processor, double expectedTimeFire)
    {        return Event::createEvent(nullptr, edge, OnReadFinish, processor, expectedTimeFire, expectedTimeFire, false, buildEdgeName(edge)+"-r-f");
    }


    static  std::vector<std::shared_ptr<Event>> createAXYBCMEvents( double taskTime, double taskMemory, double edgeWeight, std::shared_ptr<Processor> p1, std::shared_ptr<Processor> p2){

        graph_t* g = new_graph();

        auto a = new_vertex2Weights(g, "a", 100.0, 50, nullptr);
        auto b = new_vertex2Weights(g, "b", 100.0, 50, nullptr);
        auto c = new_vertex2Weights(g, "c", 100.0, 50, nullptr);
        auto m = new_vertex2Weights(g, "m", 100.0, 50, nullptr);
        auto x = new_vertex2Weights(g, "x", 100.0, 50, nullptr);
        auto y = new_vertex2Weights(g, "y", 100.0, 50, nullptr);

        new_edge(g, a, c, edgeWeight, nullptr);
        new_edge(g, b, m, edgeWeight, nullptr);
        new_edge(g, c, m, edgeWeight, nullptr);
        new_edge(g, x, b, edgeWeight, nullptr);
        new_edge(g, y, b, edgeWeight, nullptr);

        auto a_f = createTaskFinishEvent(a, p1, 11);

        auto a_c_w_s = createWriteStartEvent(a->out_edges.at(0), p1, 11);
        auto a_c_w_f = createWriteFinishEvent(a->out_edges.at(0), p1, 12);
        auto a_c_r_s = createReadStartEvent(a->out_edges.at(0), p2, 12);
        auto a_c_r_f = createReadFinishEvent(a->out_edges.at(0), p2, 13);

        auto  x_b_r_s = createReadStartEvent(x->out_edges.at(0), p1, 11);
        auto  x_b_r_f = createReadFinishEvent(x->out_edges.at(0), p1, 12);
        auto  y_b_r_s = createReadStartEvent(y->out_edges.at(0), p1, 11);
        auto  y_b_r_f = createReadFinishEvent(y->out_edges.at(0), p1, 12);

        auto b_s = createTaskStartEvent(b, p1, 12);
        auto b_f = createTaskFinishEvent(b, p1, 22);
        auto b_m_w_s = createWriteStartEvent(b->out_edges.at(0), p1, 22);
        auto b_m_w_f = createWriteFinishEvent(b->out_edges.at(0), p1, 23);
        auto m_s = createTaskStartEvent(m, p1, 22);
        auto m_f = createTaskFinishEvent(m, p1, 32);

        a_c_w_s->addPredecessorInPlanning(a_f);
        a_c_w_f->addPredecessorInPlanning(a_c_w_s);
        a_c_r_s->addPredecessorInPlanning(a_c_w_f);
        a_c_r_f->addPredecessorInPlanning(a_c_r_s);
        x_b_r_s->addPredecessorInPlanning(a_f);
        x_b_r_f->addPredecessorInPlanning(x_b_r_s);
        y_b_r_s->addPredecessorInPlanning(a_f);
        y_b_r_f->addPredecessorInPlanning(y_b_r_s);
        b_s->addPredecessorInPlanning(x_b_r_f);
        b_s->addPredecessorInPlanning(y_b_r_f);
        b_f->addPredecessorInPlanning(b_s);
        b_m_w_s->addPredecessorInPlanning(b_f);
        b_m_w_f->addPredecessorInPlanning(b_m_w_s);
        m_s->addPredecessorInPlanning(b_m_w_f);
        m_f->addPredecessorInPlanning(m_s);

    }
};