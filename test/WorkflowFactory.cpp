#include "graph.hpp"

class WorkflowFactory {
public:
    // Creates: (A) -> (B) -> (C)
    static graph_t* CreateChain(int length, double taskTime, double taskMemory, double edgeWeight) {
        graph_t* g = new_graph();
        vertex_t* prev = nullptr;
        for (int i = 0; i < length; ++i) {
            vertex_t* curr = new_vertex2Weights(g, ("Task_" + std::to_string(i)).c_str(), taskTime, taskMemory, nullptr);
            if (prev) {
                new_edge(g, prev, curr, edgeWeight, nullptr);
            }
            prev = curr;
        }
        enforce_single_source_and_target(g, "_chain");
        return g;
    }

    // Creates: (A) -> (B)
    //             -> (C) or more tasks depending on breadth
    static graph_t* CreateFork(int breadth, double taskTime, double taskMemory, double edgeWeight) {
        graph_t* g = new_graph();

        vertex_t* head = new_vertex2Weights(g, "Task_Head", taskTime, taskMemory, nullptr);
        g->source = head;

        for (int i = 0; i < breadth; ++i) {
            vertex_t* curr = new_vertex2Weights(g, ("Task_" + std::to_string(i)).c_str(), taskTime, taskMemory, nullptr);
            if (head) {
                new_edge(g, head, curr, edgeWeight, nullptr);
            }
            g->target = curr;
        }

      //  enforce_single_source_and_target(g, "");
        return g;
    }

    // Creates: a join with bradth number of tasks sinking into the sink task
    static graph_t* CreateThickDiamond(int breadth, double taskTime, double taskMemory, double edgeWeight) {
        graph_t* g = new_graph();

        vertex_t* sink = new_vertex2Weights(g, "Sink", taskTime, taskMemory, nullptr);
        g->target = sink;

        vertex_t* head = new_vertex2Weights(g, "Source", taskTime, taskMemory, nullptr);
        g->source = head;

        for (int i = 0; i < breadth; ++i) {
            vertex_t* curr = new_vertex2Weights(g, ("Task_" + std::to_string(i)).c_str(), taskTime, taskMemory, nullptr);

            new_edge(g, curr, sink, edgeWeight, nullptr);
            new_edge(g, head, curr, edgeWeight, nullptr);

        }

        enforce_single_source_and_target(g, "");
        return g;
    }

    // Creates: a join with bradth number of tasks sinking into the sink task
    static graph_t* CreateLongDiamond(double taskTime, double taskMemory, double edgeWeight) {
        graph_t* g = new_graph();

        vertex_t* sink = new_vertex2Weights(g, "Sink", taskTime, taskMemory, nullptr);
        g->target = sink;

        vertex_t* head = new_vertex2Weights(g, "Source", taskTime, taskMemory, nullptr);
        g->source = head;

        for (int i = 0; i < 2; ++i) {
            vertex_t* curr1 = new_vertex2Weights(g, ("Branch_" + std::to_string(i)+"_Task_1").c_str(), taskTime, taskMemory, nullptr);
            vertex_t* curr2= new_vertex2Weights(g, ("Branch_" + std::to_string(i)+"_Task_2").c_str(), taskTime, taskMemory, nullptr);
            vertex_t* curr3 = new_vertex2Weights(g, ("Branch_" + std::to_string(i)+"_Task_3").c_str(), taskTime, taskMemory, nullptr);

            new_edge(g, head, curr1, edgeWeight, nullptr);
            new_edge(g, curr1, curr2, edgeWeight, nullptr);
            new_edge(g, curr2, curr3, edgeWeight, nullptr);
            new_edge(g, curr3, sink, edgeWeight, nullptr);
        }

        enforce_single_source_and_target(g, "");

        for (auto vertices_by_id : g->vertices_by_id) {
            vertices_by_id.second->factorForRealExecution = 1;
        }

        auto e = g->first_edge;
        while (e != nullptr) {
            e->factorForRealExecutionRead = 1;
            e->factorForRealExecutionWrite = 1;
            e = e->next;
        }
        return g;
    }




    // Creates a "Diamond" - useful for testing parallel execution limits
    static graph_t* CreateDiamondWithQuarterSides( double taskTime, double taskMemory, double edgeWeight) {
        graph_t* g = new_graph();
        auto* start = new_vertex2Weights(g, "Source", taskTime, taskMemory,  nullptr);
        auto* left  = new_vertex2Weights(g, "Left", taskTime/4, 22,  nullptr);
        auto* right = new_vertex2Weights(g, "Right", taskTime/4, 22,  nullptr);
        auto* end   = new_vertex2Weights(g, "Sink", taskTime, taskMemory, nullptr);

        left->swapRate=50;
        right->swapRate=50;
        start->swapRate= end->swapRate=0.1;

        new_edge(g, start, left, edgeWeight, nullptr);
        new_edge(g, start, right, edgeWeight, nullptr);
        new_edge(g, left, end, edgeWeight, nullptr);
        new_edge(g, right, end, edgeWeight, nullptr);

        enforce_single_source_and_target(g, "_diamond");
        return g;
    }

    // Creates: Pred1-> (A) -> Succ1
    //          Pred2 ->    -> Succ2
    //          Pred3->
    static  vertex_t* CreateOneTaskWith3Incoming2Outgoing( double taskTime, double taskMemory, double edgeWeight) {

        graph_t* g = new_graph();
        vertex_t* result = new_vertex2Weights(g, "A", taskTime, taskMemory, nullptr);

        vertex_t* pred1 = new_vertex2Weights(g, "Pred_1" , taskTime, taskMemory/2, nullptr);
        vertex_t* pred2 = new_vertex2Weights(g, "Pred_2", taskTime, taskMemory/2, nullptr);
        vertex_t* pred3 = new_vertex2Weights(g, "Pred_3" , taskTime, taskMemory/2, nullptr);

        new_edge(g, pred1, result, edgeWeight, nullptr);
        new_edge(g, pred2, result, edgeWeight, nullptr);
        new_edge(g, pred3, result, edgeWeight, nullptr);

        vertex_t* succ1 = new_vertex2Weights(g, "Succ_1" , taskTime, taskMemory/2, nullptr);
        vertex_t* succ2 = new_vertex2Weights(g, "Succ_2", taskTime, taskMemory/2, nullptr);

        new_edge(g, result, succ1, edgeWeight, nullptr);
        new_edge(g, result, succ2, edgeWeight, nullptr);

        return result;
    }

    // Creates: Pred1-> (A) -> Succ1
    static  vertex_t* CreateOneTaskWith1Incoming1Outgoing( double taskTime, double taskMemory, double edgeWeight) {

        graph_t* g = new_graph();
        vertex_t* result = new_vertex2Weights(g, "A", taskTime, taskMemory, nullptr);

        vertex_t* pred1 = new_vertex2Weights(g, "Pred_1" , taskTime, taskMemory/2, nullptr);
        new_edge(g, pred1, result, edgeWeight, nullptr);

        vertex_t* succ1 = new_vertex2Weights(g, "Succ_1" , taskTime, taskMemory/2, nullptr);
        new_edge(g, result, succ1, edgeWeight, nullptr);


        return result;
    }



    static  vertex_t* CreateOneSimpleTaskNoEdges( std::string taskName , double taskTime, double taskMemory) {

        graph_t* g = new_graph();
        vertex_t* result = new_vertex2Weights(g, taskName.c_str(), taskTime, taskMemory, nullptr);

        return result;
    }


};