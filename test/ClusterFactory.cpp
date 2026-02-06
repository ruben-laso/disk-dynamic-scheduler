#include "cluster.hpp"

class ClusterFactory {
public:
    // A simple cluster where all nodes are identical
    static Cluster* CreateHomogeneous(int numProcs, double mem, double speed) {
        auto* cluster = new Cluster();

        for (int i = 0; i < numProcs; ++i) {
            auto p = std::make_shared<Processor>(mem, speed, i);
            p->name = "Proc_" + std::to_string(i);
            p->setAvailableMemory(mem);
            p->setAfterAvailableMemory(mem);
            p->writeSpeedDisk=10;
            p->readSpeedDisk=10;
            cluster->addProcessor(p);
        }
        return cluster;
    }

    // A cluster with one "Thick Node" with much memory and slow processor and "Tiny Node" with little memory and fast processor.
    static Cluster* CreateBottleneckCluster() {
        auto* cluster = new Cluster();
        
        // The Super Node
        auto p0 = std::make_shared<Processor>(1000.0, 1.0, 0);
        p0->setAvailableMemory(1000.0);
        p0->setAfterAvailableMemory(1000.0);
        cluster->addProcessor(p0);

        // The Weak Node
        auto p1 = std::make_shared<Processor>(25.0, 100.0, 1);
        p1->setAvailableMemory(25.0);
        p1->setAfterAvailableMemory(25.0);
        cluster->addProcessor(p1);

        p0->writeSpeedDisk=10;
        p0->readSpeedDisk=10;
        p1->writeSpeedDisk=10;
        p1->readSpeedDisk=10;

        return cluster;
    }

    static std::shared_ptr<Processor> createSingleProcessor(double mem, double speed) {
        auto p = std::make_shared<Processor>(mem, speed, 0);
        p->name = "SingleProc";
        p->setAvailableMemory(mem);
        p->setAfterAvailableMemory(mem);
        p->writeSpeedDisk=10;
        p->readSpeedDisk=10;
        return p;
    }
};