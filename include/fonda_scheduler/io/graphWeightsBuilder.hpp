/*
 * MongoDBReader.hpp
 *
 *  Created on: 07.04.2022
 *      Author: Fabian Brandt-Tumescheit, fabratu
 */

#ifndef FONDA_GRAPHWEIGHTSBUILDER_HPP_
#define FONDA_GRAPHWEIGHTSBUILDER_HPP_

#include <string>
#include <unordered_map>

#include "cluster.hpp"
#include "fonda_scheduler/options.hpp"

namespace Fonda {
void fillGraphWeightsFromExternalSource(const graph_t* graphMemTopology,
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> workflow_rows,
    Cluster* cluster, int memShorteningDivision, double ioShorteningCoef, const fonda::Options& options);
void retrieveEdgeWeights(const graph_t* graphMemTopology, const fonda::Options& options);
Cluster* buildClusterFromCsv(const std::string& file, const fonda::Options& options);
}

#endif