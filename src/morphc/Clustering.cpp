// Author: Tim Diels <timdiels.m@gmail.com>

#include "Clustering.h"
#include "util.h"
#include <boost/spirit/include/qi.hpp>
#include <boost/function_output_iterator.hpp>

using namespace std;

namespace MORPHC {

Clustering::Clustering(CONFIG::Clustering clustering_, shared_ptr<GeneExpression> gene_expression_)
:	name(clustering_.get_name()), gene_expression(gene_expression_)
{
	std::vector<size_type> genes;

	// Load
	read_file(clustering_.get_path(), [this, &genes](const char* begin, const char* end) {
		using namespace boost::spirit::qi;
		using namespace boost::fusion;

		std::map<std::string, Cluster*> cluster_map;
		auto on_cluster_item = [this, &cluster_map, &genes](const boost::fusion::vector<std::string, std::string>& item) {
			auto gene_name = at_c<0>(item);
			if (!gene_expression->has_gene(gene_name)) {
				// Note: in case clusterings are not generated by us, they might contain genes that we don't know
#ifndef NDEBUG
				cerr << "Warning: gene missing from expression matrix: " << gene_name << endl;
#endif
				return;
			}
			auto cluster_id = at_c<1>(item);
			auto it = cluster_map.find(cluster_id);
			if (it == cluster_map.end()) {
				clusters.emplace_back(cluster_id);
				it = cluster_map.emplace(cluster_id, &clusters.back()).first;
			}
			auto index = gene_expression->get_gene_index(gene_name);
			it->second->add(index);
			genes.emplace_back(index);
		};
		phrase_parse(begin, end, (as_string[lexeme[+(char_-space)]] > as_string[lexeme[+(char_-eol)]])[on_cluster_item] % eol, blank);
		return begin;
	});

	// Group together unclustered genes
	sort(genes.begin(), genes.end());
	clusters.emplace_back("unclustered");
	auto& cluster = clusters.back();
	auto& all_genes = gene_expression->get_genes(); // Note: must be ordered
	auto add_to_cluster = boost::make_function_output_iterator([&cluster](const size_type& gene) {
		cluster.add(gene);
	});
	set_difference(all_genes.begin(), all_genes.end(), genes.begin(), genes.end(), add_to_cluster);
	if (cluster.empty()) {
		clusters.pop_back();
	}
}

const std::vector<Cluster>& Clustering::get_clusters() const {
	return clusters;
}

GeneExpression& Clustering::get_source() const {
	return *gene_expression;
}

}
