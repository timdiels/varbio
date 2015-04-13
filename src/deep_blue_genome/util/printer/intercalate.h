// Author: Tim Diels <timdiels.m@gmail.com>

#pragma once

#include <sstream>
#include <deep_blue_genome/util/template_magic.h>

namespace DEEP_BLUE_GENOME {
// TODO do we ever need URef&& here? Can we ever move something? Otherwise const T& would work fine (and would pass fine through lambdas)

/**
 * Private things
 */
namespace impl {
	template <class Delimiter>
	void intercalate_(std::ostream& out, Delimiter&& delimiter) {
	}

	template <class Delimiter, class Item, class... Items>
	void intercalate_(std::ostream& out, Delimiter&& delimiter, Item&& item, Items&&... items)
	{
		out << delimiter << std::forward<Item>(item);
		intercalate_(out, delimiter, std::forward<Items>(items)...);
	}
}

/**
 * Intercalate a range of items with a delimiter.
 *
 * Like haskell intercalate.
 *
 * Args: delimiter, range of items
 */
template <class Delimiter, class Range>
std::string intercalate(Delimiter&& delimiter, Range&& items)
{
	std::ostringstream out;

	bool first = true;
	for (auto&& item : items) {
		if (!first) {
			out << delimiter;
		}
		out << std::forward<decltype(item)>(item);
		first = false;
	}

	return out.str();
}

/**
 * Intercalate a variadic argument list with a delimiter.
 *
 * Like haskell intercalate.
 *
 * Args: delimiter, item1, item2, ..., itemN.
 */
template <class Delimiter, class Item, class... Items>
std::string intercalate_(Delimiter&& delimiter, Item&& first_item, Items&&... items) {
	std::ostringstream out;
	out << std::forward<Item>(first_item);
	impl::intercalate_(out, delimiter, std::forward<Items>(items)...);
	return out.str();
}

} // end namespace