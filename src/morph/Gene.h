// Author: Tim Diels <timdiels.m@gmail.com>

#pragma once

#include <string>
#include "ublas.h"

// TODO rm this class, unused
class Gene
{
public:
	size_type index; // the index the gene has in Gene x T matrices
	std::string name;
};