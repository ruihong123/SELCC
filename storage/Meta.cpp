#include "Meta.h"
namespace DSMEngine {
DDSM** gallocators = NULL;
DDSM* default_gallocator = NULL;
size_t gThreadCount = 0;
size_t gParamBatchSize = 1000;
}
