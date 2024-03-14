#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct SQLiteOpenOptions {
    // access mode
    AccessMode access_mode = AccessMode::READ_WRITE;
    // busy time-out in ms
    idx_t busy_timeout = 5000;
    // journal mode
    string journal_mode;
};

}

}
