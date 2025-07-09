#pragma once

#include "leveldb/io/async_io.h"

#include "db_impl.h"

namespace leveldb {

class CompactionTask final : public io::IOTask {};
class FlushTask final : public io::IOTask {};

}  // namespace leveldb
