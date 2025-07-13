#pragma once

#include <chrono>

#include "leveldb/slice.h"
#include "leveldb/status.h"
namespace leveldb {

namespace io {

struct AsyncEnv;
struct AsyncOption {
  bool enable_ = false;
  AsyncEnv* env_ = nullptr;
};

}  // namespace io
}  // namespace leveldb