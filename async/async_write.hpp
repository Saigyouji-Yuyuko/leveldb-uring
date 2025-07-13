#pragma once
#include <atomic>

#include "leveldb/db.h"

#include "async/async_task.hpp"
namespace leveldb {
class DBImpl;
}

namespace leveldb::io {

struct WriteCtx {
  std::atomic<WriteCtx*> next_{nullptr};
  std::atomic<WriteCtx*> prev_{nullptr};
  WriteOptions option;
  WriteBatch* batch = nullptr;
  std::function<void(Status)> callback = nullptr;
};

class WriteTask final : public AsyncIOTask {
 public:
  WriteTask(DBImpl* db, WriteCtx*);
  void Execute(AsyncContext*) override;

  ~WriteTask() override;

 private:
  DBImpl* db = nullptr;
  std::vector<WriteCtx*> vec;
};

class AsyncWrite {
 public:
  void Push(WriteCtx*);

 private:
  DBImpl* db = nullptr;
  std::atomic<WriteCtx*> head_;
};

}  // namespace leveldb::io