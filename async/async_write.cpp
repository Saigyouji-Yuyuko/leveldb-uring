#include "async_write.hpp"

#include "db/db_impl.h"

#include "async/async_env.hpp"
namespace leveldb::io {

void AsyncWrite::Push(WriteCtx* ctx) {
  auto old = this->head_.exchange(ctx);
  ctx->next_ = old;
  if (old != nullptr) {
    old->prev_ = ctx;
  } else {
    auto writeTask = new WriteTask(this->db, ctx);
    this->db->options_.async_option.env_->Schedule(nullptr, writeTask);
  }
  return;
}

void WriteTask::Execute(AsyncContext* ctx) {}

}  // namespace leveldb::io