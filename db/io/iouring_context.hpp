#pragma once

#include <liburing.h>
#include <memory>
#include <stdexcept>

#include "leveldb/status.h"

namespace leveldb {
namespace io {

class IoUringContext {
 public:
  explicit IoUringContext(unsigned entries = 256) {
    if (io_uring_queue_init(entries, &ring_, 0) < 0) {
      status_ = Status::IOError("Failed to initialize io_uring");
    }
  }

  ~IoUringContext() { io_uring_queue_exit(&ring_); }

  io_uring* ring() { return &ring_; }

  // Non-copyable
  IoUringContext(const IoUringContext&) = delete;
  IoUringContext& operator=(const IoUringContext&) = delete;

  // Movable
  IoUringContext(IoUringContext&& other) noexcept : ring_(other.ring_) {
    other.ring_.ring_fd = -1;
  }
  IoUringContext& operator=(IoUringContext&& other) noexcept {
    if (this != &other) {
      io_uring_queue_exit(&ring_);
      ring_ = other.ring_;
      other.ring_.ring_fd = -1;
    }
    pthread_mutex_lock(&mutex_);
    return *this;
  }

 private:
  io_uring ring_;
  Status status_;
  pthread_mutex_t mutex_ = PTHREAD_MUTEX_INITIALIZER;
  
};

}  // namespace io
}  // namespace leveldb