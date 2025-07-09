#pragma once

#include <chrono>

#include "leveldb/slice.h"
#include "leveldb/status.h"
namespace leveldb {

namespace io {

class AsyncTask;
class IOTask;

class AsyncExecutor {
 public:
  virtual ~AsyncExecutor() = default;

  virtual void Submit(AsyncTask* task) = 0;
  virtual void Sleep(AsyncTask* task, std::chrono::milliseconds duration) = 0;
  virtual void DoIO(IOTask* task) = 0;
};

class AsyncTask {
 public:
  virtual ~AsyncTask() = default;
  virtual void Execute() = 0;
};

enum class IOType { Invalid, Read, Write, Sync, Open, Close };

struct ReadParam {
  int fd_;
  uint64_t offset_;
  Slice data_;
};

struct WriteParam {
  int fd_;
  uint64_t offset_;
  Slice data_;
};

struct SyncParam {
  int fd_;
};

struct OpenParam {
  Slice path_;
  int flags_;
  int mode_;
};

struct CloseParam {
  int fd_;
};

class IOTask : public AsyncTask {
 public:
  IOType type() const { return type_; }
  IOTask() : type_(IOType::Invalid) {}

  const ReadParam& read_param() const {
    assert(type_ == IOType::Read);
    return read_;
  }

  const WriteParam& write_param() const {
    assert(type_ == IOType::Write);
    return write_;
  }

  const SyncParam& sync_param() const {
    assert(type_ == IOType::Sync);
    return sync_;
  }

  const OpenParam& open_param() const {
    assert(type_ == IOType::Open);
    return open_;
  }

  const CloseParam& close_param() const {
    assert(type_ == IOType::Close);
    return close_;
  }

  void InitRead(int fd, uint64_t offset, Slice data) {
    type_ = IOType::Read;
    read_.fd_ = fd;
    read_.offset_ = offset;
    read_.data_ = data;
  }

  void InitWrite(int fd, uint64_t offset, Slice data) {
    type_ = IOType::Write;
    write_.fd_ = fd;
    write_.offset_ = offset;
    write_.data_ = data;
  }

  void InitSync(int fd) {
    type_ = IOType::Sync;
    sync_.fd_ = fd;
  }

  void InitOpen(Slice path, int flags, int mode) {
    type_ = IOType::Open;
    open_.path_ = path;
    open_.flags_ = flags;
    open_.mode_ = mode;
  }

  void InitClose(int fd) {
    type_ = IOType::Close;
    close_.fd_ = fd;
  }

  void resetIO() { type_ = IOType::Invalid; }

  Status status() const { return status_; }
  void setStatus(const Status& status) { status_ = status; }

 private:
  IOType type_;
  union {
    ReadParam read_;
    WriteParam write_;
    SyncParam sync_;
    OpenParam open_;
    CloseParam close_;
  };
  Status status_;
};

}  // namespace io
}  // namespace leveldb