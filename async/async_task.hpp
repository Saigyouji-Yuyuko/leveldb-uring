#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <variant>

#include "leveldb/slice.h"
#include "leveldb/status.h"

#include "async/intrusive_mpsc_queue.hpp"
namespace leveldb {

namespace io {
class AsyncContext;
class AsyncTask : public MPSCQueueNode {
 public:
  virtual void Execute(AsyncContext*) = 0;

  // Virtual destructor to ensure proper cleanup of derived classes
  virtual ~AsyncTask() = default;
};

enum class IOType : uint8_t {
  Invalid = 0,
  kOpen,
  kRead,
  kWrite,
  kSync,
  kClose,
  kCreatDir,
  kRename,
  kDeleteFile,
};

struct OpenParam {
  std::filesystem::path path;
  int flags;
  int mode;
};

struct ReadParam {
  int fd_;
  size_t offset_;
  Slice data_;
};

struct WriteParam {
  int fd_;
  size_t offset_;
  Slice data_;
};

struct SyncParam {
  int fd_;
  int flags_;
};

struct CloseParam {
  int fd_;
};

struct CreatDirParam {
  std::filesystem::path path;
};

struct RenameParam {
  std::filesystem::path old_path;
  std::filesystem::path new_path;
};

struct DeleteFileParam {
  std::filesystem::path path;
};

// Base class for all IO tasks
class AsyncIOTask : public AsyncTask {
 public:
  friend class AsyncEnv;
  virtual IOType GetType();

  void SetResult(Status status) { result_ = std::move(status); }
  Status GetResult() const { return result_; }

  void SetFileDescriptor(int fd) { res_ = fd; }
  int GetFileDescriptor() const { return res_; }

  // Result of read operation
  void SetBytesRead(size_t bytes) { bytes = bytes; }
  size_t GetBytesRead() const { return bytes; }

  // Result of write operation
  void SetBytesWritten(size_t bytes) { bytes = bytes; }
  size_t GetBytesWritten() const { return bytes; }

  const ReadParam& GetReadParam() const {
    assert(type_ == IOType::kRead);
    return std::get<ReadParam>(param_);
  }

  const WriteParam& GetWriteParam() const {
    assert(type_ == IOType::kWrite);
    return std::get<WriteParam>(param_);
  }

  const OpenParam& GetOpenParam() const {
    assert(type_ == IOType::kOpen);
    return std::get<OpenParam>(param_);
  }

  const SyncParam& GetSyncParam() const {
    assert(type_ == IOType::kSync);
    return std::get<SyncParam>(param_);
  }

  const CloseParam& GetCloseParam() const {
    assert(type_ == IOType::kClose);
    return std::get<CloseParam>(param_);
  }

  const CreatDirParam& GetCreatDirParam() const {
    assert(type_ == IOType::kCreatDir);
    return std::get<CreatDirParam>(param_);
  }

  const RenameParam& GetRenameParam() const {
    assert(type_ == IOType::kRename);
    return std::get<RenameParam>(param_);
  }

  const DeleteFileParam& GetDeleteFileParam() const {
    assert(type_ == IOType::kDeleteFile);
    return std::get<DeleteFileParam>(param_);
  }

 protected:
  void SetIOType(IOType type) { type_ = type; }
  IOType type_;
  std::variant<OpenParam, ReadParam, WriteParam, SyncParam, CloseParam,
               CreatDirParam, RenameParam, DeleteFileParam>
      param_;
  Status result_ = Status::OK();
  int res_ = 0;
  size_t bytes = 0;
};

}  // namespace io
}  // namespace leveldb