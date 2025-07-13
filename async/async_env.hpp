#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <liburing.h>
#include <memory>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "async/async_task.hpp"

namespace leveldb {

namespace io {
class AsyncTask;
class AsyncIOTask;

struct AsyncContext {
 public:
  AsyncContext();
  ~AsyncContext();

  // 初始化io_uring
  bool Initialize(unsigned entries = 256);

  // 运行事件循环
  void Run();

  // 停止事件循环
  void Stop();

  // 提交任务
  void Submit(AsyncTask* task);

  // only this thread;
  void Submit(AsyncIOTask* task);
  void Submit(std::chrono::milliseconds timeout, AsyncTask* task);

  // 获取io_uring实例
  io_uring* GetRing() { return &ring_; }

  // 检查是否正在运行
  bool IsRunning() const { return running_.load(); }

 private:
  // io_uring相关
  io_uring ring_;
  int event_fd_;
  std::thread::id thread_id_;

  // 运行状态
  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};

  // 任务队列
  io::TypedIntrusiveMPSCQueue<AsyncTask> pending_tasks_;
  std::priority_queue<
      std::pair<std::chrono::steady_clock::time_point, AsyncTask*>,
      std::vector<std::pair<std::chrono::steady_clock::time_point, AsyncTask*>>,
      std::greater<
          std::pair<std::chrono::steady_clock::time_point, AsyncTask*>>>
      pending_tasks_with_timeout_;
  std::unordered_map<AsyncIOTask*, AsyncIOTask*> pending_io_tasks_;
  std::vector<AsyncTask*> pending_tasks2_;

  std::vector<AsyncTask*> loop_running_tasks_;

  // 处理完成的请求
  void ProcessCompletions();

  // 处理定时器任务
  void ProcessTimerTasks();

  bool need_submit_ = false;
  __kernel_timespec timeout_ts_;

  // 设置定时器
  __kernel_timespec* SetTimer();
};

inline thread_local AsyncContext* local_context = nullptr;
struct AsyncEnvOption;
class AsyncWritableFile {
 public:
  ~AsyncWritableFile() = default;

  void Append(AsyncContext*, const Slice& data, AsyncIOTask* task);
  void Sync(AsyncContext*, AsyncIOTask* task);
  void Close(AsyncContext*, AsyncIOTask* task);
};

class AsyncReadableFile {
 public:
  ~AsyncReadableFile() = default;

  void Read(AsyncContext*, uint64_t offset, Slice& result, AsyncIOTask* task);
  void Sync(AsyncContext*, AsyncIOTask* task);
  void Close(AsyncContext*, AsyncIOTask* task);
};

struct AsyncEnv {
 public:
  AsyncContext* GetContext() const;

  void CreateWritableFile(AsyncContext* context, const std::filesystem::path&,
                          AsyncWritableFile** ptr, AsyncIOTask*);
  void CreateReadFile(AsyncContext* context, const std::filesystem::path&,
                      AsyncWritableFile** ptr, AsyncIOTask*);

  void Schedule(AsyncContext* context, AsyncTask* task);
  void Sleep(AsyncContext* context, std::chrono::milliseconds duration,
             AsyncTask* task);

  void CreateDir(AsyncContext* context, const std::filesystem::path& dirs,
                 AsyncIOTask* task);
  void DeleteFile(AsyncContext* context, const std::filesystem::path&,
                  AsyncIOTask* task);
  void DeleteDir(AsyncContext* context, const std::filesystem::path&,
                 AsyncIOTask* task);
  void RenameFile(AsyncContext* context, const std::filesystem::path&,
                  AsyncIOTask* task);
};

}  // namespace io
}  // namespace leveldb