#include "async_env.hpp"

#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <liburing.h>
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include "async_task.hpp"

namespace leveldb::io {

AsyncContext::AsyncContext() : event_fd_(-1) {
  // 初始化io_uring
  memset(&ring_, 0, sizeof(ring_));
}

AsyncContext::~AsyncContext() {
  Stop();

  if (event_fd_ >= 0) {
    close(event_fd_);
  }

  io_uring_queue_exit(&ring_);
}

bool AsyncContext::Initialize(unsigned entries) {
  // 初始化io_uring
  int ret = io_uring_queue_init(entries, &ring_, 0);
  if (ret < 0) {
    std::cerr << "Failed to initialize io_uring: " << strerror(-ret)
              << std::endl;
    return false;
  }

  // 创建事件文件描述符用于唤醒
  event_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (event_fd_ < 0) {
    std::cerr << "Failed to create eventfd: " << strerror(errno) << std::endl;
    return false;
  }

  return true;
}

void AsyncContext::Run() {
  if (running_.exchange(true)) {
    return;  // 已经在运行
  }

  should_stop_.store(false);
  local_context = this;
  this->thread_id_ = std::this_thread::get_id();

  // 准备事件监听
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe) {
    io_uring_prep_poll_multishot(sqe, event_fd_, POLLIN);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(0x1));  // 标记为事件fd
  }
  need_submit_ = true;

  std::cout << "AsyncContext started running..." << std::endl;

  while (!should_stop_.load()) {
    // 等待完成事件
    struct io_uring_cqe* cqe;

    auto ts = SetTimer();
    int ret = 0;
    if (need_submit_) {
      ret = io_uring_submit_and_wait_timeout(&ring_, &cqe, 1, ts, nullptr);
    } else {
      ret = io_uring_wait_cqes(&ring_, &cqe, 1, ts, nullptr);
    }
    need_submit_ = false;

    if (ret == 0) {
      ProcessCompletions();
    } else if (ret == -ETIME) {
      // 超时，继续循环
      continue;
    } else if (ret < 0 && ret != -EINTR) {
      std::cerr << "io_uring_wait_cqe failed: " << strerror(-ret) << std::endl;
      break;
    }
  }

  running_.store(false);
  local_context = nullptr;

  std::cout << "AsyncContext stopped." << std::endl;
}

void AsyncContext::Stop() {
  should_stop_.store(true);

  // 唤醒事件循环
  if (event_fd_ >= 0) {
    uint64_t val = 1;
    write(event_fd_, &val, sizeof(val));
  }
}

void AsyncContext::Submit(AsyncTask* task) {
  if (!task) return;

  if (std::this_thread::get_id() == this->thread_id_) {
    pending_tasks2_.push_back(task);
  } else {
    pending_tasks_.enqueue(task);
    int val = 1;
    write(event_fd_, &val, sizeof(val));
  }
}

void AsyncContext::Submit(AsyncIOTask* task) {
  if (!task) return;
  assert(std::this_thread::get_id() == this->thread_id_);
  this->pending_io_tasks_.emplace({task, nullptr});
  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  switch (task->GetType()) {
    case IOType::kOpen: {
      const auto& param = task->GetOpenParam();
      io_uring_prep_openat(sqe, AT_FDCWD, param.path.c_str(), param.flags,
                           param.flags);
      break;
    }
    case IOType::kRead: {
      const auto& param = task->GetReadParam();
      io_uring_prep_read(sqe, param.fd_, const_cast<char*>(param.data_.data()),
                         param.data_.size(), param.offset_);
      break;
    }
    case IOType::kWrite: {
      const auto& param = task->GetWriteParam();
      io_uring_prep_write(sqe, param.fd_, const_cast<char*>(param.data_.data()),
                          param.data_.size(), param.offset_);
      break;
    }
    case IOType::kSync: {
      const auto& param = task->GetSyncParam();
      io_uring_prep_fsync(sqe, param.fd_, param.flags_);
      break;
    }
    case IOType::kClose: {
      const auto& param = task->GetCloseParam();
      io_uring_prep_close(sqe, param.fd_);
      break;
    }
    case IOType::kRename: {
      const auto& param = task->GetRenameParam();
      io_uring_prep_rename(sqe, param.old_path.c_str(), param.new_path.c_str());
      break;
    }
    case IOType::kCreatDir: {
      const auto& param = task->GetCreatDirParam();
      io_uring_prep_mkdirat(sqe, AT_FDCWD, param.path.c_str(), 0755);
      break;
    }
    case IOType::kDeleteFile: {
      const auto& param = task->GetDeleteFileParam();
      io_uring_prep_unlink(sqe, param.path.c_str(), 0);
      break;
    }

    default:
      assert(false && "Unsupported IO type");
  }

  io_uring_sqe_set_data(sqe, task);
  need_submit_ = true;
}

void AsyncContext::Submit(std::chrono::milliseconds timeout, AsyncTask* task) {
  if (!task) return;
  assert(std::this_thread::get_id() == this->thread_id_);
  auto deadline = std::chrono::steady_clock::now() + timeout;
  pending_tasks_with_timeout_.emplace(deadline, task);
}

void AsyncContext::ProcessCompletions() {
  struct io_uring_cqe* cqe;
  unsigned head;
  unsigned count = 0;

  std::swap(this->loop_running_tasks_, this->pending_tasks2_);
  io_uring_for_each_cqe(&ring_, head, cqe) {
    void* user_data = io_uring_cqe_get_data(cqe);

    if (user_data == reinterpret_cast<void*>(0x1)) {
      // 事件fd被触发，清空它
      uint64_t val;
      read(event_fd_, &val, sizeof(val));
    } else if (user_data) {
      // 这是一个IO任务的完成
      AsyncIOTask* task = static_cast<AsyncIOTask*>(user_data);
      this->pending_io_tasks_.erase(task);
      this->loop_running_tasks_.emplace_back(task);
      // 设置结果
      if (cqe->res < 0) {
        task->SetResult(
            Status::IOError("IO operation failed", strerror(-cqe->res)));
      } else {
        task->SetResult(Status::OK());

        // 根据任务类型设置具体结果
        switch (task->GetType()) {
          case IOType::kOpen: {
            task->SetFileDescriptor(cqe->res);
            break;
          }
          case IOType::kRead: {
            task->SetBytesRead(cqe->res);
            break;
          }
          case IOType::kWrite: {
            task->SetBytesWritten(cqe->res);
            break;
          }
          default:
            break;
        }
      }
    }

    count++;
  }

  if (count > 0) {
    io_uring_cq_advance(&ring_, count);
  }

  auto now = std::chrono::steady_clock::now();
  for (; not this->pending_tasks_with_timeout_.empty() and
         this->pending_tasks_with_timeout_.top().first <= now;) {
    auto task = this->pending_tasks_with_timeout_.top().second;
    this->pending_tasks_with_timeout_.pop();
    this->loop_running_tasks_.emplace_back(task);
  }

  for (auto task = this->pending_tasks_.try_dequeue(); task != nullptr;
       task = this->pending_tasks_.try_dequeue()) {
    this->loop_running_tasks_.emplace_back(task);
  }

  for (auto& task : this->loop_running_tasks_) {
    task->Execute(this);
  }
  this->loop_running_tasks_.clear();
}

__kernel_timespec* AsyncContext::SetTimer() {
  if (not this->pending_tasks2_.empty()) {
    return nullptr;
  }

  if (this->pending_tasks_with_timeout_.empty()) {
    return nullptr;
  }

  auto now = std::chrono::steady_clock::now();
  if (this->pending_tasks_with_timeout_.top().first <= now) {
    return nullptr;
  }

  auto next_timeout = this->pending_tasks_with_timeout_.top().first;
  this->timeout_ts_.tv_sec =
      std::chrono::duration_cast<std::chrono::seconds>(next_timeout - now)
          .count();
  this->timeout_ts_.tv_nsec =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          next_timeout - now - std::chrono::seconds(this->timeout_ts_.tv_sec))
          .count();
  return &this->timeout_ts_;
}

// AsyncEnv implementation
AsyncContext* AsyncEnv::GetContext() const { return local_context; }

}  // namespace leveldb::io