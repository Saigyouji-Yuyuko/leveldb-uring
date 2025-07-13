#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>

namespace leveldb {
namespace io {

/**
 * 侵入式MPSC无锁队列基础节点
 * 用户的数据结构需要继承此类
 */
struct MPSCQueueNode {
  std::atomic<MPSCQueueNode*> next{nullptr};

  MPSCQueueNode() = default;

  // 禁止拷贝和移动
  MPSCQueueNode(const MPSCQueueNode&) = delete;
  MPSCQueueNode& operator=(const MPSCQueueNode&) = delete;
  MPSCQueueNode(MPSCQueueNode&&) = delete;
  MPSCQueueNode& operator=(MPSCQueueNode&&) = delete;
};

/**
 * 侵入式MPSC无锁队列
 *
 * 特点：
 * - 零内存分配：节点嵌入在用户数据结构中
 * - 高性能：避免额外的指针解引用
 * - 内存高效：无额外内存开销
 * - 缓存友好：数据和节点在同一内存块中
 */
class IntrusiveMPSCQueue {
 private:
  // 使用缓存行对齐避免伪共享
  alignas(64) std::atomic<MPSCQueueNode*> head_;
  alignas(64) std::atomic<MPSCQueueNode*> tail_;

  // 队列大小统计（可选）
  alignas(64) std::atomic<size_t> size_{0};

 public:
  IntrusiveMPSCQueue() {
    // 初始化空队列，head和tail都指向nullptr
    head_.store(nullptr, std::memory_order_relaxed);
    tail_.store(nullptr, std::memory_order_relaxed);
  }

  ~IntrusiveMPSCQueue() {
    // 侵入式队列不负责释放节点内存
    // 用户需要自己管理对象的生命周期
  }

  // 禁止拷贝和移动
  IntrusiveMPSCQueue(const IntrusiveMPSCQueue&) = delete;
  IntrusiveMPSCQueue& operator=(const IntrusiveMPSCQueue&) = delete;
  IntrusiveMPSCQueue(IntrusiveMPSCQueue&&) = delete;
  IntrusiveMPSCQueue& operator=(IntrusiveMPSCQueue&&) = delete;

  /**
   * 入队操作 - 多个生产者可以并发调用
   * @param node 要入队的节点指针
   */
  void enqueue(MPSCQueueNode* node) {
    if (!node) return;

    // 确保节点的next指针为nullptr
    node->next.store(nullptr, std::memory_order_relaxed);

    // 原子性地将节点添加到队列尾部
    MPSCQueueNode* prev_tail = tail_.exchange(node, std::memory_order_acq_rel);

    if (prev_tail) {
      // 队列非空，链接到前一个尾节点
      prev_tail->next.store(node, std::memory_order_release);
    } else {
      // 队列为空，新节点同时是头节点
      head_.store(node, std::memory_order_release);
    }

    // 更新队列大小
    size_.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * 出队操作 - 仅单个消费者可以调用
   * @return 出队的节点指针，nullptr表示队列为空
   */
  MPSCQueueNode* try_dequeue() {
    MPSCQueueNode* head = head_.load(std::memory_order_acquire);

    if (!head) {
      // 队列为空
      return nullptr;
    }

    MPSCQueueNode* next = head->next.load(std::memory_order_acquire);

    if (next) {
      // 队列中有多个节点
      head_.store(next, std::memory_order_relaxed);
    } else {
      // 队列中只有一个节点
      head_.store(nullptr, std::memory_order_relaxed);

      // 尝试重置tail指针
      MPSCQueueNode* expected = head;
      if (!tail_.compare_exchange_strong(expected, nullptr,
                                         std::memory_order_relaxed)) {
        // 有生产者正在添加节点，等待next指针更新
        while (!head->next.load(std::memory_order_acquire)) {
          // 短暂等待
        }
        head_.store(head->next.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
      }
    }

    // 更新队列大小
    size_.fetch_sub(1, std::memory_order_relaxed);

    // 清理节点的next指针
    head->next.store(nullptr, std::memory_order_relaxed);

    return head;
  }

  /**
   * 检查队列是否为空
   */
  bool empty() const {
    return head_.load(std::memory_order_acquire) == nullptr;
  }

  /**
   * 获取队列大小（近似值）
   */
  size_t size() const { return size_.load(std::memory_order_relaxed); }

  /**
   * 清空队列并返回所有节点
   * @param callback 用于处理每个节点的回调函数
   */
  template <typename Callback>
  void clear(Callback callback) {
    MPSCQueueNode* node;
    while ((node = try_dequeue()) != nullptr) {
      callback(node);
    }
  }
};

/**
 * 侵入式MPSC队列的类型安全包装器
 * 提供类型安全的接口
 */
template <typename T>
class TypedIntrusiveMPSCQueue {
 private:
  IntrusiveMPSCQueue queue_;

  // 静态断言确保T继承自MPSCQueueNode
  static_assert(std::is_base_of_v<MPSCQueueNode, T>,
                "T must inherit from MPSCQueueNode");

 public:
  TypedIntrusiveMPSCQueue() = default;

  /**
   * 类型安全的入队操作
   */
  void enqueue(T* item) { queue_.enqueue(static_cast<MPSCQueueNode*>(item)); }

  /**
   * 类型安全的出队操作
   */
  T* try_dequeue() {
    MPSCQueueNode* node = queue_.try_dequeue();
    return static_cast<T*>(node);
  }

  /**
   * 检查队列是否为空
   */
  bool empty() const { return queue_.empty(); }

  /**
   * 获取队列大小
   */
  size_t size() const { return queue_.size(); }

  /**
   * 清空队列
   */
  template <typename Callback>
  void clear(Callback callback) {
    queue_.clear(
        [callback](MPSCQueueNode* node) { callback(static_cast<T*>(node)); });
  }
};

/**
 * 侵入式双向链表节点
 * 用于需要双向链表的场景
 */
struct MPSCQueueBiNode {
  std::atomic<MPSCQueueBiNode*> next{nullptr};
  std::atomic<MPSCQueueBiNode*> prev{nullptr};

  MPSCQueueBiNode() = default;

  // 禁止拷贝和移动
  MPSCQueueBiNode(const MPSCQueueBiNode&) = delete;
  MPSCQueueBiNode& operator=(const MPSCQueueBiNode&) = delete;
  MPSCQueueBiNode(MPSCQueueBiNode&&) = delete;
  MPSCQueueBiNode& operator=(MPSCQueueBiNode&&) = delete;
};

/**
 * 侵入式双向MPSC队列
 * 支持从队列中间移除节点
 */
class IntrusiveMPSCBiQueue {
 private:
  alignas(64) std::atomic<MPSCQueueBiNode*> head_{nullptr};
  alignas(64) std::atomic<MPSCQueueBiNode*> tail_{nullptr};
  alignas(64) std::atomic<size_t> size_{0};

 public:
  IntrusiveMPSCBiQueue() = default;
  ~IntrusiveMPSCBiQueue() = default;

  // 禁止拷贝和移动
  IntrusiveMPSCBiQueue(const IntrusiveMPSCBiQueue&) = delete;
  IntrusiveMPSCBiQueue& operator=(const IntrusiveMPSCBiQueue&) = delete;

  /**
   * 入队操作
   */
  void enqueue(MPSCQueueBiNode* node) {
    if (!node) return;

    node->next.store(nullptr, std::memory_order_relaxed);
    node->prev.store(nullptr, std::memory_order_relaxed);

    MPSCQueueBiNode* prev_tail =
        tail_.exchange(node, std::memory_order_acq_rel);

    if (prev_tail) {
      prev_tail->next.store(node, std::memory_order_release);
      node->prev.store(prev_tail, std::memory_order_release);
    } else {
      head_.store(node, std::memory_order_release);
    }

    size_.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * 出队操作
   */
  MPSCQueueBiNode* try_dequeue() {
    MPSCQueueBiNode* head = head_.load(std::memory_order_acquire);

    if (!head) {
      return nullptr;
    }

    MPSCQueueBiNode* next = head->next.load(std::memory_order_acquire);

    if (next) {
      head_.store(next, std::memory_order_relaxed);
      next->prev.store(nullptr, std::memory_order_relaxed);
    } else {
      head_.store(nullptr, std::memory_order_relaxed);

      MPSCQueueBiNode* expected = head;
      if (!tail_.compare_exchange_strong(expected, nullptr,
                                         std::memory_order_relaxed)) {
        while (!head->next.load(std::memory_order_acquire)) {
          // 等待
        }
        MPSCQueueBiNode* new_head = head->next.load(std::memory_order_relaxed);
        head_.store(new_head, std::memory_order_relaxed);
        new_head->prev.store(nullptr, std::memory_order_relaxed);
      }
    }

    size_.fetch_sub(1, std::memory_order_relaxed);

    // 清理节点指针
    head->next.store(nullptr, std::memory_order_relaxed);
    head->prev.store(nullptr, std::memory_order_relaxed);

    return head;
  }

  /**
   * 从队列中移除指定节点（仅消费者可调用）
   */
  bool remove(MPSCQueueBiNode* node) {
    if (!node) return false;

    MPSCQueueBiNode* prev_node = node->prev.load(std::memory_order_acquire);
    MPSCQueueBiNode* next_node = node->next.load(std::memory_order_acquire);

    if (prev_node) {
      prev_node->next.store(next_node, std::memory_order_release);
    } else {
      // 移除的是头节点
      head_.store(next_node, std::memory_order_release);
    }

    if (next_node) {
      next_node->prev.store(prev_node, std::memory_order_release);
    } else {
      // 移除的是尾节点
      tail_.store(prev_node, std::memory_order_release);
    }

    // 清理节点指针
    node->next.store(nullptr, std::memory_order_relaxed);
    node->prev.store(nullptr, std::memory_order_relaxed);

    size_.fetch_sub(1, std::memory_order_relaxed);

    return true;
  }

  bool empty() const {
    return head_.load(std::memory_order_acquire) == nullptr;
  }

  size_t size() const { return size_.load(std::memory_order_relaxed); }
};

}  // namespace io
}  // namespace leveldb
