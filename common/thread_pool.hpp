#pragma once
#include <queue>
#include <thread>
#include <functional>
#include <thread>
#include <condition_variable>

const size_t pool_sz = 10;

class thread_pool {
public:
    using task_t = std::function<void()>;

    thread_pool(size_t sz = pool_sz) {
        _stop = false;
        for (int i = 0; i < sz; i++) {
            auto th = std::make_unique<std::thread>([this] () {
                work();
            });
            _threads.push_back(std::move(th));
        }
    }

    ~thread_pool() {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _stop = true;
        }
        _cv_th.notify_all();

        for (int i = 0; i < _threads.size(); i++) {
            _threads[i]->join();
        }
    }

    bool submit(task_t task) {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_stop) return false;
            _tasks.push(std::move(task));
        }
        
        _cv_th.notify_one();
        return true;
    }

private:
    void work() {
        while (true) {
            task_t task;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _cv_th.wait(
                    lock,
                    [this] () {
                        return _stop || !_tasks.empty();
                    }
                );
                if (_stop && _tasks.empty()) return ;
                task = std::move(_tasks.front());
                _tasks.pop();
            }
            task();
        }
    }

private:
    bool _stop;
    std::mutex _mutex;
    std::condition_variable _cv_th;
    std::vector <std::unique_ptr<std::thread>> _threads;
    std::queue <task_t> _tasks;
};