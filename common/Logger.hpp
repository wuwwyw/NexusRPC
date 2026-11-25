#pragma once
#include <memory>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

class Logger {
public:
    Logger(bool mode, int level, std::string filename) : _mode(mode), _level(level), _filename(filename) {
        if (mode) {
            _logger = spdlog::basic_logger_mt("default_logger", _filename);
            _logger->set_level((spdlog::level::level_enum)_level);
            _logger->flush_on((spdlog::level::level_enum)_level);
        } else {
            _logger = spdlog::stdout_color_mt("default_logger");
            _logger->set_level(spdlog::level::level_enum::trace);
            _logger->flush_on(spdlog::level::level_enum::trace);
        }
        _logger->set_pattern("[%n][%H:%M:%S][%t][%-8l] %v");
    }

    std::shared_ptr<spdlog::logger> get() {
        return _logger;
    }

private:
    bool _mode;
    int _level;
    std::string _filename;
    std::shared_ptr <spdlog::logger> _logger;
};

inline Logger logger(false, 0, "");
//Logger logger(true, 0, "~/NexusIM/common/logfile");

#define LOG_TRACE(format, ...) logger.get()->trace(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_DEBUG(format, ...) logger.get()->debug(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_INFO(format, ...) logger.get()->info(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_WARN(format, ...) logger.get()->warn(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_ERROR(format, ...) logger.get()->error(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_CRIT(format, ...) logger.get()->critical(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
