#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>
#include <seastar/util/process.hh>

#include <sys/wait.h>

#include <chrono>
#include <iostream>

namespace ss = seastar;

static ss::logger lg("execute-process");

class execute_process_service final {
public:
    execute_process_service(std::vector<ss::sstring> cmd, bool execute_on_all)
      : _cmd(std::move(cmd))
      , _execute_on_all(execute_on_all) {
        if (_cmd.empty()) {
            throw std::runtime_error("Must provide at least one argument");
        }
    }

    ss::future<> start() {
        co_return;
    }

    ss::future<> stop() {
        co_await _g.close();
    }

    ss::future<> execute_process() {
        auto h = _g.hold();

        if (!_execute_on_all && ss::this_shard_id() != 0) {
            co_return;
        }

        auto path = std::filesystem::path(_cmd[0]);

        lg.info(
          "Starting process {} with arguments '{}'",
          path.native(),
          fmt::join(_cmd, " "));

        ss::experimental::spawn_parameters params{
          .argv = _cmd,
        };
        auto p = co_await ss::experimental::spawn_process(path, params);

        lg.info("Waiting for pocess to finish");
        auto resp = co_await p.wait();
        lg.info("Process finished");

        ss::visit(
          resp,
          [](const ss::experimental::process::wait_exited& exited) {
              lg.info("Process exited with code {}", exited.exit_code);
          },
          [](const ss::experimental::process::wait_signaled& signaled) {
              lg.info(
                "Process signaled with signal {}", signaled.terminating_signal);
          });
    }

private:
    std::vector<ss::sstring> _cmd;
    bool _execute_on_all;
    ss::gate _g;
};

int main(int argc, char** argv) {
    seastar::sharded<execute_process_service> exec_proc;

    seastar::app_template app;
    {
        namespace po = boost::program_options;

        app.add_options()(
          "execute-on-all",
          po::value<bool>()->default_value(false),
          "execute on all shards");
    }

    return app.run(argc, argv, [&] {
        seastar::engine().at_exit([&exec_proc] { return exec_proc.stop(); });

        auto& opts = app.configuration();

        const auto execute_on_all = opts["execute-on-all"].as<bool>();

        std::vector<ss::sstring> cmd = {"/usr/bin/sleep", "3"};

        return exec_proc.start(std::move(cmd), execute_on_all)
          .then([&exec_proc] {
              return exec_proc.invoke_on_all(&execute_process_service::start)
                .then([&exec_proc] {
                    return exec_proc
                      .invoke_on_all(&execute_process_service::execute_process)
                      .then([] { return ss::make_ready_future<int>(0); });
                });
          });
    });
}
