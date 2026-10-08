CXX ?= c++
CPPFLAGS += -Iinclude -Ithird_party
CXXFLAGS ?= -O2 -DNDEBUG
CXXFLAGS += -std=c++20 -Wall -Wextra -Wpedantic -Wconversion -Wshadow
BUILD ?= build
HEADERS := $(wildcard include/quorum/*.hpp) $(wildcard third_party/matching/*.hpp)
BINARIES := quorum-node quorum-cli quorum-bench quorum-sim core-tests storage-tests

.PHONY: all test unit cluster sanitize soak mutants demo bench clean

all: $(addprefix $(BUILD)/,$(BINARIES))

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/quorum-node: src/node_main.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(BUILD)/quorum-cli: src/client_main.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(BUILD)/quorum-bench: src/bench_main.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(BUILD)/quorum-sim: src/sim_main.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(BUILD)/core-tests: tests/test_core.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(BUILD)/storage-tests: tests/test_storage.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

# Core logic, the simulator, and on-disk recovery. No network needed.
unit: all
	$(BUILD)/core-tests examples/session.txt
	$(BUILD)/storage-tests

# Real processes on localhost: failover, restart, and replica agreement.
cluster: all
	python3 tests/test_cluster.py $(BUILD)

test: unit cluster

sanitize:
	$(MAKE) BUILD=build-sanitize CXXFLAGS='-std=c++20 -O1 -g -Wall -Wextra -Wpedantic -Wconversion -Wshadow -fsanitize=address,undefined -fno-omit-frame-pointer' LDFLAGS='-fsanitize=address,undefined' unit

# 2,000 simulated clusters with faults; about 20 seconds on a laptop.
soak: $(BUILD)/quorum-sim
	$(BUILD)/quorum-sim --seeds 2000

# Show that the simulator catches each deliberately injected bug.
mutants: $(BUILD)/quorum-sim
	python3 scripts/mutants.py $(BUILD)/quorum-sim

demo: all
	python3 scripts/demo.py $(BUILD)

bench: all
	python3 scripts/run_bench.py $(BUILD)

clean:
	rm -rf build build-sanitize build-cmake data results
