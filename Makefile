CXX ?= g++
CXXFLAGS ?= -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp

.PHONY: all test docker-build run clean
all: shopping-core
shopping-core: main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<
test: shopping-core
	python3 -m unittest discover -s tests -v
docker-build:
	docker build --platform linux/amd64 -t shopping-sessions .
run: shopping-core
	DATA_DIR="$(DATA_DIR)" SCRATCH_DIR="$(SCRATCH_DIR)" OUTPUT_DIR="$(OUTPUT_DIR)" ./shopping-core
clean:
	rm -f shopping-core
