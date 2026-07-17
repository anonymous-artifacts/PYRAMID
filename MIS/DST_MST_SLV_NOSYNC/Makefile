CXX = g++
CXXFLAGS = -std=c++17 -O3 -fopenmp -pthread -Wall
METIS_INC = -I/opt/ohpc/pub/libs/gnu12/metis/5.1.0/include
METIS_LIB = -L/opt/ohpc/pub/libs/gnu12/metis/5.1.0/lib -lmetis

all: master slave

master: master.cpp partitioner.cpp protocol.h graph_utils.h partitioner.h
	$(CXX) $(CXXFLAGS) $(METIS_INC) -o $@ master.cpp partitioner.cpp $(METIS_LIB)
	@echo "Built: master (run on login node)"

slave: slave.cpp protocol.h graph_utils.h ghost_manager.h
	$(CXX) $(CXXFLAGS) -o $@ slave.cpp
	@echo "Built: slave (run on cn04/cn05 via SLURM)"

clean:
	rm -f master slave
