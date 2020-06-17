CXXFLAGS = -Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++2a -fconcepts -I/home/yurai/programs/executors-impl/include
LDFLAGS += -lpthread
CXXFLAGS += -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr

.PHONY: all clean

2pc: 2pc.cc
	$(CXX) $(CXXFLAGS) 2pc.cc -o 2pc $(LDFLAGS)

scheduler: CXX := g++
scheduler: CXXFLAGS = -Wall -W -Wextra -Wshadow -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++20 -fconcepts -fcoroutines
scheduler: CXXFLAGS += -fstack-protector -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr
scheduler: LDFLAGS = -lfmt
scheduler: scheduler_tests.cc
	$(CXX) $(CXXFLAGS) scheduler_tests.cc -o scheduler_tests $(LDFLAGS)

all: 2pc tests

heavy: 
	make CXXFLAGS='-Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++20 -fconcepts -fcoroutines' -j4 scheduler && \
		valgrind --tool=helgrind ./scheduler_tests && valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes --expensive-definedness-checks=yes ./scheduler_tests && \
clean:
	rm -rf 2pc tests raft

