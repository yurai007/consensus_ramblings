CXXFLAGS = -Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++2a -fconcepts -I/home/yurai/programs/executors-impl/include
LDFLAGS += -lpthread
CXXFLAGS += -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr

.PHONY: all clean

2pc: 2pc.cc
	$(CXX) $(CXXFLAGS) 2pc.cc -o 2pc $(LDFLAGS)

gcc: CXX := g++
gcc: CXXFLAGS = -Wall -W -Wextra -Wshadow -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++20 -fconcepts -fcoroutines
gcc: CXXFLAGS += -fstack-protector -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr
gcc: LDFLAGS = -lfmt
gcc: scheduler_tests.cc
	$(CXX) $(CXXFLAGS) scheduler_tests.cc -o scheduler_tests $(LDFLAGS)

clang: CXX := clang++
clang: CXXFLAGS = -g -std=c++20 -fcoroutines-ts -Wno-c99-extensions -Wno-c++98-compat-pedantic -stdlib=libc++
clang: CXXFLAGS += -fsanitize=undefined -fsanitize-recover=address -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr
clang: LDFLAGS = -lfmt
clang: scheduler_tests.cc
	$(CXX) $(CXXFLAGS) scheduler_tests.cc -o scheduler_tests_cl $(LDFLAGS)


all: 2pc tests

heavy: 
	make CXXFLAGS='-Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++20 -fconcepts -fcoroutines' -j4 gcc && \
		valgrind --tool=helgrind ./scheduler_tests && valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes --expensive-definedness-checks=yes ./scheduler_tests && \

clean:
	rm -rf scheduler_tests scheduler_tests_cl

