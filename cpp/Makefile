gcc: CXX := g++
gcc: CXXFLAGS = -Wall -W -Wextra -Wshadow -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++20 -fconcepts -fconcepts-diagnostics-depth=5 -fcoroutines
gcc: CXXFLAGS += -fstack-protector -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr
gcc: LDFLAGS = -lfmt -lpthread
gcc: scheduler_channel_tests.cc raft.cc
	$(CXX) $(CXXFLAGS) scheduler_channel_tests.cc -o scheduler_channel_tests $(LDFLAGS)
	$(CXX) $(CXXFLAGS) raft.cc -o raft $(LDFLAGS)

clang: CXX := clang++
clang: CXXFLAGS = -Wall -W -g -std=c++20 -fcoroutines-ts -Wno-c99-extensions -Wno-c++98-compat-pedantic -stdlib=libc++
clang: CXXFLAGS += -fsanitize=undefined -fsanitize-recover=address -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr
#clang: CXXFLAGS += -Weverything
clang: LDFLAGS = -lfmt -lpthread
clang: scheduler_channel_tests.cc raft.cc
	$(CXX) $(CXXFLAGS) scheduler_channel_tests.cc -o scheduler_channel_tests_cl $(LDFLAGS)
	$(CXX) $(CXXFLAGS) raft.cc -o raft_cl $(LDFLAGS)

tidy:
	clang-tidy scheduler_channel_tests.cc
	clang-tidy raft.cc

heavy: 
	make CXXFLAGS='-Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++20 -fconcepts -fcoroutines' -j4 gcc && \
		valgrind --tool=helgrind ./scheduler_channel_tests && valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes --expensive-definedness-checks=yes ./scheduler_channel_tests 

clean:
	rm -rf scheduler_channel_tests scheduler_channel_tests_cl raft raft_cl

