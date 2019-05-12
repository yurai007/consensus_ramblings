CXXFLAGS = -Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++2a -fconcepts -I/home/yurai/programs/executors-impl/include
LDFLAGS += -lpthread
CXXFLAGS += -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr

.PHONY: all clean

2pc: 2pc.cc
	$(CXX) $(CXXFLAGS) 2pc.cc -o 2pc $(LDFLAGS)

tests: tests.cc
	$(CXX) $(CXXFLAGS) tests.cc -o tests $(LDFLAGS)

all: 2pc tests

heavy: 
	make CXXFLAGS='-Wall -W -Wextra -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++2a -fconcepts -I/home/yurai/programs/executors-impl/include' -j4 all && \
		valgrind --tool=helgrind ./tests && valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes --expensive-definedness-checks=yes ./tests && \
		valgrind --tool=helgrind ./2pc && valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes --expensive-definedness-checks=yes ./2pc

clean:
	rm -rf 2pc tests

