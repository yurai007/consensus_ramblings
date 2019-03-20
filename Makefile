PROGS = 2pc
CXXFLAGS = -Wall -W -Wextra -Wshadow -Wpedantic -Wformat-security -Walloca -Wduplicated-branches -g -std=c++2a -I/home/yurai/programs/executors-impl/include
LDFLAGS += -lpthread
CXXFLAGS += -fsanitize=address -fsanitize-recover=address -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=signed-integer-overflow -fsanitize=vptr

.PHONY: all clean

all: $(PROGS)

clean:
	rm -rf $(PROGS)

$(PROGS): %: %.cc
	$(CXX) $(CXXFLAGS) -o$@ $< $(LDFLAGS)
