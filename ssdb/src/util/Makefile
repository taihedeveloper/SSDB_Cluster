include ../../build_config.mk

OBJS = log.o config.o bytes.o sorted_set.o app.o slot.o crc16.o hash.o spin_lock.o io_cache.o net.o
EXES = 

all: ${OBJS}
	ar -cru ./libutil.a ${OBJS}

app.o: app.h app.cpp
	${CXX} ${CFLAGS} -c app.cpp

log.o: log.h log.cpp
	${CXX} ${CFLAGS} -c log.cpp

config.o: config.h config.cpp
	${CXX} ${CFLAGS} -c config.cpp

bytes.o: bytes.h bytes.cpp
	${CXX} ${CFLAGS} -c bytes.cpp

sorted_set.o: sorted_set.h sorted_set.cpp
	${CXX} ${CFLAGS} -c sorted_set.cpp

crc16.o: crc16.h crc16.cpp
	${CXX} ${CFLAGS} -std=c++0x -c crc16.cpp

slot.o: slot.h slot.cpp
	${CXX} ${CFLAGS} -std=c++0x -c slot.cpp

hash.o: hash.h hash.cpp
	${CXX} ${CFLAGS} -c hash.cpp

spin_lock.o: spin_lock.h spin_lock.cpp
	${CXX} ${CFLAGS} -c spin_lock.cpp

io_cache.o: io_cache.h io_cache.cpp
	${CXX} ${CFLAGS} -c io_cache.cpp
net.o : net.h net.cpp
	${CXX} ${CFLAGS} -c net.cpp

test:
	$(CXX) ${CFLAGS} test_sorted_set.cpp $(OBJS)

clean:
	rm -f ${EXES} ${OBJS} *.o *.exe *.a

