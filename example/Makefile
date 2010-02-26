# Makefile for sample programs of Kyoto Cabinet



#================================================================
# Setting Variables
#================================================================


# Generic settings
SHELL = /bin/sh

# Targets
MYBINS = kccacheex kchashex kctreeex

# Building binaries
CXX = g++
CXXFLAGS = -I. -I.. -Wall -fsigned-char -O2
LDFLAGS =
LIBS = -L. -L.. -lkyotocabinet -lstdc++ -lz -lrt -lpthread -lm -lc
LDENV = LD_RUN_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..



#================================================================
# Suffix rules
#================================================================


.SUFFIXES :
.SUFFIXES : .cc .o

.cc.o :
	$(CXX) -c $(CXXFLAGS) $<



#================================================================
# Actions
#================================================================


all : $(MYBINS)


clean :
	rm -rf $(MYBINS) *.exe *.o a.out check.out gmon.out leak.log casket* *~


static :
	make LDFLAGS="$(LDFLAGS) -static"


.PHONY : all clean static tctchat.tct tctsearch.tct



#================================================================
# Building binaries
#================================================================


kccacheex : kccacheex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kchashex : kchashex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kctreeex : kctreeex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)



# END OF FILE
