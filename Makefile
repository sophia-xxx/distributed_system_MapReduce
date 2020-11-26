NETID = $(USER)

all: build deploy

build:
	go build main.go

deploy:
	for i in 01 02 03 04 05 06 07 08 09 10; do \
		scp main $(NETID)@fa20-cs425-g07-$${i}.cs.illinois.edu:/home/$(NETID)/go/src/mp3 ; \
	done
