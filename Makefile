PROGRAM=kvrocks-controller

CCCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

BUILDER_IMAGE="none"

all: $(PROGRAM)

.PHONY: all


$(PROGRAM):
	@bash build.sh $(BUILDER_IMAGE)
	@echo ""
	@printf $(MAKECOLOR)"Hint: It's a good idea to run 'make test' ;)"$(ENDCOLOR)
	@echo ""

setup:
	@cd scripts && sh setup.sh && cd ..

teardown:
	@cd scripts && sh teardown.sh && cd ..

test:
	@cd scripts && sh setup.sh && cd ..
	@scripts/run-test.sh
	@cd scripts && sh teardown.sh && cd ..

lint:
	@printf $(CCCOLOR)"GolangCI Lint...\n"$(ENDCOLOR)
	@golangci-lint run
