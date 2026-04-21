# ==============================================================================
# OpenNMS Prometheus Remote Writer — Build Facade
#
# Usage:
#   make build        Compile, run unit tests, install all modules locally
#   make test         Run unit tests only
#   make verify       Run unit + integration tests (requires Docker for IT)
#   make kar          Build the KAR (assembly/kar/target/*.kar)
#   make clean        Remove all build artifacts
#
# Overridable variables:
#   MODULE            Maven module selector (e.g. :prometheus-remote-writer)
#   TEST              Test class name (suffix IT = integration test)
#   MAVEN_FLAGS       Extra Maven flags (default: -B --no-transfer-progress)
#   MAVEN_OPTS        JVM options for Maven
# ==============================================================================

MODULE      ?=
TEST        ?=
MAVEN_FLAGS ?= -B --no-transfer-progress
MAVEN_OPTS  ?= -Xmx2g -XX:+TieredCompilation

MVN := ./mvnw

export MAVEN_OPTS

.PHONY: help build test verify kar clean test-class

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Variables (override on command line):"
	@echo "  MODULE           Maven module selector                              (current: $(MODULE))"
	@echo "  TEST             Test class name (suffix IT = integration test)     (current: $(TEST))"
	@echo "  MAVEN_FLAGS      Extra Maven flags                                  (current: $(MAVEN_FLAGS))"

build: ## Compile, run unit tests, install all modules locally
	$(MVN) $(MAVEN_FLAGS) install

test: ## Run unit tests only
	$(MVN) $(MAVEN_FLAGS) test

verify: ## Run unit + integration tests (needs Docker for testcontainers)
	$(MVN) $(MAVEN_FLAGS) verify

kar: ## Build the KAR artifact
	$(MVN) $(MAVEN_FLAGS) -DskipTests package

clean: ## Remove all build artifacts
	$(MVN) $(MAVEN_FLAGS) clean

test-class: ## Run a single test class; set MODULE and TEST
	@test -n "$(MODULE)" || (echo "ERROR: MODULE is required" && exit 1)
	@test -n "$(TEST)"   || (echo "ERROR: TEST is required" && exit 1)
	$(MVN) $(MAVEN_FLAGS) \
	  --projects $(MODULE) \
	  --also-make \
	  $(if $(filter %IT,$(TEST)),-Dit.test=$(TEST),-Dtest=$(TEST) -DskipTests=false) \
	  $(if $(filter %IT,$(TEST)),failsafe:integration-test failsafe:verify,test)
