all:
	python init.py

help:
	@echo "Makefile targets:"
	@echo "create"
	@echo "update"
	@echo "configure"
	@echo "test"
	@echo "debug"
	@echo "clean"
	@echo "all"

create:
	conda env create -f loadinsight-environment.yml

update:
	conda env update -f loadinsight-environment.yml

configure:
	aws configure

test:
	python -m unittest

debug:
	python init.py -d

clean:
	rm -rf local_data
	git checkout -- local_data
