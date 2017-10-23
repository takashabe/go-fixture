.PHONY: test deps

test:
	cat testdata/create.sql | mysql -udb_fixture db_fixture
	go test -v

deps:
	dep ensure
