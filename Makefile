DB_HOST := 127.0.0.1

.PHONY: test deps

test:
	cat testdata/create.sql | mysql -h $(DB_HOST) -u db_fixture db_fixture 
	go test -v

deps:
	dep ensure
