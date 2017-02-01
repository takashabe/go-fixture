# go-fixture

## design:

* fixture write in .yml or .sql
* setup fixture by choose .yml and target table mapping struct
  * e.g. `loadFixture("path/to/hoge.yml", &Hoge{})`

### .yml fixture file format

```
- table: foo
  records:
    - id: 1
      first_name: foo
      last_name: bar
    - id: 2
      first_name: piyo
      last_name: fuga
```

### .sql fixture file format

```
insert into person(id, name) values (1, 'foo')
```
