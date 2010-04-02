namespace java com.twitter.rowz.thrift
namespace rb Rowz

struct Row {
  1: i64 id
  2: string name
  3: i32 created_at
  4: i32 updated_at
  5: i32 state
}

exception RowzException {
  1: string description
}

service Rowz {
  i64 create(1: string name, 2: i32 at) throws(RowzException ex)
  void destroy(1: Row row, 2: i32 at) throws(RowzException ex)
  Row read(1: i64 id) throws(RowzException ex)
}
