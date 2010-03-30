namespace java com.twitter.rowz.thrift
namespace rb Rowz

struct RowInfo {
  1: string name
  2: i32 state_id
}

struct Row {
  1: i64 id
  2: RowInfo info
}

exception RowzException {
  1: string description
}

service Groups {
  i64 create(1: RowInfo info, 2: i32 at) throws(RowzException ex)
  void destroy(1: i64 id, 2: i32 at) throws(RowzException ex)
  RowInfo read(1: i64 id) throws(RowzException ex)
}
