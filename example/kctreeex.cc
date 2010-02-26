#include <kctreedb.h>

using namespace std;
using namespace kyotocabinet;

int main(int argc, char** argv) {

  // create the database object
  TreeDB db;

  // open the database
  if (!db.open("casket.kch", FileDB::OWRITER | FileDB::OCREATE)) {
    cout << "open error: " << db.error().string() << endl;
  }

  // store records
  if (!db.set("foo", "hop") ||
     !db.set("bar", "step") ||
     !db.set("baz", "jump")) {
    cout << "set error: " << db.error().string() << endl;
  }

  // retrieve records
  string* value = db.get("foo");
  if (value) {
    cout << *value << endl;
    delete value;
  } else {
    cout << "get error: " << db.error().string() << endl;
  }

  // traverse records
  class Traverser : public DB::Visitor {
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t *sp) {
      cout << string(kbuf, ksiz) << ":" << string(vbuf, vsiz) << endl;
      return NOP;
    }
  } traverser;
  db.iterate(&traverser, false);

  // close the database
  if (!db.close()) {
    cout << "close error: " << db.error().string() << endl;
  }

  return 0;
}
