# zigqlite

An [SQLite](https://sqlite.org) binding for [Zig](https://ziglang.org).

## Install

#### As a [zigmod](https://nektro.github.io/zigmod/) package:

Add a line to the `dependencies` (or `root_dependencies`) in your `zig.mod` file:

```yaml
dependencies:
  - src: http https://chiselapp.com/user/javier/repository/zigqlite/zip/zigqlite.zip

```

then you can just:

```zig
const sqlite = @import("zigqlite");
```

#### Manually:

Copy `src/sqlite.zig` somewhere in your sources (i.e. a `vendor/` directory) and import with the relative path:

```zig
const sqlite = @import("vendor/sqlite.zig");
```

## Usage

#### DB object:

To open a database in the given path, creating it if needed:

```zig
var db = try sqlite.DB.open(<path>);
defer db.close();
```

#### Prepared Statement

```zig
var stmt = try db.prep(<sql>);
```

Where `<sql>` is the SQL command text. Can include positional arguments like `?` or named arguments like `:argname`.

Once prepared, the statement is executed with the `.exec()` function:

```zig
var cursor = try stmt.exec(<args>, <rowtype>);
```

Where `<args>` is a tuple or structure holding arguments to fill in the SQL command. If field names match named arguments, they are used regardless of order; otherwise the position in the tuple or struct determines the field it fills.

The `<rowtype>` parameter is a struct type. Each returned row will be a value of this type. Each field will hold a column from the row, in order. If there are more fields than columns, the extra fields would be filled with their respective default value, if declared in the struct.

#### Cursor object

The value returned by the `stmt.exec()` function is used to retrieve results row by row with the `.fetch()` function:

```zig
var stmt = try db.prep("select name, age from children where height >= :minheight");
var cursor = try stmt.exec(.{1.30}, struct{name: []const u8, age: i16});
while (try cursor.fetch()) |kid| {
    std.debug.print("{s} is {d} years old\n", .{kid.name, kid.age});
}
```
