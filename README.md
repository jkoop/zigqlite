# zigqlite

> [!NOTE]  
> Forked from https://chiselapp.com/user/javier/repository/zigqlite

An [SQLite](https://sqlite.org) binding for [Zig](https://ziglang.org).

## Install

```sh
zig fetch --save https://github.com/jkoop/zigqlite/archive/COMMIT.zip
```

```zig
// build.zig before b.installArtifact(exe);

const zigqlite = b.dependency("zigqlite", .{
    .target = target,
    .optimize = optimize,
}).module("zigqlite");

exe.linkSystemLibrary("c");
exe.linkSystemLibrary("sqlite3"); // apt install libsqlite3-dev
exe.root_module.addImport("zigqlite", zigqlite);
```

## Usage

See also, the [API documentation](https://joekoop.com/zigqlite).

To open a database file at the given path, creating it if needed:

```zig
var db = try sqlite.DB.open(allocator, "database.db");
defer db.close() catch unreachable; // @todo figure out if I can handle this error or not
```

Create a statment:

```zig
var stmt = try db.prep("SELECT * FROM iguanas WHERE name = ?");
```

The SQL can include positional arguments like `?` or named arguments like `:argname`.

Once prepared, the statement is executed with the `.exec()` function:

```zig
// var cursor = try stmt.exec(args, rowtype);
var cursor = try stmt.exec(.{"Zero"}, struct { name: []u8, photo_url: []u8 });
```

Where `args` is a tuple or structure holding arguments to fill in the SQL command. If field names match named arguments, they are used regardless of order; otherwise the position in the tuple or struct determines the field it fills.

The `rowtype` parameter is a struct type. Each returned row will be a value of this type. Each field will hold a column from the row, in order. If there are more fields than columns, the extra fields would be filled with their respective default value, if declared in the struct.

The value returned by the `stmt.exec()` function is used to retrieve results row by row with the `.fetch()` function:

```zig
while (try cursor.fetch()) |iguana| {
    std.debug.print("{s}'s picture is at {s}\n", .{ iguana.name, iguana.photo_url });
}
```
