const std = @import("std");
const builtin = std.builtin;

const cSqlite = @cImport(@cInclude("sqlite3.h"));

pub const Error = error{
    Error,
    Internal,
    Permission,
    Abort,
    Busy,
    Locked,
    OutOfMemory,
    Readonly,
    Interrupt,
    Io,
    Corrupt,
    NotFound,
    Full,
    CantOpen,
    Protocol,
    Empty,
    Schema,
    TooBig,
    Constraint,
    Mismatch,
    Misuse,
    NoLFS,
    Auth,
    Format,
    Range,
    NotADB,
    Notice,
    Warning,
    Row,
    Done,
};

fn getErrOrVoid(err: c_int) Error!void {
    return switch (err) {
        cSqlite.SQLITE_OK => {},
        else => getErr(err),
    };
}

fn getErr(err: c_int) Error {
    return switch (err) {
        cSqlite.SQLITE_ERROR => Error.Error,
        cSqlite.SQLITE_INTERNAL => Error.Internal,
        cSqlite.SQLITE_PERM => Error.Permission,
        cSqlite.SQLITE_ABORT => Error.Abort,
        cSqlite.SQLITE_BUSY => Error.Busy,
        cSqlite.SQLITE_LOCKED => Error.Locked,
        cSqlite.SQLITE_NOMEM => Error.OutOfMemory,
        cSqlite.SQLITE_READONLY => Error.Readonly,
        cSqlite.SQLITE_INTERRUPT => Error.Interrupt,
        cSqlite.SQLITE_IOERR => Error.Io,
        cSqlite.SQLITE_CORRUPT => Error.Corrupt,
        cSqlite.SQLITE_NOTFOUND => Error.NotFound,
        cSqlite.SQLITE_FULL => Error.Full,
        cSqlite.SQLITE_CANTOPEN => Error.CantOpen,
        cSqlite.SQLITE_PROTOCOL => Error.Protocol,
        cSqlite.SQLITE_EMPTY => Error.Empty,
        cSqlite.SQLITE_SCHEMA => Error.Schema,
        cSqlite.SQLITE_TOOBIG => Error.TooBig,
        cSqlite.SQLITE_CONSTRAINT => Error.Constraint,
        cSqlite.SQLITE_MISMATCH => Error.Mismatch,
        cSqlite.SQLITE_MISUSE => Error.Misuse,
        cSqlite.SQLITE_NOLFS => Error.NoLFS,
        cSqlite.SQLITE_AUTH => Error.Auth,
        cSqlite.SQLITE_FORMAT => Error.Format,
        cSqlite.SQLITE_RANGE => Error.Range,
        cSqlite.SQLITE_NOTADB => Error.NotADB,
        cSqlite.SQLITE_NOTICE => Error.Notice,
        cSqlite.SQLITE_WARNING => Error.Warning,
        cSqlite.SQLITE_ROW => Error.Row,
        cSqlite.SQLITE_DONE => Error.Done,

        else => Error.Internal,
    };
}

fn cargsToSlices(allocator: std.mem.Allocator, argc: c_int, argv: [*c][*c]const u8) ![][]const u8 {
    const n: usize = @intCast(argc);

    var out = try allocator.alloc([]const u8, n);
    for (argv[0..n], 0..) |arg, i| {
        if (arg == null) {
            out[i] = "-NULL-";
        } else {
            out[i] = std.mem.span(arg);
        }
    }
    return out;
}

pub const DB = struct {
    db: *cSqlite.struct_sqlite3,
    errmsg: ?[*:0]u8 = null,
    allocator: std.mem.Allocator,

    const AfterRow = enum {
        Ok,
        Abort,
    };

    fn clearErr(self: *DB) void {
        if (self.errmsg) |msg| {
            cSqlite.sqlite3_free(msg);
            self.errmsg = null;
        }
    }

    pub fn open(allocator: std.mem.Allocator, filename: []const u8) !DB {
        var path_c = try std.posix.toPosixPath(filename);
        var dbptr: ?*cSqlite.struct_sqlite3 = undefined;

        try getErrOrVoid(
            cSqlite.sqlite3_open_v2(&path_c, &dbptr, cSqlite.SQLITE_OPEN_READWRITE | cSqlite.SQLITE_OPEN_CREATE, null),
        );

        return DB{
            .db = dbptr orelse return Error.Internal,
            .allocator = allocator,
        };
    }

    pub fn close(self: *DB) !void {
        self.clearErr();
        try getErrOrVoid(cSqlite.sqlite3_close_v2(self.db));
    }

    fn errCopy(msg: [*c]const u8) ?[*:0]u8 {
        const errspan = std.mem.span(msg);
        const msgcopy: [*:0]u8 = @ptrCast(cSqlite.sqlite3_malloc(@intCast(errspan.len)) orelse return null);
        for (errspan, 0..) |c, i| {
            msgcopy[i] = c;
        }
        return msgcopy;
    }

    fn getErrAndMsg(self: *DB, err: c_int) Error {
        self.errmsg = errCopy(cSqlite.sqlite3_errmsg(self.db));
        return getErr(err);
    }

    pub fn prep(self: *DB, sql: [:0]const u8) Error!Stmt {
        var pstmt: ?*cSqlite.struct_sqlite3_stmt = null;

        const rc = cSqlite.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &pstmt, null);
        if (rc != cSqlite.SQLITE_OK) {
            self.errmsg = errCopy(cSqlite.sqlite3_errmsg(self.db));
        }
        try getErrOrVoid(rc);
        if (pstmt == null) return Error.Internal;

        return Stmt{
            .db = self,
            .cStmt = pstmt orelse return Error.Internal,
        };
    }

    pub fn exec(self: *DB, sql: [:0]const u8, args: anytype) Error!void {
        var stmt = try self.prep(sql);
        stmt.on_end = .finalize;

        try stmt.exec(args, void);
    }

    pub fn query(self: *DB, sql: [:0]const u8, args: anytype, comptime Rowtype: type) Error!Cursor(Rowtype) {
        var stmt = try self.allocator.create(Stmt);
        stmt.* = try self.prep(sql);
        stmt.on_end = .destroy;
        var curs = try stmt.exec(args, Rowtype);
        curs.owns_statement = true;
        return curs;
    }
};

const Stmt = struct {
    db: ?*DB,
    cStmt: *cSqlite.sqlite3_stmt,
    on_end: enum { reset, finalize, destroy } = .reset,

    pub fn finalize(self: *Stmt) void {
        if (self.db != null) {
            switch (self.on_end) {
                .finalize => {
                    _ = cSqlite.sqlite3_finalize(self.cStmt);
                    self.db = null;
                },
                .reset => {
                    _ = cSqlite.sqlite3_reset(self.cStmt);
                },
                .destroy => {
                    const db = self.db orelse return;
                    _ = cSqlite.sqlite3_finalize(self.cStmt);
                    self.db = null;
                    db.allocator.destroy(self);
                },
            }
        }
    }

    pub fn getErrAndMsg(self: *Stmt, err: c_int) Error {
        const db = self.db orelse return Error.Misuse;
        return db.getErrAndMsg(err);
    }

    pub fn exec(self: *Stmt, args: anytype, comptime Rowtype: type) Error!Cursor(Rowtype) {
        try getErrOrVoid(cSqlite.sqlite3_reset(self.cStmt));
        try self.bind(args);
        if (Rowtype == void) {
            var curs = Cursor(struct {}){ .stmt = self };
            while (try curs.fetch()) |_| {}
            return;
        }
        return Cursor(Rowtype){ .stmt = self };
    }

    fn bind(self: *Stmt, args: anytype) Error!void {
        try getErrOrVoid(cSqlite.sqlite3_clear_bindings(self.cStmt));
        inline for (std.meta.fields(@TypeOf(args)), 0..) |fld, i| {
            try self.bind_parameter(i + 1, fld.name, @field(args, fld.name));
        }
    }

    fn bind_parameter(self: *Stmt, i: c_int, comptime name: []const u8, arg: anytype) Error!void {
        // print ("bind_parameter: [{}]'{s}': <{any}>, ({s})\n", .{i, name, arg, @typeName(@TypeOf(arg))});
        var index = i;
        if (name.len > 0 and (name[0] < '0' or name[0] > '9')) {
            index = cSqlite.sqlite3_bind_parameter_index(self.cStmt, ":" ++ name);
            if (index <= 0) index = i;
        }

        return switch (@typeInfo(@TypeOf(arg))) {
            .Bool => getErrOrVoid(cSqlite.sqlite3_bind_int(self.cStmt, index, @intFromBool(arg))),
            .Int, .ComptimeInt => getErrOrVoid(cSqlite.sqlite3_bind_int64(self.cStmt, index, @intCast(arg))),
            .Float, .ComptimeFloat => getErrOrVoid(cSqlite.sqlite3_bind_double(self.cStmt, index, @floatCast(arg))),
            .Pointer => |ptr_info| switch (ptr_info.size) {
                .One => self.bind_parameter(i, name, arg.*),
                .Slice => self.bind_slice(index, arg),
                else => self.bind_slice(index, std.mem.span(arg)),
            },
            .Array => self.bind_slice(index, arg[0..]),
            .Null => getErrOrVoid(cSqlite.sqlite3_bind_null(self.cStmt, index)),
            else => Error.Mismatch,
        };
    }

    fn bind_slice(self: *Stmt, index: c_int, s: []const u8) Error!void {
        return getErrOrVoid(
            cSqlite.sqlite3_bind_text(self.cStmt, index, s.ptr, @intCast(s.len), cSqlite.SQLITE_TRANSIENT),
        );
    }
};

fn Cursor(comptime Rowtype: type) type {
    if (Rowtype == void) return void;

    return struct {
        const Self = @This();

        stmt: ?*Stmt,
        owns_statement: bool = false,

        pub fn finalize(self: *Self) void {
            var stmt = self.stmt orelse return;
            _ = cSqlite.sqlite3_reset(stmt.cStmt);
            if (self.owns_statement)
                stmt.finalize();
            self.stmt = null;
        }

        pub fn fetch(self: *Self) Error!?Rowtype {
            const stmt = self.stmt orelse return null;
            const rc = cSqlite.sqlite3_step(stmt.cStmt);

            return switch (rc) {
                cSqlite.SQLITE_ROW => self.readRow(),
                cSqlite.SQLITE_DONE => {
                    self.finalize();
                    return null;
                },
                else => stmt.getErrAndMsg(rc),
            };
        }

        fn readRow(self: *Self) Error!?Rowtype {
            var r: Rowtype = undefined;
            inline for (std.meta.fields(Rowtype), 0..) |fld, i| {
                const colval = self.readCol(fld.type, i);
                @field(r, fld.name) = colval;
            }

            return r;
        }

        fn readCol(self: *Self, comptime T: type, i: c_int) T {
            const stmt = self.stmt orelse unreachable;

            switch (@typeInfo(T)) {
                .Bool => {
                    const ccolval = cSqlite.sqlite3_column_int(stmt.cStmt, i);
                    return ccolval != 0;
                },

                .Int => |intInfo| if (intInfo.signedness == .signed) {
                    if (intInfo.bits <= 32) {
                        const ccolval = cSqlite.sqlite3_column_int(stmt.cStmt, i);
                        return @intCast(ccolval);
                    } else {
                        return @intCast(cSqlite.sqlite3_column_int64(stmt.cStmt, i));
                    }
                } else {
                    @compileError("Unsigned field not supported.");
                },

                .Float => {
                    const fcol = cSqlite.sqlite3_column_double(stmt.cStmt, i);
                    return @floatCast(fcol);
                },

                .Pointer => {
                    const textptr = cSqlite.sqlite3_column_text(stmt.cStmt, i);
                    const textlen = cSqlite.sqlite3_column_bytes(stmt.cStmt, i);
                    if (textptr == null or textlen < 0) {
                        return "";
                    }
                    return textptr[0..@intCast(textlen)];
                },

                .Optional => |optInfo| {
                    if (cSqlite.sqlite3_column_type(stmt.cStmt, i) == cSqlite.SQLITE_NULL)
                        return null;
                    return self.readCol(optInfo.child, i);
                },

                else => {},
            }
            unreachable;
        }
    };
}

const test_allocator = std.testing.allocator;
const print = std.debug.print;

test "open db" {
    std.fs.cwd().deleteFile("testdb.db") catch {};

    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    //     errdefer print("got err: {?s}\n", .{db.errmsg});

    try db.exec("select 2+2;", .{});

    try std.testing.expectError(Error.Error, db.exec("select wrong;", .{}));
    try std.testing.expect(db.errmsg != null);
    try std.testing.expectEqualSlices(u8, "no such column: wrong", std.mem.span(db.errmsg.?));
}

test "compile statement" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    //     errdefer print("got err: {?s}\n", .{db.errmsg});

    var stmt = try db.prep("select 2+3");
    defer stmt.finalize();
}

test "don't compile bad statement" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    //     errdefer print("got err: {?s}\n", .{db.errmsg});

    try std.testing.expectError(Error.Error, db.prep("select bad 2+3"));
    try std.testing.expectEqualSlices(u8, "near \"2\": syntax error", std.mem.span(db.errmsg.?));
}

test "execute statement" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    //     errdefer print("got err: {?s}\n", .{db.errmsg});

    var stmt = try db.prep("select 2 + 5");
    defer stmt.finalize();

    var curs = try stmt.exec(.{}, struct { f1: i64, factor: i32 = 4 });
    const row = try curs.fetch();
    //     std.testing.expectEqual(@as(usize, 1), row.len);
    try std.testing.expectEqual(@as(i64, 7), row.?.f1); // read from DB
    // try std.testing.expectEqual(@as(i32, 4), row.?.factor); // default value in row struct
}

test "execute statement - antishortcut" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    // errdefer print("got err: {?s}\n", .{db.errmsg});
    {
        var stmt = try db.prep("create table testtable1 (col1, col2)");
        try stmt.exec(.{}, void);
    }
    {
        var stmt = try db.prep("insert into testtable1 (col1, col2) values (?, ?)");
        try stmt.exec(.{ 1, 2 }, void);
    }

    {
        var stmt = try db.prep("select col1, col2 from testtable1");
        var curs = try stmt.exec(.{}, struct { col1: i32, col2: i32 });
        const r1 = try curs.fetch();
        try std.testing.expectEqual(@TypeOf(r1.?){ .col1 = 1, .col2 = 2 }, r1.?);

        const r2 = try curs.fetch();
        try std.testing.expect(r2 == null);
    }
}

test "execute statement - shortcut" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    // errdefer print("got err: {?s}\n", .{db.errmsg});

    try db.exec("create table testtable1 (col1, col2)", .{});
    try db.exec("insert into testtable1 (col1, col2) values (?, ?)", .{ 1, 2 });
    var c1 = try db.query("select col1, col2 from testtable1", .{}, struct { col1: i32, col2: i32 });
    const r1 = try c1.fetch();
    try std.testing.expectEqual(@TypeOf(r1.?){ .col1 = 1, .col2 = 2 }, r1.?);

    const r2 = try c1.fetch();
    try std.testing.expect(r2 == null);
}

test "do some db stuff" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    // errdefer print("got err: {?s}\n", .{db.errmsg});

    try db.exec("create table testtable1 (col1, col2)", .{});

    var stmt = try db.prep("select * from testtable1");
    defer stmt.finalize();

    var curs = try stmt.exec(.{}, struct { c1: i32, c2: ?[]const u8 });
    const row = try curs.fetch();
    if (row) |payload| {
        std.debug.panic("expected null, found {}", .{payload});
    }

    try db.exec("insert into testtable1 (col1, col2) values (19, 'mucho')", .{});

    var curs2 = try stmt.exec(.{}, struct { c1: i32, c2: ?[]const u8 });
    const row2 = try curs2.fetch();
    try std.testing.expectEqual(@as(i32, 19), row2.?.c1);
    try std.testing.expectEqualStrings("mucho", row2.?.c2.?);
}

test "execute statement with arguments" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    // errdefer print("got err: {?s}\n", .{db.errmsg});

    var stmt = try db.prep("select ? + ?");
    defer stmt.finalize();

    var curs = try stmt.exec(.{ 2, 5 }, struct { sum: i32 });
    const row = try curs.fetch();
    try std.testing.expectEqual(@as(i32, 7), row.?.sum);
}

test "execute statement with named arguments" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    errdefer print("got err: {?s}\n", .{db.errmsg});

    var stmt = try db.prep("select :first - :second");
    defer stmt.finalize();

    var curs = try stmt.exec(.{ .second = 2, .first = 5 }, struct { sum: i32 });
    const row = try curs.fetch();
    try std.testing.expectEqual(@as(i32, 3), row.?.sum);
}

test "more types on statement arguments" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    errdefer print("got err: {?s}\n", .{db.errmsg});

    try db.exec("create table testtable1 (col1, col2)", .{});

    {
        var stmt = try db.prep("insert into testtable1 (col1, col2) values (?, ?)");
        defer stmt.finalize();

        try stmt.exec(.{ "one", "twos" }, void);
        try stmt.exec(.{ null, 4.67 }, void);
    }

    {
        var stmt2 = try db.prep("select col1, col2 from testtable1");
        defer stmt2.finalize();

        const rowtype = struct { c1: ?[]const u8, c2: []const u8 };
        var curs = try stmt2.exec(.{}, rowtype);
        const row1 = try curs.fetch();

        try std.testing.expectEqualStrings("one", row1.?.c1.?);
        try std.testing.expectEqualStrings("twos", row1.?.c2);

        const row2 = try curs.fetch();
        try std.testing.expect(row2.?.c1 == null);
        try std.testing.expectEqualStrings("4.67", row2.?.c2);

        //     const row = try curs.fetch();
        //     std.testing.expectEqual(@as(i32, 3), row.?.sum);
    }
}

test "exec on iteration" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    errdefer print("got err: {?s}\n", .{db.errmsg});

    try db.exec("create table testtable1b (col1, col2)", .{});

    var stmt = try db.prep("insert into testtable1b (col1, col2) values (:uno, :dos)");
    const rows = [_]struct { uno: []const u8, dos: []const u8 }{
        .{ .uno = "A", .dos = "B" },
        .{ .uno = "left", .dos = "right" },
        .{ .uno = "top", .dos = "bottom" },
    };
    for (rows) |row| {
        try stmt.exec(row, void);
    }
}

test "iteration style" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }
    errdefer print("got err: {?s}\n", .{db.errmsg});

    try db.exec(
        \\ create table testtable2 (col1, col2);
        \\ insert into testtable2 (col1, col2) values ("one", 1.2), ("two", 2.3), ("three", 3.4);
    , .{});

    var stmt = try db.prep("select * from testtable2");
    var curs = try stmt.exec(.{}, struct {
        a: []const u8,
        b: f32,
        c: i32 = 4,
    });
    var i: u32 = 0;
    while (try curs.fetch()) |row| : (i += 1) {
        switch (i) {
            0 => {
                try std.testing.expectEqualStrings("one", row.a);
                try std.testing.expectEqual(@as(f32, 1.2), row.b);
                try std.testing.expectEqual(@as(i32, 4), row.c);
            },
            1 => {
                try std.testing.expectEqualStrings("two", row.a);
                try std.testing.expectEqual(@as(f32, 2.3), row.b);
                try std.testing.expectEqual(@as(i32, 4), row.c);
            },
            2 => {
                try std.testing.expectEqualStrings("three", row.a);
                try std.testing.expectEqual(@as(f32, 3.4), row.b);
                try std.testing.expectEqual(@as(i32, 4), row.c);
            },
            else => unreachable,
        }
    }
}

test "read boolean" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }

    {
        var cursor = try db.query("select 1 as exists", .{}, struct { exists: bool });
        const row = try cursor.fetch();
        try std.testing.expect(row != null);
        if (row) |payload| {
            try std.testing.expect(payload.exists == true);
        }
    }

    {
        try db.exec("create table t1 (col1, col2)", .{});
        try db.exec("insert into t1 (col1, col2) values (1, 2)", .{});
        var cursor = try db.query("select exists(select * from t1 where col1 = ? and col2 = ?) as exists", .{ 1, 2 }, struct { exists: bool });
        const row = try cursor.fetch();
        try std.testing.expect(row != null);
        if (row) |payload| {
            try std.testing.expect(payload.exists == true);
        }
    }
}

test "write boolean" {
    std.fs.cwd().deleteFile("testdb.db") catch {};
    var db = try DB.open(test_allocator, "testdb.db");
    defer {
        db.close() catch {};
        std.fs.cwd().deleteFile("testdb.db") catch {};
    }

    try db.exec("create table t1 (col1, col2)", .{});
    try db.exec("insert into t1 (col1, col2) values (?, ?)", .{ true, false });
    var cursor = try db.query("select * from t1", .{}, struct { col1: bool, col2: bool });
    const row = try cursor.fetch();
    try std.testing.expect(row != null);
    if (row) |payload| {
        try std.testing.expect(payload.col1 == true);
        try std.testing.expect(payload.col2 == false);
    }
}
