const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "zigqlite",
        .root_source_file = .{ .path = "src/sqlite.zig" },
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(lib);

    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/sqlite.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.linkSystemLibrary("c");
    main_tests.linkSystemLibrary("sqlite3");

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);
}
