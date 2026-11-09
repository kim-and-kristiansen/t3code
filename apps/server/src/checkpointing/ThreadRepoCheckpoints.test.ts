// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { describe, expect } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as CheckpointStore from "./CheckpointStore.ts";
import * as ThreadRepoCheckpoints from "./ThreadRepoCheckpoints.ts";

const layerNode = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
const layerVcsProcess = VcsProcess.layer.pipe(Layer.provide(layerNode));
const layerVcsDriverRegistry = VcsDriverRegistry.layer.pipe(Layer.provide(layerVcsProcess));
const layerTest = ThreadRepoCheckpoints.layer.pipe(
  Layer.provide(CheckpointStore.layer.pipe(Layer.provide(layerVcsDriverRegistry))),
  Layer.provideMerge(layerVcsDriverRegistry),
  Layer.provideMerge(layerVcsProcess),
  Layer.provideMerge(
    ServerConfig.ServerConfig.layerTest(process.cwd(), { prefix: "t3-thread-repo-test-" }),
  ),
  Layer.provideMerge(layerNode),
);

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const vcsProcess = yield* VcsProcess.VcsProcess;
    yield* vcsProcess.run({
      operation: "ThreadRepoCheckpoints.test.git",
      command: "git",
      cwd,
      args,
      timeoutMs: 10_000,
    });
  });

const makeRepo = (parent: string, name: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const cwd = NodePath.join(parent, name);
    yield* fileSystem.makeDirectory(cwd);
    yield* git(cwd, ["init"]);
    yield* git(cwd, ["config", "user.email", "test@test.com"]);
    yield* git(cwd, ["config", "user.name", "Test"]);
    yield* fileSystem.writeFileString(NodePath.join(cwd, "README.md"), "# test\n");
    yield* git(cwd, ["add", "."]);
    yield* git(cwd, ["commit", "-m", "initial commit"]);
    return yield* fileSystem.realPath(cwd);
  });

it.layer(layerTest)("ThreadRepoCheckpoints", (it) => {
  describe("a repository outside the project", () => {
    it.effect("diffs only the thread's edits, prefixed with the repository name", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const repos = yield* ThreadRepoCheckpoints.ThreadRepoCheckpoints;
        const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "thread-repos-" });
        const project = yield* makeRepo(workspace, "project");
        const sibling = yield* makeRepo(workspace, "sibling");
        const threadId = ThreadId.make("thread-repo-checkpoints-test");
        yield* fileSystem.writeFileString(NodePath.join(sibling, "dirty.txt"), "before\n");

        const editedFile = NodePath.join(sibling, "src", "new.ts");
        yield* repos.track({ threadId, rootCwd: project, runOrdinal: 1, filePath: editedFile });
        yield* repos.track({ threadId, rootCwd: project, runOrdinal: 1, filePath: editedFile });
        yield* repos.track({
          threadId,
          rootCwd: project,
          runOrdinal: 1,
          filePath: NodePath.join(project, "README.md"),
        });
        yield* fileSystem.makeDirectory(NodePath.dirname(editedFile));
        yield* fileSystem.writeFileString(editedFile, "export const a = 1;\n");

        expect(yield* repos.capture({ threadId, ordinal: 1 })).toEqual([
          { path: "sibling/src/new.ts", kind: "modified", additions: 1, deletions: 0 },
        ]);

        yield* repos.captureBaselines({ threadId, ordinal: 1 });
        yield* fileSystem.writeFileString(NodePath.join(sibling, "README.md"), "# changed\n");
        yield* repos.capture({ threadId, ordinal: 2 });

        const diff = (fromOrdinal: number, toOrdinal: number) =>
          repos.diff({
            threadId,
            fromOrdinal,
            toOrdinal,
            ignoreWhitespace: false,
            maxOutputBytes: 1_000_000,
          });
        const firstTurn = yield* diff(0, 1);
        expect(firstTurn).toContain("diff --git a/sibling/src/new.ts b/sibling/src/new.ts");
        expect(firstTurn).not.toContain("dirty.txt");
        expect(firstTurn).not.toContain("README.md");

        const secondTurn = yield* diff(1, 2);
        expect(secondTurn).toContain("a/sibling/README.md");
        expect(secondTurn).not.toContain("new.ts");

        const wholeThread = yield* diff(0, 2);
        expect(wholeThread).toContain("a/sibling/src/new.ts");
        expect(wholeThread).toContain("a/sibling/README.md");
        expect(wholeThread).not.toContain("dirty.txt");
      }),
    );
  });
});
