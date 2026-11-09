import {
  CheckpointRef,
  type OrchestrationV2CheckpointFileSummary,
  type ThreadId,
} from "@t3tools/contracts";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as CheckpointStore from "./CheckpointStore.ts";
import type { CheckpointStoreError } from "./Errors.ts";
import { isGitImport, parseTurnDiffFilesFromNumstat } from "./Diffs.ts";

const REPO_REFS_PREFIX = "refs/t3/orchestration-v2/repos";

const Registry = Schema.Struct({ repos: Schema.Array(Schema.String) });
const decodeRegistry = Schema.decodeUnknownEffect(Schema.fromJsonString(Registry));
const encodeRegistry = Schema.encodeEffect(Schema.fromJsonString(Registry));

export interface ThreadRepoCheckpointsShape {
  /** Starts tracking the repository containing `filePath`, snapshotting it before the edit lands. */
  readonly track: (input: {
    readonly threadId: ThreadId;
    readonly rootCwd: string;
    readonly runOrdinal: number;
    readonly filePath: string;
  }) => Effect.Effect<void>;
  readonly captureBaselines: (input: {
    readonly threadId: ThreadId;
    readonly ordinal: number;
  }) => Effect.Effect<void>;
  readonly capture: (input: {
    readonly threadId: ThreadId;
    readonly ordinal: number;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2CheckpointFileSummary>>;
  readonly diff: (input: {
    readonly threadId: ThreadId;
    readonly fromOrdinal: number;
    readonly toOrdinal: number;
    readonly ignoreWhitespace: boolean;
    readonly maxOutputBytes: number;
  }) => Effect.Effect<string>;
}

export class ThreadRepoCheckpoints extends Context.Service<
  ThreadRepoCheckpoints,
  ThreadRepoCheckpointsShape
>()("t3/checkpointing/ThreadRepoCheckpoints") {}

/** @public */
export const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const checkpointStore = yield* CheckpointStore.CheckpointStore;
  const vcsRegistry = yield* VcsDriverRegistry.VcsDriverRegistry;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const crypto = yield* Digest.Digest;
  const threadLocks = yield* KeyedLock.make<ThreadId>();
  const registryDir = path.join(config.stateDir, "thread-repos");

  const hashKey = (value: string) =>
    crypto.digest(Digest.sha256, new TextEncoder().encode(value)).pipe(
      Effect.orDie,
      Effect.map((digest) => Hex.encode(digest).slice(0, 32)),
    );

  const refFor = Effect.fn("ThreadRepoCheckpoints.refFor")(function* (
    threadId: ThreadId,
    repoRoot: string,
    ordinal: number,
  ) {
    const threadKey = yield* hashKey(threadId);
    const repoKey = yield* hashKey(repoRoot);
    return CheckpointRef.make(`${REPO_REFS_PREFIX}/${threadKey}/${repoKey}/ordinal/${ordinal}`);
  });

  const registryPath = (threadId: ThreadId) =>
    hashKey(threadId).pipe(Effect.map((key) => path.join(registryDir, `${key}.json`)));

  const readRepos = Effect.fn("ThreadRepoCheckpoints.readRepos")(function* (threadId: ThreadId) {
    const filePath = yield* registryPath(threadId);
    const raw = yield* fileSystem.readFileString(filePath).pipe(Effect.orElseSucceed(() => ""));
    if (raw.trim().length === 0) return [];
    return yield* decodeRegistry(raw).pipe(
      Effect.map((registry) => registry.repos),
      Effect.orElseSucceed(() => []),
    );
  });

  const writeRepos = Effect.fn("ThreadRepoCheckpoints.writeRepos")(function* (
    threadId: ThreadId,
    repos: ReadonlyArray<string>,
  ) {
    const filePath = yield* registryPath(threadId);
    const contents = yield* encodeRegistry({ repos });
    yield* writeFileStringAtomically({ filePath, contents: `${contents}\n` }).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.provideService(HostProcess.Platform, platform),
    );
  });

  const repositoryRoot = (cwd: string) =>
    vcsRegistry.detect({ cwd, requestedKind: "git" }).pipe(
      Effect.map((handle) => handle?.repository.rootPath ?? null),
      Effect.orElseSucceed(() => null),
    );

  const nearestExistingDirectory = Effect.fn("ThreadRepoCheckpoints.nearestExistingDirectory")(
    function* (filePath: string) {
      let directory = path.dirname(filePath);
      while (!(yield* fileSystem.exists(directory).pipe(Effect.orElseSucceed(() => false)))) {
        const parent = path.dirname(directory);
        if (parent === directory) return null;
        directory = parent;
      }
      return directory;
    },
  );

  const captureIfMissing = Effect.fn("ThreadRepoCheckpoints.captureIfMissing")(function* (
    threadId: ThreadId,
    repoRoot: string,
    ordinal: number,
  ) {
    const checkpointRef = yield* refFor(threadId, repoRoot, ordinal);
    if (yield* checkpointStore.hasCheckpointRef({ cwd: repoRoot, checkpointRef })) return;
    yield* checkpointStore.captureCheckpoint({ cwd: repoRoot, checkpointRef });
  });

  // Extra repositories are best effort; they must never fail the project's own checkpoint.
  const orWarn = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    fallback: () => A,
    message: string,
    annotations: Record<string, unknown>,
  ) =>
    effect.pipe(
      Effect.catch((cause) =>
        Effect.logWarning(message, { ...annotations, cause: String(cause) }).pipe(
          Effect.map(fallback),
        ),
      ),
    );

  const track: ThreadRepoCheckpointsShape["track"] = Effect.fn("ThreadRepoCheckpoints.track")(
    function* (input) {
      const absolutePath = path.resolve(input.rootCwd, input.filePath);
      const directory = yield* nearestExistingDirectory(absolutePath);
      if (directory === null) return;
      const repoRoot = yield* repositoryRoot(directory);
      if (repoRoot === null || repoRoot === (yield* repositoryRoot(input.rootCwd))) return;

      yield* threadLocks.withLock(
        input.threadId,
        Effect.gen(function* () {
          const repos = yield* readRepos(input.threadId);
          if (repos.includes(repoRoot)) return;
          yield* captureIfMissing(input.threadId, repoRoot, Math.max(0, input.runOrdinal - 1));
          yield* writeRepos(input.threadId, [...repos, repoRoot]);
        }),
      );
    },
    (effect, input) =>
      orWarn(effect, () => undefined, "thread repo checkpoint tracking failed", {
        threadId: input.threadId,
        filePath: input.filePath,
      }),
  );

  const captureBaselines: ThreadRepoCheckpointsShape["captureBaselines"] = Effect.fn(
    "ThreadRepoCheckpoints.captureBaselines",
  )(function* (input) {
    const repos = yield* readRepos(input.threadId);
    yield* Effect.forEach(
      repos,
      (repoRoot) =>
        orWarn(
          captureIfMissing(input.threadId, repoRoot, input.ordinal),
          () => undefined,
          "thread repo baseline capture failed",
          { threadId: input.threadId, repoRoot },
        ),
      { concurrency: 4, discard: true },
    );
  });

  const captureRepo = Effect.fn("ThreadRepoCheckpoints.captureRepo")(function* (
    threadId: ThreadId,
    repoRoot: string,
    ordinal: number,
  ): Effect.fn.Return<ReadonlyArray<OrchestrationV2CheckpointFileSummary>, CheckpointStoreError> {
    const toCheckpointRef = yield* refFor(threadId, repoRoot, ordinal);
    const fromCheckpointRef = yield* refFor(threadId, repoRoot, Math.max(0, ordinal - 1));
    yield* checkpointStore.captureCheckpoint({ cwd: repoRoot, checkpointRef: toCheckpointRef });
    if (
      !(yield* checkpointStore.hasCheckpointRef({
        cwd: repoRoot,
        checkpointRef: fromCheckpointRef,
      }))
    ) {
      return [];
    }
    const refs = { cwd: repoRoot, fromCheckpointRef, toCheckpointRef };
    const [numstat, authoredPaths] = yield* Effect.all([
      checkpointStore.diffCheckpoints({ ...refs, ignoreWhitespace: false, format: "numstat" }),
      checkpointStore.listAuthoredPaths(refs).pipe(Effect.orElseSucceed(() => null)),
    ]);
    const label = path.basename(repoRoot);
    return parseTurnDiffFilesFromNumstat(numstat)
      .filter((file) => !isGitImport(file, authoredPaths))
      .map((file) => ({
        path: `${label}/${file.path}`,
        kind: "modified",
        additions: file.additions,
        deletions: file.deletions,
      }));
  });

  const capture: ThreadRepoCheckpointsShape["capture"] = Effect.fn("ThreadRepoCheckpoints.capture")(
    function* (input) {
      const repos = yield* readRepos(input.threadId);
      const files = yield* Effect.forEach(
        repos,
        (repoRoot) =>
          orWarn(
            captureRepo(input.threadId, repoRoot, input.ordinal),
            () => [],
            "thread repo checkpoint capture failed",
            { threadId: input.threadId, repoRoot },
          ),
        { concurrency: 4 },
      );
      return files.flat();
    },
  );

  const diffRepo = Effect.fn("ThreadRepoCheckpoints.diffRepo")(function* (
    input: Parameters<ThreadRepoCheckpointsShape["diff"]>[0],
    repoRoot: string,
    maxOutputBytes: number,
  ) {
    const toCheckpointRef = yield* refFor(input.threadId, repoRoot, input.toOrdinal);
    if (
      !(yield* checkpointStore.hasCheckpointRef({ cwd: repoRoot, checkpointRef: toCheckpointRef }))
    ) {
      return "";
    }
    // A repository first touched mid-thread has no refs before its first baseline.
    let fromCheckpointRef: CheckpointRef | null = null;
    for (let ordinal = input.fromOrdinal; ordinal < input.toOrdinal; ordinal += 1) {
      const candidate = yield* refFor(input.threadId, repoRoot, ordinal);
      if (yield* checkpointStore.hasCheckpointRef({ cwd: repoRoot, checkpointRef: candidate })) {
        fromCheckpointRef = candidate;
        break;
      }
    }
    if (fromCheckpointRef === null) return "";

    const comparison = {
      cwd: repoRoot,
      fromCheckpointRef,
      toCheckpointRef,
      ignoreWhitespace: input.ignoreWhitespace,
    };
    const authoredPaths = yield* checkpointStore
      .listAuthoredPaths(comparison)
      .pipe(Effect.orElseSucceed(() => null));
    const files = parseTurnDiffFilesFromNumstat(
      yield* checkpointStore.diffCheckpoints({ ...comparison, format: "numstat" }),
    );
    const retained = files.filter((file) => !isGitImport(file, authoredPaths));
    if (retained.length === 0) return "";
    return yield* checkpointStore.diffCheckpoints({
      ...comparison,
      pathPrefix: path.basename(repoRoot),
      maxOutputBytes,
      ...(retained.length === files.length
        ? {}
        : {
            filePaths: retained.flatMap((file) =>
              file.previousPath === undefined ? [file.path] : [file.previousPath, file.path],
            ),
          }),
    });
  });

  const diff: ThreadRepoCheckpointsShape["diff"] = Effect.fn("ThreadRepoCheckpoints.diff")(
    function* (input) {
      const repos = yield* readRepos(input.threadId);
      let combined = "";
      for (const repoRoot of repos) {
        const remaining = input.maxOutputBytes - Buffer.byteLength(combined);
        if (remaining <= 0) break;
        combined += yield* orWarn(
          diffRepo(input, repoRoot, remaining),
          () => "",
          "thread repo checkpoint diff failed",
          { threadId: input.threadId, repoRoot },
        );
      }
      return combined;
    },
  );

  return ThreadRepoCheckpoints.of({ track, captureBaselines, capture, diff });
});

export const layer = Layer.effect(ThreadRepoCheckpoints, make);
