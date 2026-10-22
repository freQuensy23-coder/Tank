import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { zipSync, unzipSync } from "fflate";
import { describe, expect, it, vi } from "vitest";
import type { SandboxExecutor } from "@open-managed-agents/sandbox";
import type { Session } from "@open-managed-agents/managed-agents-application";

import { NodeManagedSessionInputPreparer } from "../../../packages/control-plane-node/src/lib/node-managed-session-inputs.js";
import * as nodeManagedInputs from "../../../packages/control-plane-node/src/lib/node-managed-session-inputs.js";

const bytes = (value: string) => new TextEncoder().encode(value);

const knownClaimWorker = readFileSync(new URL('../../../packages/control-plane-node/src/lib/node-managed-skill-aliases.py', import.meta.url), 'utf8');
function knownClaimCommand(command: string, timeout: number, guest: { files: Map<string, Uint8Array>; operations: string[]; binding?: unknown; target?: string; digest?: string }, canonicalRoot: string): string | undefined {
  const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
  const prefix = `python3 -c ${quote(knownClaimWorker)} `;
  if (!command.startsWith(prefix)) return undefined;
  const suffix = command.slice(prefix.length);
  const claim = /^(check|claim) '([A-Za-z0-9+/=]+)'$/u.exec(suffix);
  if (claim) {
    expect(timeout).toBe(30_000);
    const bindings = JSON.parse(Buffer.from(claim[2]!, 'base64').toString());
    expect(bindings).toEqual([{
      name: 'certification-skill', skill_id: 'skill_inputs', version: '123', archive_sha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
    }]);
    expect(guest.operations).toEqual(claim[1] === 'check' ? [] : ['check']);
    guest.binding = bindings[0];
    guest.operations.push(claim[1]!);
    return 'exit=0\nTANK_STABLE_SKILL_BINDINGS_OK\n';
  }
  const batch = /^(stage|materialize|discard) '(\/tmp\/\.openma-skills-[a-f0-9]{32}\/([a-f0-9]{64})\.zip)' '([a-f0-9]{64})'$/u.exec(suffix);
  if (!batch) throw new Error('Unexpected skill worker arguments');
  const [, operation, target, filenameDigest, digest] = batch;
  expect(filenameDigest).toBe(digest);
  expect(timeout).toBe(operation === 'materialize' ? 120_000 : 30_000);
  if (operation === 'stage') {
    expect(guest.operations).toEqual(['check', 'claim']);
    guest.target = target; guest.digest = digest; guest.operations.push(operation);
    return 'exit=0\nTANK_STABLE_SKILL_STAGE_OK\n';
  }
  expect(target).toBe(guest.target); expect(digest).toBe(guest.digest);
  if (operation === 'discard') {
    guest.files.delete(target!);
    return 'exit=0\nTANK_STABLE_SKILL_DISCARD_OK\n';
  }
  expect(guest.operations).toEqual(['check', 'claim', 'stage']);
  const archive = guest.files.get(target!);
  expect(archive).toBeInstanceOf(Uint8Array);
  expect(createHash('sha256').update(archive!).digest('hex')).toBe(digest);
  const view = new DataView(archive!.buffer, archive!.byteOffset, archive!.byteLength);
  let offset = 0, storedEntries = 0;
  while (view.getUint32(offset, true) === 0x04034b50) {
    expect(view.getUint16(offset + 6, true) & 9).toBe(0);
    expect(view.getUint16(offset + 8, true)).toBe(0);
    const compressed = view.getUint32(offset + 18, true);
    expect(compressed).toBe(view.getUint32(offset + 22, true));
    offset += 30 + view.getUint16(offset + 26, true) + view.getUint16(offset + 28, true) + compressed;
    storedEntries++;
  }
  expect(view.getUint32(offset, true)).toBe(0x02014b50);
  const entries = unzipSync(archive!);
  expect(Object.keys(entries)).toHaveLength(storedEntries);
  expect(JSON.parse(Buffer.from(entries['__tank_skill_batch__.json']!).toString())).toEqual({
    version: 1, scopes: [{ binding: guest.binding, canonical_root: canonicalRoot }],
  });
  delete entries['__tank_skill_batch__.json'];
  const binding = guest.binding as { name: string };
  const canonicalPrefix = canonicalRoot.slice(1) + binding.name + '/';
  const aliasPrefix = 'home/user/.skills/' + binding.name + '/';
  expect(Object.keys(entries).length).toBeGreaterThan(0);
  for (const [path, content] of Object.entries(entries)) {
    expect(path.split('/').some(part => part === '.' || part === '..' || part === '')).toBe(false);
    expect(path.includes('\\')).toBe(false);
    const fromCanonical = path.startsWith(canonicalPrefix);
    expect(fromCanonical || path.startsWith(aliasPrefix)).toBe(true);
    const relative = path.slice((fromCanonical ? canonicalPrefix : aliasPrefix).length);
    expect(entries[(fromCanonical ? aliasPrefix : canonicalPrefix) + relative]).toEqual(content);
    guest.files.set('/' + path, content);
  }
  guest.files.delete(target!); guest.operations.push(operation!);
  return 'exit=0\nTANK_STABLE_SKILL_FILES_OK\n';
}

function managedSession(): Session {
  return {
    id: "session_inputs_01",
    agent: {
      id: "agent_inputs_01",
      description: null,
      mcpServers: [],
      model: { id: "model_inputs" },
      multiagent: null,
      name: "Input verifier",
      skills: [{ type: "custom", skillId: "skill_inputs", version: "latest" }],
      system: null,
      tools: [],
      version: 1,
    },
    archivedAt: null,
    budget: null,
    createdAt: "2026-09-08T00:00:00.000Z",
    environmentId: "env_inputs_01",
    metadata: {},
    outcomeEvaluations: [],
    resources: [
      {
        id: "resource_file_01",
        type: "file",
        fileId: "file_inputs_01",
        mountPath: "/workspace/inputs/attached.txt",
        createdAt: "2026-09-08T00:00:00.000Z",
        updatedAt: "2026-09-08T00:00:00.000Z",
      },
      {
        id: "resource_repo_01",
        type: "github_repository",
        url: "http://git.example.test/repository.git",
        mountPath: "/workspace/repository",
        checkout: { type: "commit", sha: "abcdef0123456789" },
        createdAt: "2026-09-08T00:00:00.000Z",
        updatedAt: "2026-09-08T00:00:00.000Z",
      },
    ],
    stats: {},
    status: "running",
    title: null,
    updatedAt: "2026-09-08T00:00:00.000Z",
    usage: {},
    vaultIds: [],
  };
}

describe("NodeManagedSessionInputPreparer", () => {
  it("describes mounted custom skills for inclusion in the model system prompt", () => {
    const buildReminders = (nodeManagedInputs as Record<string, unknown>)
      .buildNodeManagedSkillReminders as undefined | ((session: Session) => Array<{
        source: string;
        text: string;
      }>);
    expect(buildReminders).toBeTypeOf("function");
    if (buildReminders === undefined) return;

    expect(buildReminders(managedSession())).toEqual([{
      source: "skill:skill_inputs",
      text: "Custom skill skill_inputs is mounted at /workspace/.openma/skills/skill_inputs/latest/. Locate and read its SKILL.md before applying it.",
    }]);
  });

  it("resolves version-pinned appendable prompts into system prompt reminders", () => {
    const session = managedSession();
    session.agent.openma = { appendablePrompts: ["linear-mcp", "missing"] };
    const buildReminders = (nodeManagedInputs as Record<string, unknown>)
      .buildNodeManagedAppendablePromptReminders as undefined | ((session: Session) => Array<{
        source: string;
        text: string;
      }>);
    expect(buildReminders).toBeTypeOf("function");
    if (buildReminders === undefined) return;

    expect(buildReminders(session)).toEqual([
      expect.objectContaining({
        source: "appendable:linear-mcp",
        text: expect.stringContaining("linear_say"),
      }),
    ]);
  });

  it("prepares local provider outputs without calling a FUSE mount when shared OSS publication is selected", async () => {
    const session = managedSession();
    session.resources = [];
    session.agent.skills = [];
    const commands: string[] = [];
    let mounted = false;
    let executed = false;
    const preparer = new NodeManagedSessionInputPreparer({
      sharedOutputs: true,
      files: {} as never, skillVersions: {} as never,
      repositoryCredentials: {} as never, memorySnapshots: {} as never,
    });
    const sandbox = {
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: async () => { mounted = true; throw new Error("FUSE unavailable"); },
      exec: async (command: string) => { executed = true; commands.push(command); return ""; },
      setEnvVars: async (env: Record<string, string>) => {
        expect(executed).toBe(false);
        expect(env).toEqual({ OMA_OUTPUTS_DIR: "/mnt/session/outputs" });
      },
    } as unknown as SandboxExecutor;
    await preparer.prepare({ workspaceId: "default", session, sandbox, runtimeGeneration: "runtime_1" });
    expect(mounted).toBe(false);
    expect(commands).toEqual(["mkdir -p /mnt/session/outputs"]);
  });

  it("materializes file, pinned repository revision, and custom skill before execution", async () => {
    const guest = { files: new Map<string, Uint8Array>(), operations: [] as string[] };
    const writeFileBytes = vi.fn(async (path: string, content: Uint8Array) => { guest.files.set(path, content); return "written"; });
    const gitCheckout = vi.fn(async () => undefined);
    const exec = vi.fn(async (command: string, timeout: number) => knownClaimCommand(command, timeout, guest, "/workspace/.openma/skills/skill_inputs/latest/") ?? "");
    const sandbox = {
      writeFileBytes,
      gitCheckout,
      exec,
      readFile: vi.fn(async () => { throw new Error("missing"); }),
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: vi.fn(async () => undefined),
      registerCommandSecrets: vi.fn(),
    } as unknown as SandboxExecutor;
    const files = {
      downloadFile: vi.fn(async () => ({
        type: "found" as const,
        file: {
          content: bytes("FILE_INPUT_OK"),
          filename: "attached.txt",
          mimeType: "text/plain",
        },
      })),
    };
    const skillArchive = zipSync({
      "certification-skill/SKILL.md": bytes(
        "---\nname: certification-skill\ndescription: Verify mounted inputs\n---\nSKILL_INPUT_OK\n",
      ),
      "certification-skill/reference.txt": bytes("REFERENCE_OK"),
    });
    const skillVersions = {
      listSkillVersions: vi.fn(async () => ({
        type: "page" as const,
        page: {
          versions: [{
            id: "skv_inputs_01",
            createdAt: "2026-09-08T00:00:00.000Z",
            description: "Verify mounted inputs",
            directory: "certification-skill",
            name: "certification-skill",
            skillId: "skill_inputs",
            version: "123",
          }],
          nextCursor: null,
        },
      })),
      downloadSkillVersion: vi.fn(async () => ({
        type: "found" as const,
        file: {
          content: skillArchive,
          filename: "certification-skill.zip",
          mimeType: "application/zip",
        },
      })),
    };
    const repositoryCredentials = {
      findGithubToken: vi.fn(async () => "ghp_private"),
    };
    const preparer = new NodeManagedSessionInputPreparer({
      files,
      skillVersions,
      repositoryCredentials,
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });

    await preparer.prepare({
      workspaceId: "workspace_inputs",
      session: managedSession(),
      sandbox,
    });

    expect(files.downloadFile).toHaveBeenCalledWith({ fileId: "file_inputs_01" });
    expect(writeFileBytes).toHaveBeenCalledWith(
      "/workspace/inputs/attached.txt",
      bytes("FILE_INPUT_OK"),
    );
    expect(gitCheckout).toHaveBeenCalledWith(
      "http://git.example.test/repository.git",
      { targetDir: "/workspace/repository" },
    );
    expect(repositoryCredentials.findGithubToken).toHaveBeenCalledWith({
      workspaceId: "workspace_inputs",
      sessionId: "session_inputs_01",
      resourceId: "resource_repo_01",
    });
    expect(sandbox.registerCommandSecrets).toHaveBeenCalledWith("git", {
      GIT_CONFIG_COUNT: "1",
      GIT_CONFIG_KEY_0: "http.http://git.example.test/repository.git.extraHeader",
      GIT_CONFIG_VALUE_0: "Authorization: Basic eC1hY2Nlc3MtdG9rZW46Z2hwX3ByaXZhdGU=",
    });
    expect(exec).toHaveBeenCalledWith(
      "git -C 'repository' checkout --detach 'abcdef0123456789'",
      60_000,
    );
    expect(skillVersions.listSkillVersions).toHaveBeenCalledWith({
      skillId: "skill_inputs",
      pageSize: 1,
    });
    expect(skillVersions.downloadSkillVersion).toHaveBeenCalledWith({
      skillId: "skill_inputs",
      version: "123",
    });
    for (const [path, content] of Object.entries(unzipSync(skillArchive))) {
      expect(guest.files.get("/workspace/.openma/skills/skill_inputs/latest/" + path)).toEqual(content);
      expect(guest.files.get("/home/user/.skills/" + path)).toEqual(content);
    }
    expect(guest.operations).toEqual(["check", "claim", "stage", "materialize"]);
  });

  it("mounts durable outputs and memory stores before materializing inputs", async () => {
    const order: string[] = [];
    const guest = { files: new Map<string, Uint8Array>(), operations: [] as string[] };
    const session = managedSession();
    session.resources.push({
      type: "memory_store",
      memoryStoreId: "memory_01",
      name: "project-memory",
      access: "read_only",
      mountPath: "/mnt/memory/project-memory",
    });
    const sandbox = {
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: async () => { order.push("outputs"); },
      mountMemoryStore: async () => { order.push("memory-mount"); },
      writeFileBytes: async (path: string, content: Uint8Array) => { order.push("file-or-skill"); guest.files.set(path, content); return "written"; },
      gitCheckout: async () => { order.push("repository"); },
      exec: async (command: string, timeout: number) => knownClaimCommand(command, timeout, guest, "/workspace/.openma/skills/skill_inputs/latest/") ?? "",
      readFile: async () => { throw new Error("missing"); },
      registerCommandSecrets: () => undefined,
    } as unknown as SandboxExecutor;
    const preparer = new NodeManagedSessionInputPreparer({
      files: {
        downloadFile: async () => ({
          type: "found" as const,
          file: { content: bytes("FILE_INPUT_OK"), mimeType: "text/plain" },
        }),
      },
      skillVersions: {
        listSkillVersions: async () => ({
          type: "page" as const,
          page: {
            versions: [{
              id: "skv_inputs_01",
              createdAt: "2026-09-08T00:00:00.000Z",
              description: "Verify mounted inputs",
              directory: "certification-skill",
              name: "certification-skill",
              skillId: "skill_inputs",
              version: "123",
            }],
            nextCursor: null,
          },
        }),
        downloadSkillVersion: async () => ({
          type: "found" as const,
          file: {
            content: zipSync({ "certification-skill/SKILL.md": bytes("---\nname: certification-skill\ndescription: Verify mounted inputs\n---\nSKILL_INPUT_OK") }),
            mimeType: "application/zip",
          },
        }),
      },
      repositoryCredentials: { findGithubToken: async () => "ghp_private" },
      memorySnapshots: {
        materialize: async () => {
          order.push("memory-snapshot");
          return { mountStoreId: "managed-snapshot/memory_01/revision" };
        },
      },
    });

    await preparer.prepare({ workspaceId: "workspace_inputs", session, sandbox });

    expect(order.slice(0, 3)).toEqual(["outputs", "memory-snapshot", "memory-mount"]);
  });

  it("validates a retained repository before preserving its working tree", async () => {
    const session = managedSession();
    session.agent.skills = [];
    session.resources = [session.resources[1]!];
    const exec = vi.fn(async (command: string) => {
      if (command.includes("remote get-url origin")) {
        return "http://git.example.test/repository.git";
      }
      return "";
    });
    const sandbox = {
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: vi.fn(async () => undefined),
      readFile: vi.fn(async () => "ref: refs/heads/main"),
      exec,
      gitCheckout: vi.fn(async () => undefined),
      registerCommandSecrets: vi.fn(),
    } as unknown as SandboxExecutor;
    const preparer = new NodeManagedSessionInputPreparer({
      files: { downloadFile: async () => { throw new Error("unexpected file download"); } },
      skillVersions: {
        listSkillVersions: async () => { throw new Error("unexpected skill listing"); },
        downloadSkillVersion: async () => { throw new Error("unexpected skill download"); },
      },
      repositoryCredentials: { findGithubToken: async () => "ghp_private" },
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });

    await preparer.prepare({ workspaceId: "workspace_inputs", session, sandbox });

    expect(exec).toHaveBeenCalledWith(
      "git -C 'repository' remote get-url origin",
      30_000,
    );
    expect(exec).toHaveBeenCalledWith(
      "git -C 'repository' cat-file -e 'abcdef0123456789^{commit}'",
      30_000,
    );
    expect(sandbox.gitCheckout).not.toHaveBeenCalled();
  });

  it("fails closed when retained repository identity does not match the Session resource", async () => {
    const session = managedSession();
    session.agent.skills = [];
    session.resources = [session.resources[1]!];
    const sandbox = {
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: vi.fn(async () => undefined),
      readFile: vi.fn(async () => "ref: refs/heads/main"),
      exec: vi.fn(async () => "http://git.example.test/another-repository.git"),
      registerCommandSecrets: vi.fn(),
    } as unknown as SandboxExecutor;
    const preparer = new NodeManagedSessionInputPreparer({
      files: { downloadFile: async () => { throw new Error("unexpected file download"); } },
      skillVersions: {
        listSkillVersions: async () => { throw new Error("unexpected skill listing"); },
        downloadSkillVersion: async () => { throw new Error("unexpected skill download"); },
      },
      repositoryCredentials: { findGithubToken: async () => "ghp_private" },
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });

    await expect(preparer.prepare({
      workspaceId: "workspace_inputs",
      session,
      sandbox,
    })).rejects.toThrow(/retained repository.*does not match/i);
  });

  it("fails closed when the selected sandbox cannot mount durable outputs", async () => {
    const preparer = new NodeManagedSessionInputPreparer({
      files: { downloadFile: async () => { throw new Error("unexpected file download"); } },
      skillVersions: {
        listSkillVersions: async () => { throw new Error("unexpected skill listing"); },
        downloadSkillVersion: async () => { throw new Error("unexpected skill download"); },
      },
      repositoryCredentials: { findGithubToken: async () => null },
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });

    await expect(preparer.prepare({
      workspaceId: "workspace_inputs",
      session: { ...managedSession(), resources: [], agent: { ...managedSession().agent, skills: [] } },
      sandbox: {} as SandboxExecutor,
    })).rejects.toThrow(/durable session outputs/i);
  });

  it("does not require binary writes for provider-owned built-in skills", async () => {
    const preparer = new NodeManagedSessionInputPreparer({
      files: { downloadFile: async () => { throw new Error("unexpected file download"); } },
      skillVersions: {
        listSkillVersions: async () => { throw new Error("unexpected skill listing"); },
        downloadSkillVersion: async () => { throw new Error("unexpected skill download"); },
      },
      repositoryCredentials: { findGithubToken: async () => null },
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });
    const session = managedSession();
    session.resources = [];
    session.agent.skills = [{ type: "anthropic", skillId: "pdf" }];

    await expect(preparer.prepare({
      workspaceId: "workspace_inputs",
      session,
      sandbox: {
        sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
        mountSessionOutputs: async () => undefined,
      } as unknown as SandboxExecutor,
    })).resolves.toBeUndefined();
  });

  it("fails closed when a memory store is attached to a sandbox without memory mounts", async () => {
    const session = managedSession();
    session.agent.skills = [];
    session.resources = [{
      type: "memory_store",
      memoryStoreId: "memory_01",
      name: "project-memory",
      access: "read_write",
    }];
    const preparer = new NodeManagedSessionInputPreparer({
      files: { downloadFile: async () => { throw new Error("unexpected file download"); } },
      skillVersions: {
        listSkillVersions: async () => { throw new Error("unexpected skill listing"); },
        downloadSkillVersion: async () => { throw new Error("unexpected skill download"); },
      },
      repositoryCredentials: { findGithubToken: async () => null },
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });
    const sandbox = {
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: async () => undefined,
    } as unknown as SandboxExecutor;

    await expect(preparer.prepare({
      workspaceId: "workspace_inputs",
      session,
      sandbox,
    })).rejects.toThrow(/memory store mounts/i);
  });

  it("materializes read-write Managed Memory in a fenced runtime generation", async () => {
    const session = managedSession();
    session.agent.skills = [];
    session.resources = [{
      type: "memory_store",
      memoryStoreId: "memory_01",
      name: "project-memory",
      access: "read_write",
    }];
    const materialize = vi.fn(async () => ({ mountStoreId: "unused" }));
    const mountMemoryStore = vi.fn(async () => undefined);
    const setEnvVars = vi.fn(async () => undefined);
    const preparer = new NodeManagedSessionInputPreparer({
      files: { downloadFile: async () => { throw new Error("unexpected file download"); } },
      skillVersions: {
        listSkillVersions: async () => { throw new Error("unexpected skill listing"); },
        downloadSkillVersion: async () => { throw new Error("unexpected skill download"); },
      },
      repositoryCredentials: { findGithubToken: async () => null },
      memorySnapshots: { materialize },
    });
    const sandbox = {
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: async () => undefined,
      mountMemoryStore,
      setEnvVars,
    } as unknown as SandboxExecutor;

    await expect(preparer.prepare({
      workspaceId: "workspace_inputs",
      session,
      sandbox,
      runtimeGeneration: "runtime_generation_01",
    })).resolves.toBeUndefined();
    expect(materialize).toHaveBeenCalledWith({
      workspaceId: "workspace_inputs",
      sessionId: "session_inputs_01",
      memoryStoreId: "memory_01",
      access: "read_write",
      runtimeGeneration: "runtime_generation_01",
    });
    expect(mountMemoryStore).toHaveBeenCalledWith({
      storeName: "project-memory",
      storeId: "unused",
      readOnly: false,
    });
    expect(setEnvVars).toHaveBeenCalledWith({
      OMA_OUTPUTS_DIR: "/mnt/session/outputs",
    });
    expect(setEnvVars).toHaveBeenCalledWith({
      OMA_MEMORY_DIR: "/mnt/memory",
      OMA_MEMORY_PROJECT_MEMORY: "/mnt/memory/project-memory",
    });
  });

  it("fails closed when a skill archive contains a parent traversal", async () => {
    const preparer = new NodeManagedSessionInputPreparer({
      files: {
        downloadFile: vi.fn(async () => ({
          type: "found" as const,
          file: { content: bytes("FILE_INPUT_OK"), mimeType: "text/plain" },
        })),
      },
      skillVersions: {
        listSkillVersions: vi.fn(async () => ({
          type: "page" as const,
          page: {
            versions: [{
              id: "skv_inputs_01",
              createdAt: "2026-09-08T00:00:00.000Z",
              description: "unsafe",
              directory: "unsafe",
              name: "unsafe",
              skillId: "skill_inputs",
              version: "123",
            }],
            nextCursor: null,
          },
        })),
        downloadSkillVersion: vi.fn(async () => ({
          type: "found" as const,
          file: {
            content: zipSync({ "../SKILL.md": bytes("unsafe") }),
            filename: "unsafe.zip",
            mimeType: "application/zip",
          },
        })),
      },
      repositoryCredentials: { findGithubToken: async () => null },
      memorySnapshots: { materialize: async () => ({ mountStoreId: "unused" }) },
    });
    const session = managedSession();
    session.resources = [];
    const sandbox = {
      writeFileBytes: vi.fn(async () => "written"),
      readFile: vi.fn(async () => ""),
      exec: vi.fn(async () => ""),
      sessionOutputMountCapabilities: () => ({ durability: "durable" as const }),
      mountSessionOutputs: vi.fn(async () => undefined),
    } as unknown as SandboxExecutor;

    await expect(preparer.prepare({
      workspaceId: "workspace_inputs",
      session,
      sandbox,
    })).rejects.toThrow(/unsafe skill archive path/i);
  });
});
