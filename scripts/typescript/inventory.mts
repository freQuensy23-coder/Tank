import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, extname, join, relative, resolve } from "node:path";
import ts from "typescript";

type Kind = "untyped_parameter" | "untyped_return" | "any" | "optional" | "fallback" | "object" | "embedded_python" | "embedded_python_command";
interface Finding {
  path: string;
  line: number;
  symbol: string;
  kind: Kind;
  expression: string;
}
interface EmbeddedSource {
  path: string;
  line: number;
  symbol: string;
  language: "python";
  source: string;
}
interface Inventory {
  coverage: string[];
  files: number;
  counts: Partial<Record<Kind, number>>;
  modules: Record<string, Partial<Record<Kind, number>>>;
  findings: Finding[];
  embedded: EmbeddedSource[];
  immutable_exclusions: Record<string, string>;
}

const excluded = new Set([".git", ".venv", "node_modules", "vendor", "upstream", "history", "testdata", "goldens"]);
const immutable = {
  "tests/fixtures/openma/runtime-codec.ts": "Retained upstream runtime codec fixture; immutable interoperability evidence.",
};

function files(root: string): string[] {
  const result: string[] = [];
  for (const area of ["src", "tests", "scripts", "deploy", "capture"]) {
    const visit = (directory: string): void => {
      for (const entry of readdirSync(directory, { withFileTypes: true })) {
        if (excluded.has(entry.name)) continue;
        const path = join(directory, entry.name);
        const local = relative(root, path);
        if (Object.hasOwn(immutable, local)) continue;
        if (entry.isDirectory()) visit(path);
        else if ([".js", ".mjs", ".ts", ".mts", ".cts"].includes(extname(path))) result.push(path);
      }
    };
    visit(join(root, area));
  }
  return result.sort();
}

function symbol(node: ts.Node, source: ts.SourceFile): string {
  if (ts.isFunctionDeclaration(node) || ts.isMethodDeclaration(node) || ts.isClassDeclaration(node)) {
    return node.name === undefined ? "<anonymous>" : node.name.getText(source);
  }
  if (ts.isVariableDeclaration(node)) return node.name.getText(source);
  return "";
}

export function inventory(root: string): Inventory {
  const findings: Finding[] = [];
  const embedded: EmbeddedSource[] = [];
  const paths = files(root);
  for (const path of paths) {
    const local = relative(root, path);
    const text = readFileSync(path, "utf8");
    const parsed = ts.transpileModule(text, { fileName: path, reportDiagnostics: true, compilerOptions: { allowJs: true, target: ts.ScriptTarget.Latest } });
    if (parsed.diagnostics !== undefined && parsed.diagnostics.length > 0) {
      throw Error(local + ": " + parsed.diagnostics.map((diagnostic: ts.Diagnostic): string => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")).join("\n"));
    }
    const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
    const record = (node: ts.Node, kind: Kind, scope: string[]): void => {
      findings.push({
        path: local,
        line: source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1,
        symbol: scope.join("."),
        kind,
        expression: node.getText(source),
      });
    };
    const visit = (node: ts.Node, parent: string[]): void => {
      const name = symbol(node, source);
      const scope = name === "" ? parent : [...parent, name];
      if (ts.isFunctionLike(node)) {
        for (const parameter of node.parameters) {
          if (parameter.type === undefined) record(parameter, "untyped_parameter", scope);
          if (parameter.questionToken !== undefined || parameter.initializer !== undefined) record(parameter, "optional", scope);
        }
        if (node.type === undefined) record(node, "untyped_return", scope);
      }
      if (node.kind === ts.SyntaxKind.AnyKeyword) record(node, "any", scope);
      if (ts.isObjectLiteralExpression(node)) record(node, "object", scope);
      if (ts.isCallExpression(node) && node.arguments.length > 0 && node.arguments[0]?.getText(source).includes("python3")) record(node, "embedded_python_command", scope);
      if (ts.isBinaryExpression(node) && [ts.SyntaxKind.BarBarToken, ts.SyntaxKind.QuestionQuestionToken].includes(node.operatorToken.kind)) record(node, "fallback", scope);
      if (ts.isTaggedTemplateExpression(node) && node.tag.getText(source) === "String.raw" && ts.isNoSubstitutionTemplateLiteral(node.template)) {
        const text = node.template.getText(source).slice(1, -1);
        if (/^\s*(?:import |from )/m.test(text) && /^\s*(?:def |async def )/m.test(text)) {
          record(node, "embedded_python", scope);
          embedded.push({
            path: local,
            line: source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1,
            symbol: scope.join("."),
            language: "python",
            source: text,
          });
        }
      }
      ts.forEachChild(node, (child: ts.Node): void => visit(child, scope));
    };
    visit(source, []);
  }
  const counts: Partial<Record<Kind, number>> = {};
  const modules: Record<string, Partial<Record<Kind, number>>> = {};
  for (const finding of findings) {
    counts[finding.kind] = (counts[finding.kind] ?? 0) + 1;
    const module = modules[finding.path] ?? {};
    module[finding.kind] = (module[finding.kind] ?? 0) + 1;
    modules[finding.path] = module;
  }
  return {
    coverage: ["first-party JavaScript", "first-party MJS", "first-party TypeScript", "static String.raw Python snippets with import and function declarations", "Python command expressions including dynamic arguments"],
    files: paths.length,
    counts,
    modules,
    findings,
    embedded,
    immutable_exclusions: immutable,
  };
}

export function main(): void {
  const args = process.argv.slice(2);
  const root = resolve(process.cwd());
  if (args.length !== 2 || args[0] !== "--output" || args[1] === undefined) throw Error("Usage: node scripts/ci/refactor_inventory.mjs --output <report.json>");
  const report = inventory(root);
  const output = resolve(args[1]);
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify({ coverage: report.coverage, files: report.files, counts: report.counts, embedded: report.embedded.length }));
}

