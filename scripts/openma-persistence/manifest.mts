export interface PersistencePackage {
  readonly name: string;
  readonly version: string;
  readonly integrity: string;
  readonly heading: string;
}
export interface PersistenceImporter {
  readonly name: string;
  readonly section: string;
  readonly dependency: string;
  readonly specifier: string;
  readonly version: string;
}
export const PACKAGES: readonly PersistencePackage[] = [
  {
    "name": "postgres",
    "version": "3.4.9",
    "integrity": "sha512-GD3qdB0x1z9xgFI6cdRD6xu2Sp2WCOEoe3mtnyB5Ee0XrrL5Pe+e4CCnJrRMnL1zYtRDZmQQVbvOttLnKDLnaw==",
    "heading": "postgres@3.4.9"
  },
  {
    "name": "effect",
    "version": "3.22.1",
    "integrity": "sha512-TNoXushmPOBAjJlthF5d2QwnX2xBPEtcNJr5XKNKbRLbDvBcOYkXlYDfvGfSA0zriwLFuCll5MDtNMAdZL17PQ==",
    "heading": "effect@3.22.1"
  },
  {
    "name": "@standard-schema/spec",
    "version": "1.1.0",
    "integrity": "sha512-l2aFy5jALhniG5HgqrD6jXLi/rUWrKvqN/qJx6yoJsgKhblVd+iqqU4RCXavm/jPityDo5TCvKMnpjKnOriy0w==",
    "heading": "'@standard-schema/spec@1.1.0'"
  },
  {
    "name": "fast-check",
    "version": "3.23.2",
    "integrity": "sha512-h5+1OzzfCC3Ef7VbtKdcv7zsstUQwUDlYpUTvjeUsJAssPgLn7QzbboPtL5ro04Mq0rPOsMzl7q5hIbRs2wD1A==",
    "heading": "fast-check@3.23.2"
  },
  {
    "name": "pure-rand",
    "version": "6.1.0",
    "integrity": "sha512-bVWawvoZoBYpp6yIoQtQXHZjmz35RSVHnUOTefl8Vcjr8snTPY1wnpSPMWekcFwbxI6gtmT7rSYPFvz71ldiOA==",
    "heading": "pure-rand@6.1.0"
  }
];
export const IMPORTERS: readonly PersistenceImporter[] = [
  { name: 'packages/session-runtime-sql', section: 'dependencies', dependency: 'effect', specifier: '3.22.1', version: '3.22.1' },
  { name: 'packages/sql-client', section: 'devDependencies', dependency: 'postgres', specifier: '^3.4.0', version: '3.4.9' },
];
export const MANIFEST_SHA256 = "06cccd3be83fc2d573b71bec13ee9284f2a2453afaeb2ced18cdc921e0916a01";
export const LOCK_SHA256 = "00ded76a481ca57dc7a5890271650c4e93dca16265ed86107ee969a984f97d42";
