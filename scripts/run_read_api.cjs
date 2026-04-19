#!/usr/bin/env node
/**
 * npm runs scripts via cmd.exe on Windows; Ctrl+C can leave python.exe alive and the port stuck.
 * This wrapper keeps a direct parent/child link and forwards SIGINT/SIGTERM to the venv Python.
 */
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const win = process.platform === "win32";
const py = win
  ? path.join(root, ".venv", "Scripts", "python.exe")
  : path.join(root, ".venv", "bin", "python");

if (!fs.existsSync(py)) {
  console.error(`run_read_api: missing interpreter at ${py}`);
  process.exit(1);
}

const child = spawn(py, ["-m", "app.internal_read_v1"], {
  cwd: root,
  stdio: "inherit",
  env: process.env,
  shell: false,
});

let exiting = false;

function shutdown() {
  if (exiting || child.killed) return;
  exiting = true;
  if (win) {
    child.kill();
  } else {
    child.kill("SIGINT");
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

child.on("exit", (code, signal) => {
  if (signal) {
    process.exit(1);
  }
  process.exit(typeof code === "number" ? code : 1);
});

child.on("error", (err) => {
  console.error(err);
  process.exit(1);
});
