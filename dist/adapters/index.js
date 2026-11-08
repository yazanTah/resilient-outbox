"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/adapters/index.ts
var adapters_exports = {};
__export(adapters_exports, {
  GenericStorageAdapter: () => GenericStorageAdapter,
  MemoryStorageAdapter: () => MemoryStorageAdapter
});
module.exports = __toCommonJS(adapters_exports);
var MemoryStorageAdapter = class {
  storage = /* @__PURE__ */ new Map();
  async getItem(key) {
    return this.storage.get(key) ?? null;
  }
  async setItem(key, value) {
    this.storage.set(key, value);
  }
  async removeItem(key) {
    this.storage.delete(key);
  }
  clear() {
    this.storage.clear();
  }
};
var GenericStorageAdapter = class {
  constructor(client) {
    this.client = client;
  }
  client;
  async getItem(key) {
    const val = await this.client.getItem(key);
    return val ?? null;
  }
  async setItem(key, value) {
    await this.client.setItem(key, value);
  }
  async removeItem(key) {
    await this.client.removeItem(key);
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  GenericStorageAdapter,
  MemoryStorageAdapter
});
//# sourceMappingURL=index.js.map