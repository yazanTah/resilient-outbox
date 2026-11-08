// src/adapters/index.ts
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
export {
  GenericStorageAdapter,
  MemoryStorageAdapter
};
//# sourceMappingURL=index.mjs.map