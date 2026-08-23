/* ============================================================================
 * ProjectStorage —— 编辑器项目的 IndexedDB 持久化存储层
 *
 * 设计原则：
 *  1. 编辑器 UI 不直接操作 IndexedDB，只与 ProjectStorage 打交道：
 *        Editor  →  ProjectStorage  →  IndexedDB
 *  2. 将来即使更换存储后端，编辑器本身无需重构。
 *  3. 保存、读取、迁移旧数据、判定是否存在等项目都封装在此层。
 *  4. 所有写操作通过内部串行队列执行，保证“连续快速编辑时最终写入的
 *     一定是最新状态”，避免异步 save 竞态造成旧数据覆盖新数据。
 *  5. 【关键】保存必须等到 IndexedDB transaction 真正 oncomplete 才算成功，
 *     而不是仅仅在 db.put() 的 onsuccess 就判定成功——否则页面被关闭/回收时
 *     transaction 可能被回滚，数据没真正落盘。
 *
 * 数据库：daylightNightTour
 * object store：projects（keyPath: id）
 * 记录结构：
 *   { id: 'current', version: 1, updatedAt: <timestamp>, data: <项目数据模型> }
 * 其中 data 与 JSON 导入/导出使用的是同一套数据模型（见 collectDraftData()）。
 * ========================================================================== */
(function (global) {
  'use strict';

  var DB_NAME = 'daylightNightTour';
  var STORE_NAME = 'projects';
  var DB_VERSION = 1;
  var CURRENT_PROJECT_ID = 'current';

  function ProjectStorage(opts) {
    opts = opts || {};
    this.dbName = opts.dbName || DB_NAME;
    this.storeName = opts.storeName || STORE_NAME;
    this.dbVersion = opts.dbVersion || DB_VERSION;
    this.projectId = opts.projectId || CURRENT_PROJECT_ID;
    this._db = null;
    this._initPromise = null;
    // 写队列：把每一次 save 串行化，保证最终写到 IndexedDB 的一定是最新一次的数据。
    this._writeQueue = Promise.resolve();
  }

  /* ---- 初始化（幂等） ---- */
  ProjectStorage.prototype.init = function () {
    var self = this;
    if (self._db) return Promise.resolve(self._db);
    if (self._initPromise) return self._initPromise;

    if (!global.indexedDB) {
      self._initPromise = Promise.reject(new Error('IndexedDB 不可用'));
      return self._initPromise;
    }

    self._initPromise = new Promise(function (resolve, reject) {
      var request;
      try {
        request = global.indexedDB.open(self.dbName, self.dbVersion);
      } catch (e) {
        console.error('[ProjectStorage] 打开数据库异常', e);
        reject(e);
        return;
      }

      request.onupgradeneeded = function (ev) {
        var db = ev.target.result;
        if (!db.objectStoreNames.contains(self.storeName)) {
          var store = db.createObjectStore(self.storeName, { keyPath: 'id' });
          store.createIndex('by_updatedAt', 'updatedAt', { unique: false });
          store.createIndex('by_version', 'version', { unique: false });
        }
      };

      request.onsuccess = function () {
        self._db = request.result;
        self._db.onversionchange = function () {
          try { self._db.close(); } catch (e) {}
          self._db = null;
        };
        console.info('[ProjectStorage] IndexedDB initialized', {
          db: self.dbName, store: self.storeName, version: self.dbVersion,
        });
        resolve(self._db);
      };

      request.onerror = function () {
        var err = request.error || new Error('IndexedDB open failed');
        console.error('[ProjectStorage] 打开数据库失败', err);
        reject(err);
      };

      request.onblocked = function () {
        console.error('[ProjectStorage] 数据库升级被其他标签页阻塞');
      };
    });

    return self._initPromise;
  };

  /* ---- 内部：拿当前 object store ---- */
  ProjectStorage.prototype._store = function (mode) {
    return this._db.transaction(this.storeName, mode).objectStore(this.storeName);
  };

  /* ---- 内部：计算对象大小（字节） ---- */
  ProjectStorage.prototype._bytes = function (obj) {
    try {
      return new Blob([JSON.stringify(obj)]).size;
    } catch (e) {
      return JSON.stringify(obj || '').length;
    }
  };

  /* ---- 保存项目（data 为与 JSON 导出同构的项目数据模型） ---- */
  ProjectStorage.prototype.save = function (data) {
    var self = this;
    var run = self._writeQueue.then(function () { return self._doSave(data); });
    self._writeQueue = run.then(function () {}, function () {});
    return run;
  };

  ProjectStorage.prototype._doSave = function (data) {
    var self = this;
    return self.init().then(function () {
      var now = Date.now();
      var record = {
        id: self.projectId,
        version: (data && data.version ? data.version : 1) || 1,
        updatedAt: now,
        data: data || {},
      };
      console.info('[ProjectStorage] save started', {
        id: record.id, version: record.version, updatedAt: record.updatedAt,
      });

      return new Promise(function (resolve, reject) {
        var tx, store, request;
        try {
          tx = self._db.transaction(self.storeName, 'readwrite');
          store = tx.objectStore(self.storeName);
          request = store.put(record);
        } catch (e) {
          console.error('[ProjectStorage] 保存异常（事务创建失败）', e);
          reject(e);
          return;
        }

        request.onsuccess = function () {
          // put 已被接受，但事务尚未提交——真正成功与否看 transaction.oncomplete
        };
        request.onerror = function (ev) {
          var err = ev.target.error || request.error || new Error('put failed');
          console.error('[ProjectStorage] put 失败', err);
        };

        tx.oncomplete = function () {
          console.info('[ProjectStorage] 保存成功（transaction complete）', {
            id: record.id, updatedAt: record.updatedAt,
          });
          resolve(record);
        };
        tx.onerror = function (ev) {
          var err = ev.target.error || tx.error || request.error || new Error('transaction error');
          console.error('[ProjectStorage] 保存失败（transaction error）', err);
          reject(err);
        };
        tx.onabort = function (ev) {
          var err = tx.error || new Error('transaction aborted');
          console.error('[ProjectStorage] 保存失败（transaction aborted）', err);
          reject(err);
        };
      });
    });
  };

  /* ---- 读取当前项目；不存在时返回 null ---- */
  ProjectStorage.prototype.load = function () {
    var self = this;
    return self.init().then(function () {
      return new Promise(function (resolve, reject) {
        var tx, store, request;
        try {
          tx = self._db.transaction(self.storeName, 'readonly');
          store = tx.objectStore(self.storeName);
          request = store.get(self.projectId);
        } catch (e) {
          console.error('[ProjectStorage] 读取异常（事务创建失败）', e);
          reject(e);
          return;
        }

        request.onsuccess = function () {
          var record = request.result || null;
          console.info('[ProjectStorage] project ' + (record ? 'found' : 'not found'));
          resolve(record);
        };
        request.onerror = function (ev) {
          var err = ev.target.error || request.error || new Error('get failed');
          console.error('[ProjectStorage] 读取失败', err);
          reject(err);
        };
        tx.onerror = function (ev) {
          var err = ev.target.error || tx.error || request.error || new Error('transaction error');
          console.error('[ProjectStorage] 读取失败（transaction error）', err);
          reject(err);
        };
        tx.onabort = function (ev) {
          var err = tx.error || new Error('transaction aborted');
          console.error('[ProjectStorage] 读取失败（transaction aborted）', err);
          reject(err);
        };
      });
    });
  };

  /* ---- 是否存在当前项目 ---- */
  ProjectStorage.prototype.hasProject = function () {
    var self = this;
    return self.load().then(function (record) {
      return !!(record && record.data);
    });
  };

  /* ---- 清除当前项目（诊断工具不调用；仅明确需要时用） ---- */
  ProjectStorage.prototype.clearProject = function () {
    var self = this;
    return self.init().then(function () {
      return new Promise(function (resolve, reject) {
        var tx, store, request;
        try {
          tx = self._db.transaction(self.storeName, 'readwrite');
          store = tx.objectStore(self.storeName);
          request = store.delete(self.projectId);
        } catch (e) {
          console.error('[ProjectStorage] 清除异常', e);
          reject(e);
          return;
        }
        request.onsuccess = function () {
          console.info('[ProjectStorage] clearProject completed', { id: self.projectId });
          resolve(true);
        };
        request.onerror = function () {
          var err = request.error || new Error('IndexedDB delete failed');
          console.error('[ProjectStorage] 清除失败', err);
          reject(err);
        };
      });
    });
  };

  /* ---- 读取当前项目的元信息（不含 data 本体） ---- */
  ProjectStorage.prototype.getMetadata = function () {
    var self = this;
    return self.load().then(function (record) {
      if (!record) return null;
      return { id: record.id, version: record.version, updatedAt: record.updatedAt };
    });
  };

  /* ---- 释放连接（极少使用） ---- */
  ProjectStorage.prototype.close = function () {
    if (this._db) {
      try { this._db.close(); } catch (e) {}
      this._db = null;
    }
    this._initPromise = null;
    this._writeQueue = Promise.resolve();
  };

  global.ProjectStorage = ProjectStorage;
})(typeof window !== 'undefined' ? window : globalThis);
