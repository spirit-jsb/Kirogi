//
//  VMKVStorage.swift
//  Kirogi
//
//  Created by Max on 2022/3/13.
//

#if canImport(UIKit) && canImport(QuartzCore) && canImport(CSQLite)

import UIKit
import QuartzCore
import CSQLite

internal enum VMKVStorageType: Int {
  case sqlite
  case file
  case mixed
}

internal class VMKVStorageItem: NSObject {
  
  var key: String?
  var value: Data?
  
  var extendedData: Data?
  
  var filename: String?
  
  var size: Int
  
  var lastModificationTimestamp: Int32
  var lastAccessTimestamp: Int32
  
  init(key: String?, value: Data?, extendedData: Data?, filename: String?, size: Int, lastModificationTimestamp: Int32, lastAccessTimestamp: Int32) {
    self.key = key
    self.value = value
    
    self.extendedData = extendedData
    
    self.filename = filename
    
    self.size = size
    
    self.lastModificationTimestamp = lastModificationTimestamp
    self.lastAccessTimestamp = lastAccessTimestamp
  }
  
  init(key: String?, filename: String?, size: Int) {
    self.key = key
    self.value = nil
    
    self.extendedData = nil
    
    self.filename = filename
    
    self.size = size
    
    self.lastModificationTimestamp = 0
    self.lastAccessTimestamp = 0
  }
}

internal class VMKVStorage: NSObject {
  
  private(set) var path: String
  
  private(set) var type: VMKVStorageType
  
  var errorLogsEnabled: Bool
  
  private let _dbPath: String
  
  private let _dataPath: String
  
  private let _trashPath: String
  
  private let _trashQueue: DispatchQueue
  
  private let _dbFilename = "kirogi.sqlite"
  private let _dbWALFilename = "kirogi.sqlite-wal"
  private let _dbWALIndexFilename = "kirogi.sqlite-shm"
  
  private let _dataDirname = "data"
  private let _trashDirname = "trash"
  
  private let _dbOpenRetryMaxCount: UInt = 8
  private let _dbOpenRetryTimeInterval: TimeInterval = 2.0
  
  private var _db: OpaquePointer?
  
  private var _dbStmtCache: [String: OpaquePointer]?
  
  private var _dbOpenFailCount: UInt = 0
  private var _dbOpenFailLastTime: TimeInterval = 0.0
  
  init?(path: String?, type: VMKVStorageType) {
    guard let path = path else {
      return nil
    }
    
    self.path = path
    
    self.type = type
    
    self.errorLogsEnabled = true
    
    let pathUrl = URL(fileURLWithPath: path)
    
    let dbPathUrl = pathUrl.appendingPathComponent(self._dbFilename)
    
    let dataPathUrl = pathUrl.appendingPathComponent(self._dataDirname, isDirectory: true)
    
    let trashPathUrl = pathUrl.appendingPathComponent(self._trashDirname, isDirectory: true)
    
    self._dbPath = dbPathUrl.absoluteString
    
    self._dataPath = dataPathUrl.absoluteString
    
    self._trashPath = trashPathUrl.absoluteString
    
    self._trashQueue = DispatchQueue(label: "com.max.jian.Kirogi.cache.disk.trash")
    
    do {
      try FileManager.default.createDirectory(at: pathUrl, withIntermediateDirectories: true, attributes: nil)
      
      try FileManager.default.createDirectory(at: dataPathUrl, withIntermediateDirectories: true, attributes: nil)
      
      try FileManager.default.createDirectory(at: trashPathUrl, withIntermediateDirectories: true, attributes: nil)
    }
    catch {
      return nil
    }
    
    super.init()
    
    if !self._dbOpen() || !self._dbInitialize() {
      // db file may broken...
      self._dbClose()
      
      // rebuild
      self._reset()
      
      if !self._dbOpen() || !self._dbInitialize() {
        self._dbClose()
        
        print("VMKVStorage init error: fail to open sqlite db.")
        
        return nil
      }
    }
    
    // empty the trash if failed at last time
    self._fileEmptyTrashInBackground()
  }
  
  deinit {
    self._dbClose()
  }
  
  func itemExists(forKey key: String) -> Bool {
    guard !key.isEmpty else {
      return false
    }
    
    return self._dbGetItemCount(withKey: key) > 0
  }
  
  @discardableResult
  func saveItem(_ item: VMKVStorageItem) -> Bool {
    return true
  }
  
  @discardableResult
  func saveItem(withKey key: String?, value: Data) -> Bool {
    return true
  }
  
  @discardableResult
  func saveItem(withKey key: String?, value: Data, filename: String?, extendedData: Data?) -> Bool {
    return true
  }
  
  func getItem(forKey key: String) -> VMKVStorageItem? {
    guard !key.isEmpty else {
      return nil
    }
    
    var item = self._dbGetItem(withKey: key, excludeInlineData: false)
    
    if item != nil {
      self._dbUpdateLastAccessTimestamp(withKey: key)
      
      if let filename = item?.filename {
        item!.value = try? self._fileRead(withName: filename)
        
        if item!.value == nil {
          self._dbDeleteItem(withKey: key)
          
          item = nil
        }
      }
    }
    
    return item
  }
  
  func getItemInfo(forKey key: String) -> VMKVStorageItem? {
    guard !key.isEmpty else {
      return nil
    }
    
    return self._dbGetItem(withKey: key, excludeInlineData: true)
  }
  
  func getItemValue(forKey key: String) -> Data? {
    guard !key.isEmpty else {
      return nil
    }
    
    var itemValue: Data?
    
    switch self.type {
      case .sqlite:
        itemValue = self._dbGetValue(forKey: key)
      case .file:
        if let filename = self._dbGetFilename(forKey: key) {
          itemValue = try? self._fileRead(withName: filename)
          
          if itemValue == nil {
            self._dbDeleteItem(withKey: key)
            itemValue = nil
          }
        }
      case .mixed:
        if let filename = self._dbGetFilename(forKey: key) {
          itemValue = try? self._fileRead(withName: filename)
          
          if itemValue == nil {
            self._dbDeleteItem(withKey: key)
            itemValue = nil
          }
        }
        else {
          itemValue = self._dbGetValue(forKey: key)
        }
    }
    
    if itemValue != nil {
      self._dbUpdateLastAccessTimestamp(withKey: key)
    }
    
    return itemValue
  }
  
  func getItems(forKeys keys: [String]) -> [VMKVStorageItem]? {
    guard !keys.isEmpty else {
      return nil
    }
    
    var items = self._dbGetItems(withKeys: keys, excludeInlineData: false) ?? []
    
    if self.type != .sqlite {
      items.enumerated().forEach { (indices, _) in
        if let filename = items[indices].filename {
          items[indices].value = try? self._fileRead(withName: filename)
          
          if items[indices].value == nil {
            if let key = items[indices].key {
              self._dbDeleteItem(withKey: key)
            }
            
            items.remove(at: indices)
          }
        }
      }
    }
    
    if !items.isEmpty {
      self._dbUpdateLastAccessTimestamp(withKeys: keys)
    }
    
    return items
  }
  
  func getItemInfos(forKeys keys: [String]) -> [VMKVStorageItem]? {
    guard !keys.isEmpty else {
      return nil
    }
    
    return self._dbGetItems(withKeys: keys, excludeInlineData: true)
  }
  
  func getItemValues(forKeys keys: [String]) -> [String: Data]? {
    let items = self.getItems(forKeys: keys)
    
    let kv = items?.reduce(into: [String: Data]()) {
      if let key = $1.key, let value = $1.value {
        $0[key] = value
      }
    }
    
    return kv
  }
  
  @discardableResult
  func removeItem(forKey key: String) -> Bool {
    guard !key.isEmpty else {
      return false
    }
    
    switch self.type {
      case .file:
        fallthrough
      case .mixed:
        if let filename = self._dbGetFilename(forKey: key) {
          try? self._fileDelete(withName: filename)
        }
      default:
        break
    }
    
    let result = self._dbDeleteItem(withKey: key)
    
    return result
  }
  
  @discardableResult
  func removeItems(forKeys keys: [String]) -> Bool {
    guard !keys.isEmpty else {
      return false
    }
    
    switch self.type {
      case .file:
        fallthrough
      case .mixed:
        if let filenames = self._dbGetFilenames(forKeys: keys) {
          filenames.forEach {
            try? self._fileDelete(withName: $0)
          }
        }
      default:
        break
    }
    
    let result = self._dbDeleteItems(withKeys: keys)
    
    return result
  }
  
  @discardableResult
  func removeItemsLargerThanSize(_ size: Int) -> Bool {
    guard size != .max else {
      return true
    }
    
    var result: Bool = false
    
    if size <= 0 {
      result = self.removeAllItems()
    }
    else {
      switch self.type {
        case .file:
          fallthrough
        case .mixed:
          if let filenames = self._dbGetFilenamesLargerThanSize(size) {
            filenames.forEach {
              try? self._fileDelete(withName: $0)
            }
          }
        default:
          break
      }
      
      if self._dbDeleteItemsLargerThanSize(size) {
        self._dbCheckpoint()
        
        result = true
      }
    }
    
    return result
  }
  
  @discardableResult
  func removeItemsEarlierThanTime(_ time: Int32) -> Bool {
    guard time > 0 else {
      return true
    }
    
    var result: Bool = false
    
    if time == .max {
      result = self.removeAllItems()
    }
    else {
      switch self.type {
        case .file:
          fallthrough
        case .mixed:
          if let filenames = self._dbGetFilenamesEarlierThanTime(time) {
            filenames.forEach {
              try? self._fileDelete(withName: $0)
            }
          }
        default:
          break
      }
      
      if self._dbDeleteItemsEarlierThanTime(time) {
        self._dbCheckpoint()
        
        result = true
      }
    }
    
    return result
  }
  
  @discardableResult
  func removeItemsToFixSize(_ maxSize: Int) -> Bool {
    guard maxSize != .max else {
      return true
    }
    
    guard maxSize > 0 else {
      return self.removeAllItems()
    }
    
    var totalItemSize = self._dbGetTotalItemSize()
    
    guard totalItemSize >= 0 else {
      return false
    }
    
    guard totalItemSize > maxSize else {
      return true
    }
    
    var result: Bool = false
    
    var deletableItems: [VMKVStorageItem]!
    
    repeat {
      deletableItems = self._dbGetItemSizeInfosOrderByAccessTime(withLimit: 16) ?? []
      
      for deletableItem in deletableItems {
        if totalItemSize > maxSize {
          if let filename = deletableItem.filename {
            try? self._fileDelete(withName: filename)
          }
          
          result = self._dbDeleteItem(withKey: deletableItem.key)
          totalItemSize -= deletableItem.size
        }
        else {
          break
        }
        
        if !result {
          break
        }
      }
    } while totalItemSize > maxSize && deletableItems.count > 0 && result
    
    if result {
      self._dbCheckpoint()
    }
    
    return result
  }
  
  @discardableResult
  func removeItemsToFitCount(_ maxCount: Int) -> Bool {
    guard maxCount != .max else {
      return true
    }
    
    guard maxCount > 0 else {
      return self.removeAllItems()
    }
    
    var totalItemCount = self._dbGetTotalItemCount()
    
    guard totalItemCount >= 0 else {
      return false
    }
    
    guard totalItemCount > maxCount else {
      return true
    }
    
    var result: Bool = false
    
    var deletableItems: [VMKVStorageItem]!
    
    repeat {
      deletableItems = self._dbGetItemSizeInfosOrderByAccessTime(withLimit: 16) ?? []
      
      for deletableItem in deletableItems {
        if totalItemCount > maxCount {
          if let filename = deletableItem.filename {
            try? self._fileDelete(withName: filename)
          }
          
          result = self._dbDeleteItem(withKey: deletableItem.key)
          totalItemCount -= 1
        }
        else {
          break
        }
        
        if !result {
          break
        }
      }
    } while totalItemCount > maxCount && deletableItems.count > 0 && result
    
    if result {
      self._dbCheckpoint()
    }
    
    return result
  }
  
  @discardableResult
  func removeAllItems() -> Bool {
    guard self._dbClose() else {
      return false
    }
    
    self._reset()
    
    guard self._dbOpen() else {
      return false
    }
    
    guard self._dbInitialize() else {
      return false
    }
    
    return true
  }
  
  func removeAllItems(_ progress: ((Int, Int) -> Void)?, completion: ((Bool) -> Void)?) {
    let totalItemCount = self._dbGetTotalItemCount()
    
    guard totalItemCount > 0 else {
      completion?(totalItemCount < 0)
      
      return
    }
    
    var left = totalItemCount
    
    var result: Bool = false
    
    var deletableItems: [VMKVStorageItem]!
    
    repeat {
      deletableItems = self._dbGetItemSizeInfosOrderByAccessTime(withLimit: 32) ?? []
      
      for deletableItem in deletableItems {
        if left > 0 {
          if let filename = deletableItem.filename {
            try? self._fileDelete(withName: filename)
          }
          
          result = self._dbDeleteItem(withKey: deletableItem.key)
          left -= 1
        }
        else {
          break
        }
        
        if !result {
          break
        }
      }
      
      progress?(totalItemCount - left, totalItemCount)
    } while left > 0 && deletableItems.count > 0 && result
    
    if result {
      self._dbCheckpoint()
    }
    
    completion?(!result)
  }
  
  func itemsCount() -> Int {
    return self._dbGetTotalItemCount()
  }
  
  func itemsSize() -> Int {
    return self._dbGetTotalItemSize()
  }
}

extension VMKVStorage {
  
  // MARK: - private
  private func _reset() {
    let pathUrl = URL(fileURLWithPath: self.path)
    
    let dbPathUrl = pathUrl.appendingPathComponent(self._dbFilename)
    let dbWALPathUrl = pathUrl.appendingPathComponent(self._dbWALFilename)
    let dbWALIndexPathUrl = pathUrl.appendingPathComponent(self._dbWALIndexFilename)
    
    try? FileManager.default.removeItem(at: dbPathUrl)
    try? FileManager.default.removeItem(at: dbWALPathUrl)
    try? FileManager.default.removeItem(at: dbWALIndexPathUrl)
    
    try? self._fileMoveAllToTrash()
    self._fileEmptyTrashInBackground()
  }
}

extension VMKVStorage {
  
  // MARK: - database
  @discardableResult
  private func _dbOpen() -> Bool {
    guard self._db == nil else {
      return true
    }
    
    let openCode = sqlite3_open(self._dbPath, &self._db)
    if openCode == SQLITE_OK {
      self._dbStmtCache = [String: OpaquePointer]()
      
      self._dbOpenFailCount = 0
      self._dbOpenFailLastTime = 0.0
    }
    else {
      self._db = nil
      
      if self._dbStmtCache != nil {
        self._dbStmtCache!.removeAll()
      }
      self._dbStmtCache = nil
      
      self._dbOpenFailCount += 1
      self._dbOpenFailLastTime = CACurrentMediaTime()
      
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite open failed (\(openCode)).")
      }
    }
    
    return openCode == SQLITE_OK
  }
  
  @discardableResult
  private func _dbInitialize() -> Bool {
    let sql = """
      pragma journal_mode = wal;
      
      pragma synchronous = normal;
      
      create table if not exists kirogi (
        key text,
        inline_data blob,
        extended_data blob,
        filename text,
        size integer,
        last_modification_timestamp integer,
        last_access_timestamp integer,
        primary key(key)
      );
      
      create index if not exists last_access_time_idx on manifest(last_access_time);
      """
    
    return self._dbExecute(sql)
  }
  
  @discardableResult
  private func _dbClose() -> Bool {
    guard self._db != nil else {
      return true
    }
    
    var needsRetry = false
    var stmtFinalized = false
    
    var closeCode: Int32 = 0
    
    if self._dbStmtCache != nil {
      self._dbStmtCache!.removeAll()
    }
    self._dbStmtCache = nil
    
    repeat {
      needsRetry = false
      
      closeCode = sqlite3_close(self._db!)
      if closeCode == SQLITE_BUSY || closeCode == SQLITE_LOCKED {
        if !stmtFinalized {
          stmtFinalized = true
          
          while let stmt = sqlite3_next_stmt(self._db!, nil) {
            sqlite3_finalize(stmt)
            needsRetry = true
          }
        }
      }
      else if closeCode != SQLITE_OK {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite close failed (\(closeCode)).")
        }
      }
    } while needsRetry
    
    self._db = nil
    
    return true
  }
  
  @discardableResult
  private func _dbCheck() -> Bool {
    guard self._db == nil else {
      return true
    }
    
    let effectiveRetryCount = self._dbOpenFailCount < self._dbOpenRetryMaxCount
    let effectiveRetryTimeInterval = CACurrentMediaTime() - self._dbOpenFailLastTime > self._dbOpenRetryTimeInterval
    
    return effectiveRetryCount && effectiveRetryTimeInterval ? self._dbOpen() && self._dbInitialize() : false
  }
  
  private func _dbCheckpoint() {
    guard self._dbCheck() else {
      return
    }
    
    // Cause a checkpoint to occur, merge `sqlite-wal` file to `sqlite` file.
    sqlite3_wal_checkpoint(self._db!, nil)
  }
  
  @discardableResult
  private func _dbExecute(_ sql: String) -> Bool {
    guard !sql.isEmpty && self._dbCheck() else {
      return false
    }
    
    var execError: UnsafeMutablePointer<Int8>?
    
    let execCode = sqlite3_exec(self._db!, sql, nil, nil, &execError)
    
    if let execError = execError {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite exec error (\(execCode)): \(String(cString: execError))")
      }
      
      sqlite3_free(execError)
    }
    
    return execCode == SQLITE_OK
  }
  
  private func _dbPrepareStmt(_ sql: String) -> OpaquePointer? {
    guard self._dbCheck() && !sql.isEmpty && self._dbStmtCache != nil else {
      return nil
    }
    
    var stmt = self._dbStmtCache![sql]
    if stmt != nil {
      let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
      guard prepareCode == SQLITE_OK else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
        }
        
        return nil
      }
      
      self._dbStmtCache![sql] = stmt!
    }
    else {
      sqlite3_reset(stmt)
    }
    
    return stmt
  }
  
  private func _dbJoinedKeys(_ keys: [String]) -> String {
    return keys.map { _ in "?" }.joined(separator: ",")
  }
  
  private func _dbBindJoinedKeys(_ keys: [String], stmt: OpaquePointer, fromIndex index: Int) {
    keys.enumerated().forEach { (indices, element) in
      sqlite3_bind_text(stmt, Int32(index + indices), element, -1, nil)
    }
  }
  
  @discardableResult
  private func _dbSaveItem(withKey key: String, value: Data, extendedData: Data?, filename: String?) -> Bool {
    guard !key.isEmpty else {
      return false
    }
    
    let sql = """
      insert
        or replace into kirogi (
          key,
          inline_data,
          extended_data,
          filename,
          size,
          last_modification_timestamp,
          last_access_timestamp,
        )
      values
        (?1, ?2, ?3, ?4, ?5, ?6, ?7);
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return false
    }
    
    let timestamp = Int32(Date().timeIntervalSince1970)
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    if !filename.isEmpty {
      sqlite3_bind_blob(stmt!, 2, [UInt8](value), Int32([UInt8](value).count), nil)
    }
    else {
      sqlite3_bind_blob(stmt!, 2, nil, 0, nil)
    }
    
    sqlite3_bind_blob(stmt!, 3, [UInt8](extendedData), Int32([UInt8](extendedData).count), nil)
    
    sqlite3_bind_text(stmt!, 4, filename, -1, nil)
    
    sqlite3_bind_int(stmt!, 5, Int32([UInt8](value).count))
    
    sqlite3_bind_int(stmt!, 6, timestamp)
    sqlite3_bind_int(stmt!, 7, timestamp)
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite insert error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
  
  private func _dbGetItem(withKey key: String?, excludeInlineData: Bool) -> VMKVStorageItem? {
    guard let key = key, !key.isEmpty else {
      return nil
    }
    
    let sql = excludeInlineData ?
      """
      select
        key,
        extended_data,
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp,
      from
        kirogi
      where
        key = ?1;
      """
    :
      """
      select
        key,
        inline_data,
        extended_data,
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp,
      from
        kirogi
      where
        key = ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return nil
    }
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    var item: VMKVStorageItem?
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode == SQLITE_ROW {
      item = self._dbGetItem(fromStmt: stmt!, excludeInlineData: excludeInlineData)
    }
    else if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return item
  }
  
  private func _dbGetItemCount(withKey key: String?) -> Int {
    guard let key = key, !key.isEmpty else {
      return -1
    }
    
    let sql = """
      select
        count(key)
      from
        kirogi
      where
        key = ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return -1
    }
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    var itemCount = -1
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode == SQLITE_ROW {
      itemCount = Int(sqlite3_column_int(stmt!, 0))
    }
    else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return itemCount
  }
  
  private func _dbGetItems(withKeys keys: [String]?, excludeInlineData: Bool) -> [VMKVStorageItem]? {
    guard let keys = keys, !keys.isEmpty else {
      return nil
    }

    guard self._dbCheck() else {
      return nil
    }
    
    let sql = excludeInlineData ?
      """
      select
        key,
        extended_data,
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp,
      from
        kirogi
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    :
      """
      select
        key,
        inline_data,
        extended_data,
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp,
      from
        kirogi
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    
    var stmt: OpaquePointer? = nil
    let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
    guard prepareCode == SQLITE_OK else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
      
      return nil
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    var stepCode: Int32 = 0
    
    var items: [VMKVStorageItem]? = []
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        if let item = self._dbGetItem(fromStmt: stmt!, excludeInlineData: excludeInlineData) {
          items?.append(item)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
        }
        
        items = nil
        
        break
      }
      
    } while true
    
    sqlite3_finalize(stmt!)
    
    return items
  }
  
  private func _dbGetItem(fromStmt stmt: OpaquePointer, excludeInlineData: Bool) -> VMKVStorageItem? {
    var iCol: Int32 = 0
    
    let key = sqlite3_column_text(stmt, iCol++).flatMap { String(cString: $0) }
    
    let inlineDataLength = excludeInlineData ? 0 : Int(sqlite3_column_bytes(stmt, iCol))
    let inlineData = excludeInlineData ? nil : sqlite3_column_blob(stmt, iCol++).flatMap { inlineDataLength > 0 ? Data(bytes: $0, count: inlineDataLength) : nil }
    
    let extendedDataLength = Int(sqlite3_column_bytes(stmt, iCol))
    let extendedData = sqlite3_column_blob(stmt, iCol++).flatMap { extendedDataLength > 0 ? Data(bytes: $0, count: extendedDataLength) : nil }
    
    let filename = sqlite3_column_text(stmt, iCol++).flatMap { $0.pointee != 0 ? String(cString: $0) : nil }
    
    let size = Int(sqlite3_column_int(stmt, iCol++))
    
    let lastModificationTimestamp = sqlite3_column_int(stmt, iCol++)
    let lastAccessTimestamp = sqlite3_column_int(stmt, iCol++)
    
    let item = VMKVStorageItem(key: key, value: inlineData, extendedData: extendedData, filename: filename, size: size, lastModificationTimestamp: lastModificationTimestamp, lastAccessTimestamp: lastAccessTimestamp)
    
    return item
  }
  
  private func _dbGetItemSizeInfosOrderByAccessTime(withLimit limit: Int32) -> [VMKVStorageItem]? {
    let sql = """
      select
        key,
        filename,
        size
      from
        kirogi
      order by
        last_access_timestamp asc
      limit
        ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return nil
    }
    
    sqlite3_bind_int(stmt!, 1, limit)
    
    var stepCode: Int32 = 0
    
    var itemSizeInfos: [VMKVStorageItem]? = []
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        let key = sqlite3_column_text(stmt, 0).flatMap { String(cString: $0) }
        
        let filename = sqlite3_column_text(stmt, 1).flatMap { $0.pointee != 0 ? String(cString: $0) : nil }
        
        let size = Int(sqlite3_column_int(stmt, 2))
        
        if key != nil {
          let itemSizeInfo = VMKVStorageItem(key: key, filename: filename, size: size)
          
          itemSizeInfos?.append(itemSizeInfo)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
        }
        
        itemSizeInfos = nil
        
        break
      }
      
    } while true
    
    return itemSizeInfos
  }
  
  private func _dbGetValue(forKey key: String) -> Data? {
    let sql = """
      select
        inline_data
      from
        kirogi
      where
        key = ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return nil
    }
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    var value: Data?
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode == SQLITE_ROW {
      let inlineDataLength = Int(sqlite3_column_bytes(stmt!, 0))
      
      value = sqlite3_column_blob(stmt, 0).flatMap { inlineDataLength > 0 ? Data(bytes: $0, count: inlineDataLength) : nil }
    }
    else if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return value
  }
  
  private func _dbGetFilename(forKey key: String) -> String? {
    let sql = """
      select
        filename
      from
        kirogi
      where
        key = ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return nil
    }
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    var filename: String?
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode == SQLITE_ROW {
      filename = sqlite3_column_text(stmt!, 0).flatMap { $0.pointee != 0 ? String(cString: $0) : nil }
    }
    else if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return filename
  }
  
  private func _dbGetFilenames(forKeys keys: [String]) -> [String]? {
    guard self._dbCheck() else {
      return nil
    }
    
    let sql = """
      select
        filename
      from
        kirogi
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    
    var stmt: OpaquePointer? = nil
    let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
    guard prepareCode == SQLITE_OK else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
      
      return nil
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    var stepCode: Int32 = 0
    
    var filenames: [String]? = []
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        if let filename = sqlite3_column_text(stmt!, 0).flatMap({ $0.pointee != 0 ? String(cString: $0) : nil }) {
          filenames?.append(filename)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
        }
        
        filenames = nil
        
        break
      }
      
    } while true
    
    sqlite3_finalize(stmt!)
    
    return filenames
  }
  
  private func _dbGetFilenamesLargerThanSize(_ size: Int) -> [String]? {
    let sql = """
      select
        filename
      from
        kirogi
      where
        size > ?1
        and filename is not null;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return nil
    }
    
    sqlite3_bind_int(stmt!, 1, Int32(size))
    
    var stepCode: Int32 = 0
    
    var filenames: [String]? = []
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        if let filename = sqlite3_column_text(stmt!, 0).flatMap({ $0.pointee != 0 ? String(cString: $0) : nil }) {
          filenames?.append(filename)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
        }
        
        filenames = nil
        
        break
      }
      
    } while true
    
    return filenames
  }
  
  private func _dbGetFilenamesEarlierThanTime(_ time: Int32) -> [String]? {
    let sql = """
      select
        filename
      from
        kirogi
      where
        last_access_timestamp < ?1
        and filename is not null;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return nil
    }
    
    sqlite3_bind_int(stmt!, 1, time)
    
    var stepCode: Int32 = 0
    
    var filenames: [String]? = []
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        if let filename = sqlite3_column_text(stmt!, 0).flatMap({ $0.pointee != 0 ? String(cString: $0) : nil }) {
          filenames?.append(filename)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
        }
        
        filenames = nil
        
        break
      }
      
    } while true
    
    return filenames
  }
  
  private func _dbGetTotalItemCount() -> Int {
    let sql = """
      select
        count(*)
      from
        kirogi;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return -1
    }
    
    var totalItemCount = -1
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode == SQLITE_ROW {
      totalItemCount = Int(sqlite3_column_int(stmt!, 0))
    }
    else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return totalItemCount
  }
  
  private func _dbGetTotalItemSize() -> Int {
    let sql = """
      select
        sum(size)
      from
        kirogi;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return -1
    }
    
    var totalItemSize = -1
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode == SQLITE_ROW {
      totalItemSize = Int(sqlite3_column_int(stmt!, 0))
    }
    else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return totalItemSize
  }
  
  @discardableResult
  private func _dbDeleteItem(withKey key: String?) -> Bool {
    guard let key = key, !key.isEmpty else {
      return false
    }
    
    let sql = """
      delete from
        kirogi
      where
        key = ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return false
    }
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
  
  @discardableResult
  private func _dbDeleteItems(withKeys keys: [String]?) -> Bool {
    guard let keys = keys, !keys.isEmpty else {
      return false
    }
    
    guard self._dbCheck() else {
      return false
    }
    
    let sql = """
      delete from
        kirogi
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    
    var stmt: OpaquePointer? = nil
    let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
    guard prepareCode == SQLITE_OK else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
      
      return false
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    let stepCode = sqlite3_step(stmt!)
    
    sqlite3_finalize(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
  
  @discardableResult
  private func _dbDeleteItemsLargerThanSize(_ size: Int) -> Bool {
    let sql = """
      delete from
        kirogi
      where
        size > ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return false
    }
    
    sqlite3_bind_int(stmt!, 1, Int32(size))
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
  
  @discardableResult
  private func _dbDeleteItemsEarlierThanTime(_ time: Int32) -> Bool {
    let sql = """
      delete from
        kirogi
      where
        last_access_timestamp < ?1;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return false
    }
    
    sqlite3_bind_int(stmt!, 1, time)
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
  
  @discardableResult
  private func _dbUpdateLastAccessTimestamp(withKey key: String?) -> Bool {
    guard let key = key, !key.isEmpty else {
      return false
    }
    
    let sql = """
      update
        kirogi
      set
        last_access_timestamp = ?1
      where
        key = ?2;
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return false
    }
    
    sqlite3_bind_int(stmt!, 1, Int32(Date().timeIntervalSince1970))
    
    sqlite3_bind_text(stmt!, 2, key, -1, nil)
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
  
  @discardableResult
  private func _dbUpdateLastAccessTimestamp(withKeys keys: [String]?) -> Bool {
    guard let keys = keys, !keys.isEmpty else {
      return false
    }
    
    guard self._dbCheck() else {
      return false
    }
    
    let timestamp = Int32(Date().timeIntervalSince1970)
    
    let sql = """
      update
        kirogi
      set
        last_access_timestamp = \(timestamp)
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    
    var stmt: OpaquePointer? = nil
    let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
    guard prepareCode == SQLITE_OK else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
      
      return false
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    let stepCode = sqlite3_step(stmt!)
    
    sqlite3_finalize(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(describing: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
}

extension VMKVStorage {
  
  // MARK: - file
  private func _fileWrite(withName filename: String, data: Data) throws {
    let fileDataUrl = URL(fileURLWithPath: self._dataPath).appendingPathComponent(filename)
    
    try data.write(to: fileDataUrl, options: [])
  }
  
  private func _fileRead(withName filename: String) throws -> Data {
    let fileDataUrl = URL(fileURLWithPath: self._dataPath).appendingPathComponent(filename)
    
    return try Data(contentsOf: fileDataUrl, options: [])
  }
  
  private func _fileDelete(withName filename: String) throws {
    let fileDataUrl = URL(fileURLWithPath: self._dataPath).appendingPathComponent(filename)
    
    try FileManager.default.removeItem(at: fileDataUrl)
  }
  
  private func _fileMoveAllToTrash() throws {
    let uuidString = UUID().uuidString
    
    let dataUrl = URL(fileURLWithPath: self._dataPath)
    let tmpTrashUrl = URL(fileURLWithPath: self._trashPath).appendingPathComponent(uuidString)
    
    try FileManager.default.moveItem(at: dataUrl, to: tmpTrashUrl)
    try FileManager.default.createDirectory(at: dataUrl, withIntermediateDirectories: true, attributes: nil)
  }
  
  private func _fileEmptyTrashInBackground() {
    let trashUrl = URL(fileURLWithPath: self._trashPath)
    
    self._trashQueue.async {
      let fileManager = FileManager.default
      
      let directoryContents = try? fileManager.contentsOfDirectory(atPath: trashUrl.absoluteString)
      (directoryContents ?? []).forEach {
        let contentTrashUrl = trashUrl.appendingPathComponent($0)
        
        try? fileManager.removeItem(at: contentTrashUrl)
      }
    }
  }
}

postfix operator ++
fileprivate postfix func ++ (left: inout Int32) -> Int32 {
  defer {
    left += 1
  }
  return left
}

#endif
