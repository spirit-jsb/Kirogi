//
//  VMKVStorage.swift
//  Kirogi
//
//  Created by Max on 2022/3/13.
//

#if canImport(Foundation) && canImport(UIKit) && canImport(CSQLite)

import Foundation
import UIKit
import CSQLite

private func _VMSharedApplication() -> UIApplication? {
  let UIApplicationClass: AnyClass? = NSClassFromString("UIApplication")
  
  guard UIApplicationClass != nil && UIApplicationClass!.responds(to: Selector(("sharedApplication"))) else {
    return nil
  }
  
  let application = UIApplication.perform(Selector(("sharedApplication"))).takeUnretainedValue() as? UIApplication
  
  return application
}

internal enum VMKVStorageType: Int {
  case sqlite
  case file
  case mixed
}

internal class VMKVStorageItem: NSObject {
  
  var key: String?
  var value: Data?
    
  var filename: String?
  
  var size: Int
  
  var lastModificationTimestamp: Int32
  var lastAccessTimestamp: Int32
  
  init(key: String?, value: Data?, filename: String?, size: Int, lastModificationTimestamp: Int32, lastAccessTimestamp: Int32) {
    self.key = key
    self.value = value
        
    self.filename = filename
    
    self.size = size
    
    self.lastModificationTimestamp = lastModificationTimestamp
    self.lastAccessTimestamp = lastAccessTimestamp
  }
  
  init(key: String?, filename: String?, size: Int) {
    self.key = key
    self.value = nil
        
    self.filename = filename
    
    self.size = size
    
    self.lastModificationTimestamp = 0
    self.lastAccessTimestamp = 0
  }
}

/// File:
/// /Path/
///      /kirogi.sqlite
///      /kirogi.sqlite-wal
///      /kirogi.sqlite-shm
///      /data/
///           /5cb41d04007f8d688a11903ac16451d1
///      /trash/
///            /unused_file_or_folder
///
/// SQL:
/// pragma journal_mode = wal;
///
/// pragma synchronous = normal;
///
/// create table if not exists kirogi (
///   key text,
///   inline_data blob,
///   filename text,
///   size integer,
///   last_modification_timestamp integer,
///   last_access_timestamp integer,
///   primary key(key)
/// );
///
/// create index if not exists last_access_time_idx on kirogi(last_access_timestamp);

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
  
  static func initialize(path: String?, type: VMKVStorageType) -> VMKVStorage? {
    let kvStorage = VMKVStorage(path: path, type: type)
    
    return kvStorage
  }
  
  private init?(path: String?, type: VMKVStorageType) {
    guard let path = path, !path.isEmpty else {
      print("VMKVStorage init error: invalid path: [\(String(describing: path))].")
      
      return nil
    }
    
    self.path = path
    
    self.type = type
    
    self.errorLogsEnabled = true
    
    self._dbPath = path.stringByAppendingPathComponent(self._dbFilename)
    
    self._dataPath = path.stringByAppendingPathComponent(self._dataDirname)
    
    self._trashPath = path.stringByAppendingPathComponent(self._trashDirname)
    
    self._trashQueue = DispatchQueue(label: "com.max.jian.Kirogi.cache.disk.trash")
    
    do {
      try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true, attributes: nil)
      
      try FileManager.default.createDirectory(atPath: path.stringByAppendingPathComponent(self._dataDirname), withIntermediateDirectories: true, attributes: nil)
      
      try FileManager.default.createDirectory(atPath: path.stringByAppendingPathComponent(self._trashDirname), withIntermediateDirectories: true, attributes: nil)
    }
    catch {
      print("VMKVStorage init error: \(error)");
      
      return nil
    }
    
    super.init()
    
    let openResult = self._dbOpen()
    let initializeResult = self._dbInitialize()
    
    if !openResult || !initializeResult {
      // db file may broken...
      self._dbClose()
      
      // rebuild
      self._reset()
      
      let openResult = self._dbOpen()
      let initializeResult = self._dbInitialize()
      
      if !openResult || !initializeResult {
        self._dbClose()
        
        print("VMKVStorage init error: fail to open sqlite db.")
        
        return nil
      }
    }
    
    // empty the trash if failed at last time
    self._emptyTrashInBackground()
  }
  
  deinit {
    let taskIdentifier = _VMSharedApplication()?.beginBackgroundTask(withName: "com.max.jian.Kirogi.background.task", expirationHandler: nil)
    
    self._dbClose()
    
    if taskIdentifier != nil && taskIdentifier != .invalid {
      _VMSharedApplication()?.endBackgroundTask(taskIdentifier!)
    }
  }
  
  func itemExists(forKey key: String?) -> Bool {
    guard let key = key, !key.isEmpty else {
      return false
    }
    
    let result = self._dbGetItemCount(withKey: key) > 0
    
    return result
  }
  
  @discardableResult
  func saveItem(_ item: VMKVStorageItem) -> Bool {
    return self.saveItem(withKey: item.key, value: item.value, filename: item.filename)
  }
  
  @discardableResult
  func saveItem(withKey key: String?, value: Data?) -> Bool {
    return self.saveItem(withKey: key, value: value, filename: nil)
  }
  
  @discardableResult
  func saveItem(withKey key: String?, value: Data?, filename: String?) -> Bool {
    guard let key = key, !key.isEmpty, let value = value, !value.isEmpty else {
      return false
    }
    
    var saveItemResult: Bool
    
    switch self.type {
      case .file where filename != nil && !filename!.isEmpty:
        fallthrough
      case .mixed where filename != nil && !filename!.isEmpty:
        let writeResult = self._fileWrite(withName: filename!, data: value)
        if !writeResult {
          return writeResult
        }
        
        saveItemResult = self._dbSaveItem(withKey: key, value: value, filename: filename!)
        if !saveItemResult {
          self._fileDelete(withName: filename!)
        }
      case .sqlite:
        fallthrough
      case .mixed:
        let filename = self._dbGetFilename(forKey: key)
        if let filename = filename {
          self._fileDelete(withName: filename)
        }
        
        saveItemResult = self._dbSaveItem(withKey: key, value: value, filename: nil)
      case .file:
        saveItemResult = false
    }
    
    return saveItemResult
  }
  
  func getItem(forKey key: String?) -> VMKVStorageItem? {
    guard let key = key, !key.isEmpty else {
      return nil
    }
    
    var item = self._dbGetItem(withKey: key, excludeInlineData: false)
    
    if item != nil {
      switch self.type {
        case .sqlite:
          break
        case .file:
          fallthrough
        case .mixed:
          if let filename = item!.filename {
            item!.value = self._fileRead(withName: filename)
            if item!.value == nil {
              self._dbDeleteItem(withKey: key)
              
              item = nil
            }
          }
      }
    }
    
    if item != nil {
      self._dbUpdateLastAccessTimestamp(withKey: key)
    }
    
    return item
  }
  
  func getItemInfo(forKey key: String?) -> VMKVStorageItem? {
    guard let key = key, !key.isEmpty else {
      return nil
    }
    
    let itemInfo = self._dbGetItem(withKey: key, excludeInlineData: true)
    
    return itemInfo
  }
  
  func getItemValue(forKey key: String?) -> Data? {
    guard let key = key, !key.isEmpty else {
      return nil
    }
    
    let item = self.getItem(forKey: key)
    
    let itemValue = item?.value
    
    return itemValue
  }
  
  func getItems(forKeys keys: [String]?) -> [VMKVStorageItem]? {
    guard let keys = keys, !keys.isEmpty else {
      return nil
    }
    
    var items = self._dbGetItems(withKeys: keys, excludeInlineData: false) ?? []
    
    if !items.isEmpty {
      switch self.type {
        case .sqlite:
          break
        case .file:
          fallthrough
        case .mixed:
          items.enumerated().forEach { (indices, element) in
            if let filename = element.filename {
              items[indices].value = self._fileRead(withName: filename)
              if items[indices].value == nil {
                self._dbDeleteItem(withKey: element.key)
              }
            }
          }
          
          items.removeAll(where: { $0.value == nil })
      }
    }
    
    if !items.isEmpty {
      self._dbUpdateLastAccessTimestamp(withKeys: keys)
    }
    
    return items
  }
  
  func getItemInfos(forKeys keys: [String]?) -> [VMKVStorageItem]? {
    guard let keys = keys, !keys.isEmpty else {
      return nil
    }
    
    let itemInfos = self._dbGetItems(withKeys: keys, excludeInlineData: true)
    
    return itemInfos
  }
  
  func getItemValues(forKeys keys: [String]?) -> [String: Data]? {
    guard let keys = keys, !keys.isEmpty else {
      return nil
    }
    
    let items = self.getItems(forKeys: keys)
    
    let itemValues = items?.reduce(into: [String: Data]()) {
      if let key = $1.key, let value = $1.value {
        $0[key] = value
      }
    }
    
    return itemValues
  }
  
  @discardableResult
  func removeItem(forKey key: String?) -> Bool {
    guard let key = key, !key.isEmpty else {
      return false
    }
    
    switch self.type {
      case .sqlite:
        break
      case .file:
        fallthrough
      case .mixed:
        let filename = self._dbGetFilename(forKey: key)
        if let filename = filename {
          self._fileDelete(withName: filename)
        }
    }
    
    let removeItemResult = self._dbDeleteItem(withKey: key)
    
    return removeItemResult
  }
  
  @discardableResult
  func removeItems(forKeys keys: [String]?) -> Bool {
    guard let keys = keys, !keys.isEmpty else {
      return false
    }
    
    switch self.type {
      case .sqlite:
        break
      case .file:
        fallthrough
      case .mixed:
        let filenames = self._dbGetFilenames(forKeys: keys)
        if let filenames = filenames {
          filenames.forEach {
            self._fileDelete(withName: $0)
          }
        }
    }
    
    let removeItemsResult = self._dbDeleteItems(withKeys: keys)
    
    return removeItemsResult
  }
  
  @discardableResult
  func removeItemsLargerThanSize(_ size: Int) -> Bool {
    guard size != .max else {
      return true
    }
    
    var removeItemsResult: Bool = false
    
    if size <= 0 {
      removeItemsResult = self.removeAllItems()
    }
    else {
      switch self.type {
        case .sqlite:
          break
        case .file:
          fallthrough
        case .mixed:
          let filenames = self._dbGetFilenamesLargerThanSize(size)
          if let filenames = filenames {
            filenames.forEach {
              self._fileDelete(withName: $0)
            }
          }
      }
      
      removeItemsResult = self._dbDeleteItemsLargerThanSize(size)
      
      if removeItemsResult {
        self._dbCheckpoint()
      }
    }
    
    return removeItemsResult
  }
  
  @discardableResult
  func removeItemsEarlierThanTime(_ time: Int32) -> Bool {
    guard time > 0 else {
      return true
    }
    
    var removeItemsResult: Bool = false
    
    if time == .max {
      removeItemsResult = self.removeAllItems()
    }
    else {
      switch self.type {
        case .sqlite:
          break
        case .file:
          fallthrough
        case .mixed:
          let filenames = self._dbGetFilenamesEarlierThanTime(time)
          if let filenames = filenames {
            filenames.forEach {
              self._fileDelete(withName: $0)
            }
          }
      }
      
      removeItemsResult = self._dbDeleteItemsEarlierThanTime(time)
      
      if removeItemsResult {
        self._dbCheckpoint()
      }
    }
    
    return removeItemsResult
  }
  
  @discardableResult
  func removeItemsToFitSize(_ maxSize: Int) -> Bool {
    guard maxSize != .max else {
      return true
    }
    
    var removeItemsResult: Bool = false
    
    if maxSize <= 0 {
      removeItemsResult = self.removeAllItems()
    }
    else {
      var totalItemSize = self._dbGetTotalItemSize()
      
      if totalItemSize < 0 {
        removeItemsResult = false
      }
      else if totalItemSize <= maxSize {
        removeItemsResult = true
      }
      else {
        var deletableItems: [VMKVStorageItem]!
        
        repeat {
          deletableItems = self._dbGetItemSizeInfosOrderByAccessTime(withLimit: 16) ?? []
          
          for deletableItem in deletableItems {
            if totalItemSize > maxSize {
              switch self.type {
                case .sqlite:
                  break
                case .file:
                  fallthrough
                case .mixed:
                  if let filename = deletableItem.filename {
                    self._fileDelete(withName: filename)
                  }
              }
              
              removeItemsResult = self._dbDeleteItem(withKey: deletableItem.key)
              
              totalItemSize -= deletableItem.size
            }
            else {
              break
            }
            
            if !removeItemsResult {
              break
            }
          }
        } while totalItemSize > maxSize && deletableItems.count > 0 && removeItemsResult
        
        if removeItemsResult {
          self._dbCheckpoint()
        }
      }
    }
    
    return removeItemsResult
  }
  
  @discardableResult
  func removeItemsToFitCount(_ maxCount: Int) -> Bool {
    guard maxCount != .max else {
      return true
    }
    
    var removeItemsResult: Bool = false
    
    if maxCount <= 0 {
      removeItemsResult = self.removeAllItems()
    }
    else {
      var totalItemCount = self._dbGetTotalItemCount()
      
      if totalItemCount < 0 {
        removeItemsResult = false
      }
      else if totalItemCount <= maxCount {
        removeItemsResult = true
      }
      else {
        var deletableItems: [VMKVStorageItem]!
        
        repeat {
          deletableItems = self._dbGetItemSizeInfosOrderByAccessTime(withLimit: 16) ?? []
          
          for deletableItem in deletableItems {
            if totalItemCount > maxCount {
              switch self.type {
                case .sqlite:
                  break
                case .file:
                  fallthrough
                case .mixed:
                  if let filename = deletableItem.filename {
                    self._fileDelete(withName: filename)
                  }
              }
              
              removeItemsResult = self._dbDeleteItem(withKey: deletableItem.key)
              
              totalItemCount -= 1
            }
            else {
              break
            }
            
            if !removeItemsResult {
              break
            }
          }
        } while totalItemCount > maxCount && deletableItems.count > 0 && removeItemsResult
        
        if removeItemsResult {
          self._dbCheckpoint()
        }
      }
    }
    
    return removeItemsResult
  }
  
  @discardableResult
  func removeAllItems() -> Bool {
    let closeResult = self._dbClose()
    guard closeResult else {
      return closeResult
    }
    
    self._reset()
    
    let openResult = self._dbOpen()
    let initializeResult = self._dbInitialize()
    
    return openResult && initializeResult
  }
  
  func removeAllItems(_ progress: ((Int, Int) -> Void)?, block: ((Bool) -> Void)?) {
    let totalItemCount = self._dbGetTotalItemCount()
    
    if totalItemCount <= 0 {
      block?(totalItemCount == 0)
    }
    else {
      var leftItemCount = totalItemCount
      
      var deletableItems: [VMKVStorageItem]!
      
      var removeItemsResult: Bool = false
      
      repeat {
        deletableItems = self._dbGetItemSizeInfosOrderByAccessTime(withLimit: 32) ?? []
        
        for deletableItem in deletableItems {
          if leftItemCount > 0 {
            switch self.type {
              case .sqlite:
                break
              case .file:
                fallthrough
              case .mixed:
                if let filename = deletableItem.filename {
                  self._fileDelete(withName: filename)
                }
            }
            
            removeItemsResult = self._dbDeleteItem(withKey: deletableItem.key)
            
            leftItemCount -= 1
          }
          else {
            break
          }
          
          if !removeItemsResult {
            break
          }
        }
        
        progress?(totalItemCount - leftItemCount, totalItemCount)
      } while leftItemCount > 0 && deletableItems.count > 0 && removeItemsResult
      
      if removeItemsResult {
        self._dbCheckpoint()
      }
      
      block?(removeItemsResult)
    }
  }
  
  func itemsCount() -> Int {
    let totalItemCount = self._dbGetTotalItemCount()
    
    return totalItemCount
  }
  
  func itemsSize() -> Int {
    let totalItemSize = self._dbGetTotalItemSize()
    
    return totalItemSize
  }
}

extension VMKVStorage {
  
  // MARK: - private
  private func _reset() {
    try? FileManager.default.removeItem(atPath: self.path.stringByAppendingPathComponent(self._dbFilename))
    try? FileManager.default.removeItem(atPath: self.path.stringByAppendingPathComponent(self._dbWALFilename))
    try? FileManager.default.removeItem(atPath: self.path.stringByAppendingPathComponent(self._dbWALIndexFilename))
    
    self._allFileMoveToTrash()
    self._emptyTrashInBackground()
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
      self._dbOpenFailLastTime = ProcessInfo.processInfo.systemUptime
      
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
        filename text,
        size integer,
        last_modification_timestamp integer,
        last_access_timestamp integer,
        primary key(key)
      );
      
      create index if not exists last_access_timestamp_idx on kirogi(last_access_timestamp);
      """
    
    return self._dbExecute(sql)
  }
  
  @discardableResult
  private func _dbClose() -> Bool {
    guard self._db != nil else {
      return true
    }
    
    if self._dbStmtCache != nil {
      self._dbStmtCache!.removeAll()
    }
    self._dbStmtCache = nil
    
    var needsRetry = false
    var stmtFinalized = false
    
    var closeCode: Int32 = 0
    
    repeat {
      needsRetry = false
      
      closeCode = sqlite3_close(self._db!)
      if closeCode == SQLITE_BUSY || closeCode == SQLITE_LOCKED {
        if !stmtFinalized {
          stmtFinalized = true
          
          while let nextStmt = sqlite3_next_stmt(self._db!, nil) {
            sqlite3_finalize(nextStmt)
            
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
    let effectiveRetryTimeInterval = ProcessInfo.processInfo.systemUptime - self._dbOpenFailLastTime > self._dbOpenRetryTimeInterval
    
    var result: Bool = false
    
    if effectiveRetryCount && effectiveRetryTimeInterval {
      let openResult = self._dbOpen()
      let initializeResult = self._dbInitialize()
      
      result = openResult && initializeResult
    }
    
    return result
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
    if stmt == nil {
      let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
      guard prepareCode == SQLITE_OK else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
  private func _dbSaveItem(withKey key: String, value: Data, filename: String?) -> Bool {
    guard !key.isEmpty else {
      return false
    }
    
    let sql = """
      insert
        or replace into kirogi (
          key,
          inline_data,
          filename,
          size,
          last_modification_timestamp,
          last_access_timestamp
        )
      values
        (?1, ?2, ?3, ?4, ?5, ?6);
      """
    
    let stmt = self._dbPrepareStmt(sql)
    guard stmt != nil else {
      return false
    }
    
    let timestamp = Int32(Date().timeIntervalSince1970)
    
    sqlite3_bind_text(stmt!, 1, key, -1, nil)
    
    if filename == nil || filename!.isEmpty {
      sqlite3_bind_blob(stmt!, 2, [UInt8](value), Int32([UInt8](value).count), nil)
    }
    else {
      sqlite3_bind_blob(stmt!, 2, nil, 0, nil)
    }
        
    sqlite3_bind_text(stmt!, 3, filename, -1, nil)
    
    sqlite3_bind_int(stmt!, 4, Int32([UInt8](value).count))
    
    sqlite3_bind_int(stmt!, 5, timestamp)
    sqlite3_bind_int(stmt!, 6, timestamp)
    
    let stepCode = sqlite3_step(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite insert error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp
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
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp
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
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp
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
        filename,
        size,
        last_modification_timestamp,
        last_access_timestamp
      from
        kirogi
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    
    var stmt: OpaquePointer? = nil
    
    let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
    guard prepareCode == SQLITE_OK else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
      }
      
      return nil
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    var items: [VMKVStorageItem]? = []
    
    var stepCode: Int32 = 0
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        let item = self._dbGetItem(fromStmt: stmt!, excludeInlineData: excludeInlineData)
        if let item = item {
          items?.append(item)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
    
    let filename = sqlite3_column_text(stmt, iCol++).flatMap { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
    
    let size = Int(sqlite3_column_int(stmt, iCol++))
    
    let lastModificationTimestamp = sqlite3_column_int(stmt, iCol++)
    let lastAccessTimestamp = sqlite3_column_int(stmt, iCol++)
    
    let item = VMKVStorageItem(key: key, value: inlineData, filename: filename, size: size, lastModificationTimestamp: lastModificationTimestamp, lastAccessTimestamp: lastAccessTimestamp)
    
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
    
    var itemSizeInfos: [VMKVStorageItem]? = []
    
    var stepCode: Int32 = 0
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        let key = sqlite3_column_text(stmt, 0).flatMap { String(cString: $0) }
        
        let filename = sqlite3_column_text(stmt, 1).flatMap { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
        
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
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
        }
        
        itemSizeInfos = nil
        
        break
      }
      
    } while true
    
    return itemSizeInfos
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
      filename = sqlite3_column_text(stmt!, 0).flatMap { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
    }
    else if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
      }
      
      return nil
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    var filenames: [String]? = []
    
    var stepCode: Int32 = 0
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        let filename = sqlite3_column_text(stmt!, 0).flatMap { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
        if let filename = filename {
          filenames?.append(filename)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
    
    var filenames: [String]? = []
    
    var stepCode: Int32 = 0
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        let filename = sqlite3_column_text(stmt!, 0).flatMap { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
        if let filename = filename {
          filenames?.append(filename)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
    
    var filenames: [String]? = []
    
    var stepCode: Int32 = 0
    
    repeat {
      stepCode = sqlite3_step(stmt!)
      
      if stepCode == SQLITE_ROW {
        let filename = sqlite3_column_text(stmt!, 0).flatMap { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
        if let filename = filename {
          filenames?.append(filename)
        }
      }
      else if stepCode == SQLITE_DONE {
        break
      }
      else {
        if self.errorLogsEnabled {
          print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite query error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
      }
      
      return false
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    let stepCode = sqlite3_step(stmt!)
    
    sqlite3_finalize(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
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
    
    let sql = """
      update
        kirogi
      set
        last_access_timestamp = \(Int32(Date().timeIntervalSince1970))
      where
        key in (\(self._dbJoinedKeys(keys)));
      """
    
    var stmt: OpaquePointer? = nil
    
    let prepareCode = sqlite3_prepare_v2(self._db!, sql, -1, &stmt, nil)
    guard prepareCode == SQLITE_OK else {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite stmp prepare error (\(prepareCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
      }
      
      return false
    }
    
    self._dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
    
    let stepCode = sqlite3_step(stmt!)
    
    sqlite3_finalize(stmt!)
    
    if stepCode != SQLITE_DONE {
      if self.errorLogsEnabled {
        print("\(#function) line:\(#line) sqlite update error (\(stepCode)): \(String(cString: sqlite3_errmsg(self._db!)))")
      }
    }
    
    return stepCode == SQLITE_DONE
  }
}

extension VMKVStorage {
  
  // MARK: - file
  @discardableResult
  private func _fileWrite(withName filename: String, data: Data) -> Bool {
    let fileDataUrl = URL(fileURLWithPath: self._dataPath).appendingPathComponent(filename)
    
    do {
      try data.write(to: fileDataUrl, options: [])
      
      return true
    }
    catch {
      return false
    }
  }
  
  private func _fileRead(withName filename: String) -> Data? {
    let fileDataUrl = URL(fileURLWithPath: self._dataPath).appendingPathComponent(filename)
    
    let fileData = try? Data(contentsOf: fileDataUrl, options: [])
    
    return fileData
  }
  
  @discardableResult
  private func _fileDelete(withName filename: String) -> Bool {
    let fileDataPath = self._dataPath.stringByAppendingPathComponent(filename)
    
    do {
      try FileManager.default.removeItem(atPath: fileDataPath)
      
      return true
    }
    catch {
      return false
    }
  }
  
  @discardableResult
  private func _allFileMoveToTrash() -> Bool {
    let uuidString = UUID().uuidString
    
    let dataPath = self._dataPath
    let tmpTrashPath = self._trashPath.stringByAppendingPathComponent(uuidString)
    
    do {
      try FileManager.default.moveItem(atPath: dataPath, toPath: tmpTrashPath)
      try FileManager.default.createDirectory(atPath: dataPath, withIntermediateDirectories: true, attributes: nil)
      
      return true
    }
    catch {
      return false
    }
  }
  
  private func _emptyTrashInBackground() {
    let trashPath = self._trashPath
    
    self._trashQueue.async {
      let fileManager = FileManager.default
      
      do {
        let directoryContents = try fileManager.contentsOfDirectory(atPath: trashPath)
        
        directoryContents.forEach {
          let contentTrashPath = trashPath.stringByAppendingPathComponent($0)
          
          try? fileManager.removeItem(atPath: contentTrashPath)
        }
      }
      catch {
        
      }
    }
  }
}

#endif
