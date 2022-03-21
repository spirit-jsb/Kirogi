//
//  VMDiskCache.swift
//  Kirogi
//
//  Created by Max on 2022/3/13.
//

#if canImport(Foundation) && canImport(UIKit)

import Foundation
import UIKit

private func _VMFreeDiskSpace() -> Int {
  let homePath = NSHomeDirectory()
  
  var freeDiskSpace: Int = -1
  
  let attributes = try? FileManager.default.attributesOfFileSystem(forPath: homePath)
  if let attributes = attributes {
    let systemFreeSize = attributes[.systemFreeSize] as? Int
    if let systemFreeSize = systemFreeSize {
      freeDiskSpace = systemFreeSize < 0 ? -1 : systemFreeSize
    }
  }
  
  return freeDiskSpace
}

internal class VMDiskCache<Key: Hashable, Value: Codable>: NSObject {
  
  var name: String?
  
  private(set) var path: String
  
  private(set) var inlineThreshold: UInt
  
  var costLimit: UInt
  
  var countLimit: UInt
  
  var ageLimit: TimeInterval
  
  var autoTrimInterval: TimeInterval
  
  var freeDiskSpaceLimit: UInt
  
  var errorLogsEnabled: Bool {
    get {
      self._lock.wait(timeout: .distantFuture)
      
      let errorLogsEnabled = self._kvStorage?.errorLogsEnabled ?? false
      
      self._lock.signal()
      
      return errorLogsEnabled
    }
    set {
      self._lock.wait(timeout: .distantFuture)
      
      self._kvStorage?.errorLogsEnabled = newValue
      
      self._lock.signal()
    }
  }
  
  private let _nonrandomHasher: Hasher
  
  private let _queue: DispatchQueue
  
  private let _lock: DispatchSemaphore
  
  private var _kvStorage: VMKVStorage?
  
  static func initialize(path: String?) -> VMDiskCache? {
    
  }
  
  static func initialize(path: String?, inlineThreshold: UInt) -> VMDiskCache? {
    
  }
  
  private init?(path: String?, inlineThreshold: UInt) {
    guard let path = path, !path.isEmpty else {
      print("VMDiskCache init error: invalid path: [\(String(describing: path))].")
      
      return nil
    }
    
    let kvStorageType: VMKVStorageType
    switch inlineThreshold {
      case 0:
        kvStorageType = .file
      case .max:
        kvStorageType = .sqlite
      default:
        kvStorageType = .mixed
    }
    
    let kvStorage = VMKVStorage.initialize(path: path, type: kvStorageType)
    guard let kvStorage = kvStorage else {
      return nil
    }
    
    self.path = path
    
    self.inlineThreshold = inlineThreshold
    
    self.costLimit = .max
    self.countLimit = .max
    self.ageLimit = .greatestFiniteMagnitude
    
    self.autoTrimInterval = 60.0
    
    self.freeDiskSpaceLimit = 0
    
    self._nonrandomHasher = Hasher.nonrandomHasher()
    
    self._queue = DispatchQueue(label: "com.max.jian.Kirogi.cache.disk", attributes: .concurrent)
    
    self._lock = DispatchSemaphore(value: 1)
    
    self._kvStorage = kvStorage
    
    super.init()
    
    self._addObserver()
    
    self._trimRecursively()
  }
  
  deinit {
    self._removeObserver()
  }
  
  func contains(forKey key: Key?) -> Bool {
    guard self._kvStorage != nil else {
      return false
    }
    
    self._lock.wait(timeout: .distantFuture)
    
    let kvStorageKey = self._kvStorageKey(forKey: key)
    let containsResult = self._kvStorage!.itemExists(forKey: kvStorageKey)
    
    self._lock.signal()
    
    return containsResult
  }
  
  func contains(forKey key: Key?, block: ((Key?, Bool) -> Void)?) {
    guard block != nil else {
      return
    }
    
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      let containsResult = self.contains(forKey: key)
      
      block!(key, containsResult)
    }
  }
  
  func setObject(_ object: Value?, forKey key: Key?) {
    guard self._kvStorage != nil else {
      return
    }
    
    guard let key = key else {
      return
    }
    
    guard let object = object else {
      self.removeObject(forKey: key)
      
      return
    }
    
    let kvStorageKey = self._kvStorageKey(forKey: key)
    
    let kvStorageValue: Data?
    do {
      let archiver = NSKeyedArchiver(requiringSecureCoding: true)
      
      try archiver.encodeEncodable(object, forKey: NSKeyedArchiveRootObjectKey)
      
      archiver.finishEncoding()
      
      kvStorageValue = archiver.encodedData
    }
    catch {
      kvStorageValue = nil
    }
    
    guard let kvStorageValue = kvStorageValue else {
      return
    }
    
    let kvStorageFilename = self._kvStorage!.type != .sqlite && [UInt8](kvStorageValue).count > self.inlineThreshold ? self._filename(forKey: key) : nil
    
    self._lock.wait(timeout: .distantFuture)
    
    self._kvStorage!.saveItem(withKey: kvStorageKey, value: kvStorageValue, filename: kvStorageFilename)
    
    self._lock.signal()
  }
  
  func setObject(_ object: Value?, forKey key: Key?, block: (() -> Void)?) {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self.setObject(object, forKey: key)
      
      if block != nil {
        block!()
      }
    }
  }
  
  func object(forKey key: Key?) -> Value? {
    guard self._kvStorage != nil else {
      return nil
    }
    
    guard let key = key else {
      return nil
    }
    
    let kvStorageKey = self._kvStorageKey(forKey: key)
    
    self._lock.wait(timeout: .distantFuture)
    
    let item = self._kvStorage!.getItem(forKey: kvStorageKey)
    
    self._lock.signal()
    
    guard let itemValue = item?.value else {
      return nil
    }
    
    let value: Value?
    do {
      let unarchiver = try NSKeyedUnarchiver(forReadingFrom: itemValue)
      unarchiver.decodingFailurePolicy = .setErrorAndReturn
      
      value = unarchiver.decodeDecodable(Value.self, forKey: NSKeyedArchiveRootObjectKey)
      
      unarchiver.finishDecoding()
    }
    catch {
      value = nil
    }
    
    return value
  }
  
  func object(forKey key: Key?, block: ((Key?, Value?) -> Void)?) {
    guard block != nil else {
      return
    }
    
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      let value = self.object(forKey: key)
      
      block!(key, value)
    }
  }
  
  func removeObject(forKey key: Key?) {
    guard self._kvStorage != nil else {
      return
    }
    
    self._lock.wait(timeout: .distantFuture)
    
    let kvStorageKey = self._kvStorageKey(forKey: key)
    self._kvStorage!.removeItem(forKey: kvStorageKey)
    
    self._lock.signal()
  }
  
  func removeObject(forKey key: Key?, block: ((Key?) -> Void)?) {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self.removeObject(forKey: key)
      
      if block != nil {
        block!(key)
      }
    }
  }
  
  func removeAllObjects() {
    guard self._kvStorage != nil else {
      return
    }
    
    self._lock.wait(timeout: .distantFuture)
    
    self._kvStorage!.removeAllItems()
    
    self._lock.signal()
  }
  
  func removeAllObjects(_ block: (() -> Void)?) {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self.removeAllObjects()
      
      if block != nil {
        block!()
      }
    }
  }
  
  func removeAllObjects(_ progress: ((Int, Int) -> Void)?, completion: ((Bool) -> Void)?) {
    guard self._kvStorage != nil else {
      if completion != nil {
        completion!(true)
      }
      
      return
    }
    
    self._queue.async { [weak self] in
      guard let self = self else {
        if completion != nil {
          completion!(true)
        }
        
        return
      }
      
      self._lock.wait(timeout: .distantFuture)
      
      self._kvStorage!.removeAllItems(progress, completion: completion)
      
      self._lock.signal()
    }
  }
  
  func totalCost() -> Int {
    guard self._kvStorage != nil else {
      return -1
    }
    
    self._lock.wait(timeout: .distantFuture)
    
    let totalCostResult = self._kvStorage!.itemsSize()
    
    self._lock.signal()
    
    return totalCostResult
  }
  
  func totalCost(_ block: ((Int) -> Void)?) {
    guard block != nil else {
      return
    }
    
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      let totalCostResult = self.totalCost()
      
      block!(totalCostResult)
    }
  }
  
  func totalCount() -> Int {
    guard self._kvStorage != nil else {
      return -1
    }
    
    self._lock.wait(timeout: .distantFuture)
    
    let totalCountResult = self._kvStorage!.itemsCount()
    
    self._lock.signal()
    
    return totalCountResult
  }
  
  func totalCount(_ block: ((Int) -> Void)?) {
    guard block != nil else {
      return
    }
    
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      let totalCountResult = self.totalCount()
      
      block!(totalCountResult)
    }
  }
  
  func trim(forCost costLimit: UInt) {
    self._lock.wait(timeout: .distantFuture)
    
    self._trim(forCost: costLimit)
    
    self._lock.signal()
  }
  
  func trim(forCost costLimit: UInt, block: (() -> Void)?) {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self.trim(forCost: costLimit)
      
      if block != nil {
        block!()
      }
    }
  }
  
  func trim(forCount countLimit: UInt) {
    self._lock.wait(timeout: .distantFuture)
    
    self._trim(forCount: countLimit)
    
    self._lock.signal()
  }
  
  func trim(forCount countLimit: UInt, block: (() -> Void)?) {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self.trim(forCount: countLimit)
      
      if block != nil {
        block!()
      }
    }
  }
  
  func trim(forAge ageLimit: TimeInterval) {
    self._lock.wait(timeout: .distantFuture)
    
    self._trim(forAge: ageLimit)
    
    self._lock.signal()
  }
  
  func trim(forAge ageLimit: TimeInterval, block: (() -> Void)?) {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self.trim(forAge: ageLimit)
      
      if block != nil {
        block!()
      }
    }
  }
}

extension VMDiskCache {
  
  private func _kvStorageKey(forKey key: Key?) -> String? {
    guard let key = key else {
      return nil
    }
    
    var kvStorageKeyResult: String
    
    if key is String {
      kvStorageKeyResult = key as! String
    }
    else {
      var nonrandomHasher = self._nonrandomHasher
      key.hash(into: &nonrandomHasher)
      
      let nonrandomHashValue = nonrandomHasher.finalize()
      
      kvStorageKeyResult = "\(nonrandomHashValue)"
    }
    
    return kvStorageKeyResult
  }
  
  private func _filename(forKey key: Key?) -> String? {
    let filenameResult = self._kvStorageKey(forKey: key)?.md5()
    
    return filenameResult
  }
  
  private func _trimRecursively() {
    DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .nanoseconds(Int(self.autoTrimInterval * Double(NSEC_PER_SEC)))) { [weak self] in
      guard let self = self else {
        return
      }
      
      self._trimInBackground()
      self._trimRecursively()
    }
  }
  
  private func _trimInBackground() {
    self._queue.async { [weak self] in
      guard let self = self else {
        return
      }
      
      self._lock.wait(timeout: .distantFuture)
      
      self._trim(forCost: self.costLimit)
      self._trim(forCount: self.countLimit)
      self._trim(forAge: self.ageLimit)
      self._trim(forFreeDiskSpace: self.freeDiskSpaceLimit)
      
      self._lock.signal()
    }
  }
  
  private func _trim(forCost costLimit: UInt) {
    guard self._kvStorage != nil else {
      return
    }
    
    guard costLimit < .max else {
      return
    }
    
    self._kvStorage!.removeItemsToFitSize(Int(costLimit))
  }
  
  private func _trim(forCount countLimit: UInt) {
    guard self._kvStorage != nil else {
      return
    }
    
    guard countLimit < .max else {
      return
    }
    
    self._kvStorage!.removeItemsToFitCount(Int(countLimit))
  }
  
  private func _trim(forAge ageLimit: TimeInterval) {
    guard self._kvStorage != nil else {
      return
    }
    
    guard ageLimit > 0 else {
      self._kvStorage!.removeAllItems()
      
      return
    }
    
    let timestamp = Date().timeIntervalSince1970
    guard timestamp > ageLimit else {
      return
    }
    
    let age = timestamp - ageLimit
    guard age < .greatestFiniteMagnitude else {
      return
    }
    
    self._kvStorage!.removeItemsEarlierThanTime(Int32(age))
  }
  
  private func _trim(forFreeDiskSpace freeDiskSpaceLimit: UInt) {
    guard self._kvStorage != nil else {
      return
    }
    
    guard freeDiskSpaceLimit != 0 else {
      return
    }
    
    let totalItemSize = self._kvStorage!.itemsSize()
    guard totalItemSize > 0 else {
      return
    }
    
    let freeDiskSpace = _VMFreeDiskSpace()
    guard freeDiskSpace >= 0 else {
      return
    }
    
    let needsTrimSize = Int(freeDiskSpaceLimit) - freeDiskSpace
    guard needsTrimSize > 0 else {
      return
    }
    
    var costLimit = totalItemSize - needsTrimSize
    if costLimit < 0 {
      costLimit = 0
    }
    
    self._trim(forCost: UInt(costLimit))
  }
}

extension VMDiskCache {
  
  private func _addObserver() {
    NotificationCenter.default.addObserver(forName: UIApplication.willTerminateNotification, object: nil, queue: nil) { [weak self] (notification) in
      self?._appWillTerminate(notification)
    }
  }
  
  private func _removeObserver() {
    NotificationCenter.default.removeObserver(self, name: UIApplication.willTerminateNotification, object: nil)
  }
  
  private func _appWillTerminate(_ notification: Notification) {
    self._lock.wait(timeout: .distantFuture)
    
    self._kvStorage = nil
    
    self._lock.signal()
  }
}

#endif
