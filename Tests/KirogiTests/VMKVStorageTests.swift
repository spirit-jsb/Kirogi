//
//  VMKVStorageTests.swift
//  KirogiTests
//
//  Created by max on 2022/4/2.
//

import XCTest
@testable import Kirogi

class VMKVStorageTests: XCTestCase {
  
  var kvFileStorage: VMKVStorage!
  var kvSqliteStorage: VMKVStorage!
  var kvMixedStorage: VMKVStorage!
  
  private let _basePath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!
  
  private let _baseEncoder = JSONEncoder()
  private let _baseDecoder = JSONDecoder()
  
  private lazy var _kvFileStoragePath = self._basePath.stringByAppendingPathComponent("kvFile")
  private lazy var _kvSqliteStoragePath = self._basePath.stringByAppendingPathComponent("kvSqlite")
  private lazy var _kvMixedStoragePath = self._basePath.stringByAppendingPathComponent("kvMixed")
  
  override func setUp() {
    super.setUp()
    
    self.kvFileStorage = VMKVStorage.initialize(path: self._kvFileStoragePath, type: .file)
    self.kvSqliteStorage = VMKVStorage.initialize(path: self._kvSqliteStoragePath, type: .sqlite)
    self.kvMixedStorage = VMKVStorage.initialize(path: self._kvMixedStoragePath, type: .mixed)
  }
  
  override func tearDown() {
    self.kvFileStorage.removeAllItems()
    self.kvSqliteStorage.removeAllItems()
    self.kvMixedStorage.removeAllItems()
    
    super.tearDown()
  }
  
  func test_property() {
    XCTAssertEqual(self.kvFileStorage.path, self._kvFileStoragePath)
    XCTAssertEqual(self.kvSqliteStorage.path, self._kvSqliteStoragePath)
    XCTAssertEqual(self.kvMixedStorage.path, self._kvMixedStoragePath)
    
    XCTAssertEqual(self.kvFileStorage.type, .file)
    XCTAssertEqual(self.kvSqliteStorage.type, .sqlite)
    XCTAssertEqual(self.kvMixedStorage.type, .mixed)
    
    XCTAssertTrue(self.kvFileStorage.errorLogsEnabled)
    self.kvFileStorage.errorLogsEnabled = false
    XCTAssertFalse(self.kvFileStorage.errorLogsEnabled)
    
    XCTAssertTrue(self.kvSqliteStorage.errorLogsEnabled)
    self.kvSqliteStorage.errorLogsEnabled = false
    XCTAssertFalse(self.kvSqliteStorage.errorLogsEnabled)
    
    XCTAssertTrue(self.kvMixedStorage.errorLogsEnabled)
    self.kvMixedStorage.errorLogsEnabled = false
    XCTAssertFalse(self.kvMixedStorage.errorLogsEnabled)
  }
  
  func test_itemExists() {
    XCTAssertFalse(self.kvFileStorage.itemExists(forKey: nil))
    XCTAssertFalse(self.kvSqliteStorage.itemExists(forKey: nil))
    XCTAssertFalse(self.kvMixedStorage.itemExists(forKey: nil))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.itemExists(forKey: "user_max"))
    XCTAssertFalse(self.kvFileStorage.itemExists(forKey: "kirogi"))
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue)
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.itemExists(forKey: "user_max"))
    XCTAssertFalse(self.kvSqliteStorage.itemExists(forKey: "kirogi"))
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue)
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.itemExists(forKey: "user_max"))
    XCTAssertFalse(self.kvMixedStorage.itemExists(forKey: "kirogi"))
  }
  
  func test_saveItem() {
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    
    let kvNullKeyStorageItem = VMKVStorageItem(key: nil, value: maxValue, filename: nil, size: 0, lastModificationTimestamp: 0, lastAccessTimestamp: 0)
    let kvEmptyKeyStorageItem = VMKVStorageItem(key: "", value: maxValue, filename: "", size: 0, lastModificationTimestamp: 0, lastAccessTimestamp: 0)
    
    let kvNullValueStorageItem = VMKVStorageItem(key: "user", value: nil, filename: "user", size: 0, lastModificationTimestamp: 0, lastAccessTimestamp: 0)
    let kvEmptyValueStorageItem = VMKVStorageItem(key: "user", value: Data(), filename: "user", size: 0, lastModificationTimestamp: 0, lastAccessTimestamp: 0)
    
    let kvStorageItem = VMKVStorageItem(key: "user_max", value: maxValue, filename: "user_max", size: 0, lastModificationTimestamp: 0, lastAccessTimestamp: 0)
    
    XCTAssertFalse(self.kvFileStorage.saveItem(kvNullKeyStorageItem))
    XCTAssertFalse(self.kvFileStorage.saveItem(kvEmptyKeyStorageItem))
    XCTAssertFalse(self.kvFileStorage.saveItem(kvNullValueStorageItem))
    XCTAssertFalse(self.kvFileStorage.saveItem(kvEmptyValueStorageItem))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: nil, value: maxValue))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "", value: maxValue))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "user", value: nil))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "user", value: Data()))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: nil, value: maxValue, filename: nil))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "", value: maxValue, filename: ""))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "user", value: nil, filename: "user"))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "user", value: Data(), filename: "user"))
    XCTAssertTrue(self.kvFileStorage.saveItem(kvStorageItem))
    XCTAssertFalse(self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue))
    XCTAssertTrue(self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max"))
    
    XCTAssertFalse(self.kvSqliteStorage.saveItem(kvNullKeyStorageItem))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(kvEmptyKeyStorageItem))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(kvNullValueStorageItem))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(kvEmptyValueStorageItem))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: nil, value: maxValue))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: "", value: maxValue))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: "user", value: nil))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: "user", value: Data()))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: nil, value: maxValue, filename: nil))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: "", value: maxValue, filename: ""))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: "user", value: nil, filename: "user"))
    XCTAssertFalse(self.kvSqliteStorage.saveItem(withKey: "user", value: Data(), filename: "user"))
    XCTAssertTrue(self.kvSqliteStorage.saveItem(kvStorageItem))
    XCTAssertTrue(self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue))
    XCTAssertTrue(self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max"))
    
    XCTAssertFalse(self.kvMixedStorage.saveItem(kvNullKeyStorageItem))
    XCTAssertFalse(self.kvMixedStorage.saveItem(kvEmptyKeyStorageItem))
    XCTAssertFalse(self.kvMixedStorage.saveItem(kvNullValueStorageItem))
    XCTAssertFalse(self.kvMixedStorage.saveItem(kvEmptyValueStorageItem))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: nil, value: maxValue))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: "", value: maxValue))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: "user", value: nil))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: "user", value: Data()))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: nil, value: maxValue, filename: nil))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: "", value: maxValue, filename: ""))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: "user", value: nil, filename: "user"))
    XCTAssertFalse(self.kvMixedStorage.saveItem(withKey: "user", value: Data(), filename: "user"))
    XCTAssertTrue(self.kvMixedStorage.saveItem(kvStorageItem))
    XCTAssertTrue(self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue))
    XCTAssertTrue(self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max"))
  }
  
  func test_getItem() {
    XCTAssertNil(self.kvFileStorage.getItem(forKey: nil))
    XCTAssertNil(self.kvFileStorage.getItem(forKey: ""))
    
    XCTAssertNil(self.kvSqliteStorage.getItem(forKey: nil))
    XCTAssertNil(self.kvSqliteStorage.getItem(forKey: ""))
    
    XCTAssertNil(self.kvMixedStorage.getItem(forKey: nil))
    XCTAssertNil(self.kvMixedStorage.getItem(forKey: ""))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvFileStorage.getItem(forKey: "user_max"))
    XCTAssertNotNil(self.kvFileStorage.getItem(forKey: "user_max")?.value)
    XCTAssertEqual(self.kvFileStorage.getItem(forKey: "user_max").flatMap { $0.value }.flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.name, "max")
    XCTAssertEqual(self.kvFileStorage.getItem(forKey: "user_max").flatMap { $0.value }.flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.age, 28)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvSqliteStorage.getItem(forKey: "user_max"))
    XCTAssertNotNil(self.kvSqliteStorage.getItem(forKey: "user_max")?.value)
    XCTAssertEqual(self.kvSqliteStorage.getItem(forKey: "user_max").flatMap { $0.value }.flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.name, "max")
    XCTAssertEqual(self.kvSqliteStorage.getItem(forKey: "user_max").flatMap { $0.value }.flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.age, 28)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvMixedStorage.getItem(forKey: "user_max"))
    XCTAssertNotNil(self.kvMixedStorage.getItem(forKey: "user_max")?.value)
    XCTAssertEqual(self.kvMixedStorage.getItem(forKey: "user_max").flatMap { $0.value }.flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.name, "max")
    XCTAssertEqual(self.kvMixedStorage.getItem(forKey: "user_max").flatMap { $0.value }.flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.age, 28)
  }
  
  func test_getItemInfo() {
    XCTAssertNil(self.kvFileStorage.getItemInfo(forKey: nil))
    XCTAssertNil(self.kvFileStorage.getItemInfo(forKey: ""))
    
    XCTAssertNil(self.kvSqliteStorage.getItemInfo(forKey: nil))
    XCTAssertNil(self.kvSqliteStorage.getItemInfo(forKey: ""))
    
    XCTAssertNil(self.kvMixedStorage.getItemInfo(forKey: nil))
    XCTAssertNil(self.kvMixedStorage.getItemInfo(forKey: ""))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvFileStorage.getItemInfo(forKey: "user_max"))
    XCTAssertNil(self.kvFileStorage.getItemInfo(forKey: "user_max")?.value)
    XCTAssertEqual(self.kvFileStorage.getItemInfo(forKey: "user_max").flatMap { $0.key }, "user_max")
    XCTAssertEqual(self.kvFileStorage.getItemInfo(forKey: "user_max").flatMap { $0.filename }, "user_max")
    XCTAssertGreaterThan(self.kvFileStorage.getItemInfo(forKey: "user_max").flatMap { $0.size } ?? 0, 0)
    XCTAssertGreaterThan(self.kvFileStorage.getItemInfo(forKey: "user_max").flatMap { $0.lastAccessTimestamp } ?? 0, 0)
    XCTAssertGreaterThan(self.kvFileStorage.getItemInfo(forKey: "user_max").flatMap { $0.lastModificationTimestamp } ?? 0, 0)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvSqliteStorage.getItemInfo(forKey: "user_max"))
    XCTAssertNil(self.kvSqliteStorage.getItemInfo(forKey: "user_max")?.value)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfo(forKey: "user_max").flatMap { $0.key }, "user_max")
    XCTAssertNotEqual(self.kvSqliteStorage.getItemInfo(forKey: "user_max").flatMap { $0.filename }, "user_max")
    XCTAssertGreaterThan(self.kvSqliteStorage.getItemInfo(forKey: "user_max").flatMap { $0.size } ?? 0, 0)
    XCTAssertGreaterThan(self.kvSqliteStorage.getItemInfo(forKey: "user_max").flatMap { $0.lastAccessTimestamp } ?? 0, 0)
    XCTAssertGreaterThan(self.kvSqliteStorage.getItemInfo(forKey: "user_max").flatMap { $0.lastModificationTimestamp } ?? 0, 0)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvMixedStorage.getItemInfo(forKey: "user_max"))
    XCTAssertNil(self.kvMixedStorage.getItemInfo(forKey: "user_max")?.value)
    XCTAssertEqual(self.kvMixedStorage.getItemInfo(forKey: "user_max").flatMap { $0.key }, "user_max")
    XCTAssertEqual(self.kvMixedStorage.getItemInfo(forKey: "user_max").flatMap { $0.filename }, "user_max")
    XCTAssertGreaterThan(self.kvMixedStorage.getItemInfo(forKey: "user_max").flatMap { $0.size } ?? 0, 0)
    XCTAssertGreaterThan(self.kvMixedStorage.getItemInfo(forKey: "user_max").flatMap { $0.lastAccessTimestamp } ?? 0, 0)
    XCTAssertGreaterThan(self.kvMixedStorage.getItemInfo(forKey: "user_max").flatMap { $0.lastModificationTimestamp } ?? 0, 0)
  }

  func test_getItemValue() {
    XCTAssertNil(self.kvFileStorage.getItemValue(forKey: nil))
    XCTAssertNil(self.kvFileStorage.getItemValue(forKey: ""))
    
    XCTAssertNil(self.kvSqliteStorage.getItemValue(forKey: nil))
    XCTAssertNil(self.kvSqliteStorage.getItemValue(forKey: ""))
    
    XCTAssertNil(self.kvMixedStorage.getItemValue(forKey: nil))
    XCTAssertNil(self.kvMixedStorage.getItemValue(forKey: ""))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvFileStorage.getItemValue(forKey: "user_max"))
    XCTAssertEqual(self.kvFileStorage.getItemValue(forKey: "user_max").flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.name, "max")
    XCTAssertEqual(self.kvFileStorage.getItemValue(forKey: "user_max").flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.age, 28)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvSqliteStorage.getItemValue(forKey: "user_max"))
    XCTAssertEqual(self.kvSqliteStorage.getItemValue(forKey: "user_max").flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.name, "max")
    XCTAssertEqual(self.kvSqliteStorage.getItemValue(forKey: "user_max").flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.age, 28)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertNotNil(self.kvMixedStorage.getItemValue(forKey: "user_max"))
    XCTAssertEqual(self.kvMixedStorage.getItemValue(forKey: "user_max").flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.name, "max")
    XCTAssertEqual(self.kvMixedStorage.getItemValue(forKey: "user_max").flatMap { try? self._baseDecoder.decode(User.self, from: $0) }?.age, 28)
  }
  
  func test_getItems() {
    XCTAssertNil(self.kvFileStorage.getItems(forKeys: nil))
    XCTAssertNil(self.kvFileStorage.getItems(forKeys: []))
    
    XCTAssertNil(self.kvSqliteStorage.getItems(forKeys: nil))
    XCTAssertNil(self.kvSqliteStorage.getItems(forKeys: []))
    
    XCTAssertNil(self.kvMixedStorage.getItems(forKeys: nil))
    XCTAssertNil(self.kvMixedStorage.getItems(forKeys: []))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvFileStorage.getItems(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvFileStorage.getItems(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertTrue(self.kvFileStorage.getItems(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? false)
    XCTAssertTrue(self.kvFileStorage.getItems(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? false)
    XCTAssertNotNil(self.kvFileStorage.getItems(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvFileStorage.getItems(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertTrue(self.kvFileStorage.getItems(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? false)
    XCTAssertFalse(self.kvFileStorage.getItems(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? true)
    XCTAssertNotNil(self.kvFileStorage.getItems(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvFileStorage.getItems(forKeys: ["kirogi"])?.count, 0)
    XCTAssertFalse(self.kvFileStorage.getItems(forKeys: ["kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? true)
    XCTAssertFalse(self.kvFileStorage.getItems(forKeys: ["kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? true)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvSqliteStorage.getItems(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvSqliteStorage.getItems(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertTrue(self.kvSqliteStorage.getItems(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? false)
    XCTAssertTrue(self.kvSqliteStorage.getItems(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? false)
    XCTAssertNotNil(self.kvSqliteStorage.getItems(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvSqliteStorage.getItems(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertTrue(self.kvSqliteStorage.getItems(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? false)
    XCTAssertFalse(self.kvSqliteStorage.getItems(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? true)
    XCTAssertNotNil(self.kvSqliteStorage.getItems(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvSqliteStorage.getItems(forKeys: ["kirogi"])?.count, 0)
    XCTAssertFalse(self.kvSqliteStorage.getItems(forKeys: ["kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? true)
    XCTAssertFalse(self.kvSqliteStorage.getItems(forKeys: ["kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? true)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvMixedStorage.getItems(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvMixedStorage.getItems(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertTrue(self.kvMixedStorage.getItems(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? false)
    XCTAssertTrue(self.kvMixedStorage.getItems(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? false)
    XCTAssertNotNil(self.kvMixedStorage.getItems(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvMixedStorage.getItems(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertTrue(self.kvMixedStorage.getItems(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? false)
    XCTAssertFalse(self.kvMixedStorage.getItems(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? true)
    XCTAssertNotNil(self.kvMixedStorage.getItems(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvMixedStorage.getItems(forKeys: ["kirogi"])?.count, 0)
    XCTAssertFalse(self.kvMixedStorage.getItems(forKeys: ["kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(maxValue) } ?? true)
    XCTAssertFalse(self.kvMixedStorage.getItems(forKeys: ["kirogi"]).flatMap { $0.map { $0.value } }.flatMap { $0.contains(jianValue) } ?? true)
  }
  
  func test_getItemInfos() {
    XCTAssertNil(self.kvFileStorage.getItemInfos(forKeys: nil))
    XCTAssertNil(self.kvFileStorage.getItemInfos(forKeys: []))
    
    XCTAssertNil(self.kvSqliteStorage.getItemInfos(forKeys: nil))
    XCTAssertNil(self.kvSqliteStorage.getItemInfos(forKeys: []))
    
    XCTAssertNil(self.kvMixedStorage.getItemInfos(forKeys: nil))
    XCTAssertNil(self.kvMixedStorage.getItemInfos(forKeys: []))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertTrue(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertTrue(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_jian") } ?? false)
    XCTAssertTrue(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertTrue(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("user_jian") } ?? false)
    XCTAssertNotNil(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertTrue(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertFalse(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertTrue(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertFalse(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertNotNil(self.kvFileStorage.getItemInfos(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["kirogi"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertFalse(self.kvFileStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertFalse(self.kvFileStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("kirogi") } ?? true)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertTrue(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertTrue(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_jian") } ?? false)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.compactMap { $0.filename } }?.count, 0)
    XCTAssertNotNil(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertTrue(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertFalse(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.compactMap { $0.filename } }?.count, 0)
    XCTAssertNotNil(self.kvSqliteStorage.getItemInfos(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["kirogi"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertFalse(self.kvSqliteStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.compactMap { $0.filename } }?.count, 0)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertTrue(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertTrue(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_jian") } ?? false)
    XCTAssertTrue(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertTrue(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "user_jian"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("user_jian") } ?? false)
    XCTAssertNotNil(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertTrue(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertFalse(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertTrue(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("user_max") } ?? false)
    XCTAssertFalse(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertNotNil(self.kvMixedStorage.getItemInfos(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["kirogi"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.compactMap { $0.value } }?.count, 0)
    XCTAssertFalse(self.kvMixedStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.map { $0.key } }.flatMap { $0.contains("kirogi") } ?? true)
    XCTAssertFalse(self.kvMixedStorage.getItemInfos(forKeys: ["kirogi"]).flatMap { $0.map { $0.filename } }.flatMap { $0.contains("kirogi") } ?? true)
  }
  
  func test_getItemValues() {
    XCTAssertNil(self.kvFileStorage.getItemValues(forKeys: nil))
    XCTAssertNil(self.kvFileStorage.getItemValues(forKeys: []))
    
    XCTAssertNil(self.kvSqliteStorage.getItemValues(forKeys: nil))
    XCTAssertNil(self.kvSqliteStorage.getItemValues(forKeys: []))
    
    XCTAssertNil(self.kvMixedStorage.getItemValues(forKeys: nil))
    XCTAssertNil(self.kvMixedStorage.getItemValues(forKeys: []))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, maxValue)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    XCTAssertNotNil(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvFileStorage.getItemInfos(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "kirogi"]).flatMap { $0["user_max"] }, maxValue)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "kirogi"]).flatMap { $0["kirogi"] }, nil)
    XCTAssertNotNil(self.kvFileStorage.getItemValues(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["kirogi"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["kirogi"]).flatMap { $0["kirogi"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, maxValue)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    XCTAssertNotNil(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemInfos(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "kirogi"]).flatMap { $0["user_max"] }, maxValue)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "kirogi"]).flatMap { $0["kirogi"] }, nil)
    XCTAssertNotNil(self.kvSqliteStorage.getItemValues(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["kirogi"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["kirogi"]).flatMap { $0["kirogi"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertNotNil(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 2)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, maxValue)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    XCTAssertNotNil(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"]))
    XCTAssertEqual(self.kvMixedStorage.getItemInfos(forKeys: ["user_max", "kirogi"])?.count, 1)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "kirogi"]).flatMap { $0["user_max"] }, maxValue)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "kirogi"]).flatMap { $0["kirogi"] }, nil)
    XCTAssertNotNil(self.kvMixedStorage.getItemValues(forKeys: ["kirogi"]))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["kirogi"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["kirogi"]).flatMap { $0["kirogi"] }, nil)
  }
  
  func test_removeItem() {
    XCTAssertFalse(self.kvFileStorage.removeItem(forKey: nil))
    XCTAssertFalse(self.kvFileStorage.removeItem(forKey: ""))
    
    XCTAssertFalse(self.kvSqliteStorage.removeItem(forKey: nil))
    XCTAssertFalse(self.kvSqliteStorage.removeItem(forKey: ""))
    
    XCTAssertFalse(self.kvMixedStorage.removeItem(forKey: nil))
    XCTAssertFalse(self.kvMixedStorage.removeItem(forKey: ""))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItem(forKey: "user_max"))
    XCTAssertNil(self.kvFileStorage.getItem(forKey: "user_max"))
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItem(forKey: "user_max"))
    XCTAssertNil(self.kvSqliteStorage.getItem(forKey: "user_max"))
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItem(forKey: "user_max"))
    XCTAssertNil(self.kvMixedStorage.getItem(forKey: "user_max"))
  }
  
  func test_removeItems() {
    XCTAssertFalse(self.kvFileStorage.removeItems(forKeys: nil))
    XCTAssertFalse(self.kvFileStorage.removeItems(forKeys: []))
    
    XCTAssertFalse(self.kvSqliteStorage.removeItems(forKeys: nil))
    XCTAssertFalse(self.kvSqliteStorage.removeItems(forKeys: []))
    
    XCTAssertFalse(self.kvMixedStorage.removeItems(forKeys: nil))
    XCTAssertFalse(self.kvMixedStorage.removeItems(forKeys: []))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItems(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItems(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItems(forKeys: ["user_max", "user_jian"]))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
  }
  
  func test_removeItemsLargerThanSize() {
    XCTAssertTrue(self.kvFileStorage.removeItemsLargerThanSize(.max))
    
    XCTAssertTrue(self.kvSqliteStorage.removeItemsLargerThanSize(.max))
    
    XCTAssertTrue(self.kvMixedStorage.removeItemsLargerThanSize(.max))
    
    let oneHundredValue = Data(repeating: 1, count: 100)
    let twoHundredValue = Data(repeating: 2, count: 200)
    
    self.kvFileStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvFileStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsLargerThanSize(0))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvFileStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvFileStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsLargerThanSize(150))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 1)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, oneHundredValue)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvSqliteStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsLargerThanSize(0))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvSqliteStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsLargerThanSize(150))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 1)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, oneHundredValue)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvMixedStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsLargerThanSize(0))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvMixedStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsLargerThanSize(150))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 1)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, oneHundredValue)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
  }
  
  func test_removeItemsEarlierThanTime() {
    XCTAssertTrue(self.kvFileStorage.removeItemsEarlierThanTime(0))
    
    XCTAssertTrue(self.kvSqliteStorage.removeItemsEarlierThanTime(0))
    
    XCTAssertTrue(self.kvMixedStorage.removeItemsEarlierThanTime(0))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsEarlierThanTime(.max))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(1_000_000)
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(1_000_000)
    XCTAssertTrue(self.kvFileStorage.removeItemsEarlierThanTime(Int32(Date().timeIntervalSince1970 - 1)))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 1)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsEarlierThanTime(.max))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(1_000_000)
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(1_000_000)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsEarlierThanTime(Int32(Date().timeIntervalSince1970 - 1)))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 1)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsEarlierThanTime(.max))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    usleep(1_000_000)
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(1_000_000)
    XCTAssertTrue(self.kvMixedStorage.removeItemsEarlierThanTime(Int32(Date().timeIntervalSince1970 - 1)))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 1)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
  }
  
  func test_removeItemsToFitSize() {
    XCTAssertTrue(self.kvFileStorage.removeItemsToFitSize(.max))
    
    XCTAssertTrue(self.kvSqliteStorage.removeItemsToFitSize(.max))
    
    XCTAssertTrue(self.kvMixedStorage.removeItemsToFitSize(.max))
    
    let oneHundredValue = Data(repeating: 1, count: 100)
    let twoHundredValue = Data(repeating: 2, count: 200)
    
    self.kvFileStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvFileStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsToFitSize(0))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvFileStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvFileStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsToFitSize(200))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 1)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, twoHundredValue)
    
    self.kvSqliteStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvSqliteStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsToFitSize(0))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvSqliteStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsToFitSize(200))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 1)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, twoHundredValue)
    
    self.kvMixedStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvMixedStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsToFitSize(0))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvMixedStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsToFitSize(200))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"])?.count, 1)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["one_hundred"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["one_hundred", "two_hundred"]).flatMap { $0["two_hundred"] }, twoHundredValue)
  }
  
  func test_removeItemsToFitCount() {
    XCTAssertTrue(self.kvFileStorage.removeItemsToFitCount(.max))
    
    XCTAssertTrue(self.kvSqliteStorage.removeItemsToFitCount(.max))
    
    XCTAssertTrue(self.kvMixedStorage.removeItemsToFitCount(.max))
    
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsToFitCount(0))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvFileStorage.removeItemsToFitCount(1))
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 1)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsToFitCount(0))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvSqliteStorage.removeItemsToFitCount(1))
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 1)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsToFitCount(0))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    usleep(100)
    XCTAssertTrue(self.kvMixedStorage.removeItemsToFitCount(1))
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 1)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, jianValue)
  }
  
  func test_removeAllItems() {
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    XCTAssertTrue(self.kvFileStorage.removeAllItems())
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    
    let kvFileStorageExpectation = self.expectation(description: "test kvFileStorage async removeAllItems")
    
    self.kvFileStorage.removeAllItems(nil, block: { (isCompletion) in
      if isCompletion {
        kvFileStorageExpectation.fulfill()
      }
    })

    self.wait(for: [kvFileStorageExpectation], timeout: 1.0)
    
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvFileStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    XCTAssertTrue(self.kvSqliteStorage.removeAllItems())
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    
    let kvSqliteStorageExpectation = self.expectation(description: "test kvSqliteStorage async removeAllItems")
    
    self.kvSqliteStorage.removeAllItems(nil, block: { (isCompletion) in
      if isCompletion {
        kvSqliteStorageExpectation.fulfill()
      }
    })

    self.wait(for: [kvSqliteStorageExpectation], timeout: 1.0)
    
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvSqliteStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    XCTAssertTrue(self.kvMixedStorage.removeAllItems())
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    
    let kvMixedStorageExpectation = self.expectation(description: "test kvMixedStorage async removeAllItems")
    
    self.kvMixedStorage.removeAllItems(nil, block: { (isCompletion) in
      if isCompletion {
        kvMixedStorageExpectation.fulfill()
      }
    })

    self.wait(for: [kvMixedStorageExpectation], timeout: 1.0)
    
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"])?.count, 0)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_max"] }, nil)
    XCTAssertEqual(self.kvMixedStorage.getItemValues(forKeys: ["user_max", "user_jian"]).flatMap { $0["user_jian"] }, nil)
  }
  
  func test_itemsCount() {
    let maxValue = try? self._baseEncoder.encode(User(name: "max", age: 28))
    let jianValue = try? self._baseEncoder.encode(User(name: "jian", age: 0))
    
    self.kvFileStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvFileStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    XCTAssertEqual(self.kvFileStorage.itemsCount(), 2)
    
    self.kvSqliteStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvSqliteStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    XCTAssertEqual(self.kvSqliteStorage.itemsCount(), 2)
    
    self.kvMixedStorage.saveItem(withKey: "user_max", value: maxValue, filename: "user_max")
    self.kvMixedStorage.saveItem(withKey: "user_jian", value: jianValue, filename: "user_jian")
    XCTAssertEqual(self.kvMixedStorage.itemsCount(), 2)
  }
  
  func test_itemsSize() {
    let oneHundredValue = Data(repeating: 1, count: 100)
    let twoHundredValue = Data(repeating: 2, count: 200)
    
    self.kvFileStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvFileStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    XCTAssertEqual(self.kvFileStorage.itemsSize(), 300)
    
    self.kvSqliteStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvSqliteStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    XCTAssertEqual(self.kvSqliteStorage.itemsSize(), 300)
    
    self.kvMixedStorage.saveItem(withKey: "one_hundred", value: oneHundredValue, filename: "one_hundred")
    self.kvMixedStorage.saveItem(withKey: "two_hundred", value: twoHundredValue, filename: "two_hundred")
    XCTAssertEqual(self.kvMixedStorage.itemsSize(), 300)
  }
  
  func test_set_1_000_key_value_pairs_for_file_storage() {
    var keys = [String]()
    var values = [Int]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
      values.append($0)
    }
    
    print("disk cache set 1_000 key-value pairs for fileStorage")

    /// measured
    ///
    /// average: 0.173
    /// relative standard deviation: 5.564%
    /// values: [0.195358, 0.171570, 0.184378, 0.176587, 0.166625, 0.173907, 0.169895, 0.166717, 0.159061, 0.170744]
    self.measure {
      (0 ..< 1_000).forEach {
        self.kvFileStorage.saveItem(withKey: keys[$0], value: try? NSKeyedArchiver.archivedData(withRootObject: values[$0], requiringSecureCoding: true), filename: keys[$0])
      }
    }
  }
  
  func test_set_1_000_key_value_pairs_for_sqlite_storage() {
    var keys = [String]()
    var values = [Int]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
      values.append($0)
    }
    
    print("disk cache set 1_000 key-value pairs for sqliteStorage")

    /// measured
    ///
    /// average: 0.045
    /// relative standard deviation: 9.353%
    /// values: [0.049412, 0.039152, 0.042112, 0.040455, 0.042365, 0.045352, 0.051956, 0.051039, 0.045645, 0.043312]
    self.measure {
      (0 ..< 1_000).forEach {
        self.kvSqliteStorage.saveItem(withKey: keys[$0], value: try? NSKeyedArchiver.archivedData(withRootObject: values[$0], requiringSecureCoding: true), filename: keys[$0])
      }
    }
  }
  
  func test_set_1_000_key_value_pairs_value_size_100kb_for_file_storage() {
    var keys = [String]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
    }
    
    let value = Data(repeating: 1, count: 100 * 1024)
    
    print("disk cache set 1_000 key-value pairs (value size 100kb) for fileStorage")

    /// measured
    ///
    /// average: 0.205
    /// relative standard deviation: 9.457%
    /// values: [0.251514, 0.204521, 0.196837, 0.192883, 0.196089, 0.199748, 0.194918, 0.197394, 0.231931, 0.185177]
    self.measure {
      (0 ..< 1_000).forEach {
        self.kvFileStorage.saveItem(withKey: keys[$0], value: value, filename: keys[$0])
      }
    }
  }
  
  func test_set_1_000_key_value_pairs_value_size_100kb_for_sqlite_storage() {
    var keys = [String]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
    }
    
    let value = Data(repeating: 1, count: 100 * 1024)
    
    print("disk cache set 1_000 key-value pairs (value size 100kb) for sqliteStorage")

    /// measured
    ///
    /// average: 0.238
    /// relative standard deviation: 18.929%,
    /// values: [0.368510, 0.214494, 0.226523, 0.241791, 0.216688, 0.207740, 0.234227, 0.225912, 0.206559, 0.235274]
    self.measure {
      (0 ..< 1_000).forEach {
        self.kvSqliteStorage.saveItem(withKey: keys[$0], value: value, filename: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_randomly_for_file_storage() {
    var keys = [String]()
    var values = [Int]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
      values.append($0)
    }
    
    (0 ..< 1_000).forEach {
      self.kvFileStorage.saveItem(withKey: keys[$0], value: try? NSKeyedArchiver.archivedData(withRootObject: values[$0], requiringSecureCoding: true), filename: keys[$0])
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache get 1_000 key-value pairs randomly for fileStorage")

    /// measured
    ///
    /// average: 0.047
    /// relative standard deviation: 42.420%
    /// values: [0.105268, 0.047231, 0.040224, 0.039432, 0.039323, 0.039403, 0.038467, 0.038121, 0.039565, 0.038268]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvFileStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_randomly_for_sqlite_storage() {
    var keys = [String]()
    var values = [Int]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
      values.append($0)
    }
    
    (0 ..< 1_000).forEach {
      self.kvSqliteStorage.saveItem(withKey: keys[$0], value: try? NSKeyedArchiver.archivedData(withRootObject: values[$0], requiringSecureCoding: true), filename: keys[$0])
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache set 1_000 key-value pairs randomly for sqliteStorage")

    /// measured
    ///
    /// average: 0.014
    /// relative standard deviation: 32.426%
    /// values: [0.026601, 0.011895, 0.010720, 0.015379, 0.011657, 0.011776, 0.014261, 0.011025, 0.011525, 0.017491]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvSqliteStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_value_size_100kb_randomly_for_file_storage() {
    var keys = [String]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
    }
    
    let value = Data(repeating: 1, count: 100 * 1024)
    
    (0 ..< 1_000).forEach {
      self.kvFileStorage.saveItem(withKey: keys[$0], value: value, filename: keys[$0])
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache get 1_000 key-value pairs (value size 100kb) randomly for fileStorage")

    /// measured
    ///
    /// average: 0.070
    /// relative standard deviation: 17.350%
    /// values: [0.103418, 0.068698, 0.068933, 0.071582, 0.060288, 0.064363, 0.070375, 0.072854, 0.059938, 0.059230]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvFileStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_value_size_100kb_randomly_for_sqlite_storage() {
    var keys = [String]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
    }
    
    let value = Data(repeating: 1, count: 100 * 1024)
    
    (0 ..< 1_000).forEach {
      self.kvSqliteStorage.saveItem(withKey: keys[$0], value: value, filename: keys[$0])
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache get 1_000 key-value pairs (value size 100kb) randomly for sqliteStorage")

    /// measured
    ///
    /// average: 0.085
    /// relative standard deviation: 13.094%
    /// values: [0.080257, 0.071841, 0.070587, 0.078389, 0.080556, 0.093093, 0.083378, 0.089516, 0.095790, 0.108969]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvSqliteStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_none_exist_for_file_storage() {
    var keys = [String]()
    var values = [Int]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
      values.append($0)
    }
    
    (0 ..< 1_000).forEach {
      self.kvFileStorage.saveItem(withKey: keys[$0], value: try? NSKeyedArchiver.archivedData(withRootObject: values[$0], requiringSecureCoding: true), filename: keys[$0])
    }
    
    (0 ..< 1_000).forEach {
      keys.append("\($0 + 1_000)")
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache get 1_000 key-value pairs none exist for fileStorage")

    /// measured
    ///
    /// average: 0.025
    /// relative standard deviation: 9.895%
    /// values: [0.029140, 0.022869, 0.023675, 0.023436, 0.025303, 0.023018, 0.024327, 0.027468, 0.022671, 0.029497]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvFileStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_none_exist_for_sqlite_storage() {
    var keys = [String]()
    var values = [Int]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
      values.append($0)
    }
    
    (0 ..< 1_000).forEach {
      self.kvSqliteStorage.saveItem(withKey: keys[$0], value: try? NSKeyedArchiver.archivedData(withRootObject: values[$0], requiringSecureCoding: true), filename: keys[$0])
    }
    
    (0 ..< 1_000).forEach {
      keys.append("\($0 + 1_000)")
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache set 1_000 key-value pairs none exist for sqliteStorage")

    /// measured
    ///
    /// average: 0.013
    /// relative standard deviation: 33.909%
    /// values: [0.021015, 0.015160, 0.012431, 0.012128, 0.018388, 0.007776, 0.007864, 0.012669, 0.015191, 0.007026]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvSqliteStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_value_size_100kb_none_exist_for_file_storage() {
    var keys = [String]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
    }
    
    let value = Data(repeating: 1, count: 100 * 1024)
    
    (0 ..< 1_000).forEach {
      self.kvFileStorage.saveItem(withKey: keys[$0], value: value, filename: keys[$0])
    }
    
    (0 ..< 1_000).forEach {
      keys.append("\($0 + 1_000)")
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache get 1_000 key-value pairs (value size 100kb) none exist for fileStorage")

    /// measured
    ///
    /// average: 0.037
    /// relative standard deviation: 33.726%
    /// values: [0.073623, 0.035866, 0.030530, 0.031678, 0.038573, 0.032149, 0.031838, 0.030900, 0.034382, 0.030232]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvFileStorage.getItem(forKey: keys[$0])
      }
    }
  }
  
  func test_get_1_000_key_value_pairs_value_size_100kb_none_exist_for_sqlite_storage() {
    var keys = [String]()
    (0 ..< 1_000).forEach {
      keys.append("\($0)")
    }
    
    let value = Data(repeating: 1, count: 100 * 1024)
    
    (0 ..< 1_000).forEach {
      self.kvSqliteStorage.saveItem(withKey: keys[$0], value: value, filename: keys[$0])
    }
    
    (0 ..< 1_000).forEach {
      keys.append("\($0 + 1_000)")
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    print("disk cache get 1_000 key-value pairs (value size 100kb) none exist for sqliteStorage")

    /// measured
    ///
    /// average: 0.044
    /// relative standard deviation: 9.804%
    /// values: [0.054378, 0.041450, 0.045764, 0.038730, 0.041727, 0.040828, 0.040912, 0.041031, 0.046279, 0.044656]
    self.measure {
      (0 ..< 1_000).forEach {
        _ = self.kvSqliteStorage.getItem(forKey: keys[$0])
      }
    }
  }
}
