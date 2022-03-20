//
//  VMMemoryCache.swift
//  Kirogi
//
//  Created by Max on 2022/3/12.
//

#if canImport(Foundation) && canImport(UIKit)

import Foundation
import UIKit

private func _VMMemoryCacheGetReleaseQueue(_ releaseOnMainThread: Bool) -> DispatchQueue {
  return releaseOnMainThread ? DispatchQueue.main : DispatchQueue.global(qos: .utility)
}

private class _VMLinkedMapNode: NSObject {
  
  weak var _prev: _VMLinkedMapNode?
  weak var _next: _VMLinkedMapNode?
  
  var _key: AnyHashable
  var _value: Any
  
  var _cost: UInt
  
  var _time: TimeInterval
  
  init(prev: _VMLinkedMapNode?, next: _VMLinkedMapNode?, key: AnyHashable, value: Any, cost: UInt, time: TimeInterval) {
    self._prev = prev
    self._next = next
    
    self._key = key
    self._value = value
    
    self._cost = cost
    
    self._time = time
  }
}

private class _VMLinkedMap: NSObject {
  
  var _dict: [AnyHashable: _VMLinkedMapNode]
  
  private(set) var _head: _VMLinkedMapNode?
  private(set) var _tail: _VMLinkedMapNode?
  
  var _totalCost: UInt
  
  var _totalCount: UInt
  
  var _releaseOnMainThread: Bool
  var _releaseAsynchronously: Bool
  
  override init() {
    self._dict = [AnyHashable: _VMLinkedMapNode]()
    
    self._head = nil
    self._tail = nil
    
    self._totalCost = 0
    
    self._totalCount = 0
    
    self._releaseOnMainThread = false
    self._releaseAsynchronously = true
    
    super.init()
  }
  
  deinit {
    self._dict.removeAll()
  }
  
  func insertNodeAtHead(_ node: _VMLinkedMapNode) {
    self._dict[node._key] = node
    
    self._totalCost += node._cost
    
    self._totalCount += 1
    
    if self._head != nil {
      node._next = self._head!
      
      self._head!._prev = node
      
      self._head = node
    }
    else {
      self._head = node
      
      self._tail = node
    }
  }
  
  func bringNodeToHead(_ node: _VMLinkedMapNode) {
    guard self._head != node else {
      return
    }
    
    if self._tail == node {
      self._tail = node._prev
      
      self._tail?._next = nil
    }
    else {
      node._next?._prev = node._prev
      node._prev?._next = node._next
    }
    
    node._next = self._head
    node._prev = nil
    
    self._head?._prev = node
    
    self._head = node
  }
  
  func removeNode(_ node: _VMLinkedMapNode) {
    self._dict.removeValue(forKey: node._key)
    
    self._totalCost -= node._cost
    
    self._totalCount -= 1
    
    if node._next != nil {
      node._next!._prev = node._prev
    }
    
    if node._prev != nil {
      node._prev!._next = node._next
    }
    
    if self._head == node {
      self._head = node._next
    }
    
    if self._tail == node {
      self._tail = node._prev
    }
  }
  
  func removeTail() -> _VMLinkedMapNode? {
    guard let tail = self._tail else {
      return nil
    }
    
    self._dict.removeValue(forKey: tail._key)
    
    self._totalCost -= tail._cost
    
    self._totalCount -= 1
    
    if self._head == tail {
      self._head = nil
      self._tail = nil
    }
    else {
      self._tail = tail._prev
      self._tail?._next = nil
    }
    
    return tail
  }
  
  func removeAll() {
    self._totalCost = 0
    
    self._totalCount = 0
    
    self._head = nil
    self._tail = nil
    
    if !self._dict.isEmpty {
      var holder = self._dict
      
      self._dict = [AnyHashable: _VMLinkedMapNode]()
      
      if self._releaseAsynchronously {
        let releaseQueue = _VMMemoryCacheGetReleaseQueue(self._releaseOnMainThread)
        releaseQueue.async {
          holder.removeAll()
        }
      }
      else if self._releaseOnMainThread && pthread_main_np() == 0 {
        DispatchQueue.main.async {
          holder.removeAll()
        }
      }
      else {
        holder.removeAll()
      }
    }
  }
}

internal class VMMemoryCache<Key: Hashable, Value>: NSObject {
  
  var name: String?
  
  var totalCost: UInt {
    self._lock.lock()
    
    let totalCost = self._lru._totalCost
    
    self._lock.unlock()
    
    return totalCost
  }
  
  var totalCount: UInt {
    self._lock.lock()
    
    let totalCount = self._lru._totalCount
    
    self._lock.unlock()
    
    return totalCount
  }
  
  var costLimit: UInt
  
  var countLimit: UInt
  
  var ageLimit: TimeInterval
  
  var autoTrimInterval: TimeInterval
  
  var shouldRemoveAllOnMemoryWarning: Bool
  
  var shouldRemoveAllWhenEnterBackground: Bool
  
  var releaseOnMainThread: Bool {
    get {
      self._lock.lock()
      
      let releaseOnMainThread = self._lru._releaseOnMainThread
      
      self._lock.unlock()
      
      return releaseOnMainThread
    }
    set {
      self._lock.lock()
      
      self._lru._releaseOnMainThread = newValue
      
      self._lock.unlock()
    }
  }
  
  var releaseAsynchronously: Bool {
    get {
      self._lock.lock()
      
      let releaseAsynchronously = self._lru._releaseAsynchronously
      
      self._lock.unlock()
      
      return releaseAsynchronously
    }
    set {
      self._lock.lock()
      
      self._lru._releaseAsynchronously = newValue
      
      self._lock.unlock()
    }
  }
  
  private let _queue: DispatchQueue
  
  private let _lock: NSLock
  
  private var _lru: _VMLinkedMap
  
  static func initialize() -> VMMemoryCache {
    let memoryCache = VMMemoryCache()
    
    return memoryCache
  }
  
  private override init() {
    self.costLimit = .max
    self.countLimit = .max
    self.ageLimit = .greatestFiniteMagnitude
    
    self.autoTrimInterval = 5.0
    
    self.shouldRemoveAllOnMemoryWarning = true
    
    self.shouldRemoveAllWhenEnterBackground = true
    
    self._queue = DispatchQueue(label: "com.max.jian.Kirogi.cache.memory")
    
    self._lock = NSLock()
    
    self._lru = _VMLinkedMap()
    
    super.init()
    
    self.releaseOnMainThread = false
    self.releaseAsynchronously = true
    
    self._addObserver()
    
    self._trimRecursively()
  }
  
  deinit {
    self._removeObserver()
    
    self._lru.removeAll()
  }
  
  func contains(forKey key: Key?) -> Bool {
    guard let key = key else {
      return false
    }
    
    self._lock.lock()
    
    let containsResult = self._lru._dict.contains(where: { $0.key == key as AnyHashable })
    
    self._lock.unlock()
    
    return containsResult
  }
  
  func setObject(_ object: Value?, forKey key: Key?) {
    self.setObject(object, forKey: key, withCost: 0)
  }
  
  func setObject(_ object: Value?, forKey key: Key?, withCost cost: UInt) {
    guard let key = key else {
      return
    }
    
    guard let object = object else {
      self.removeObject(forKey: key)
      
      return
    }
    
    self._lock.lock()
    
    let nowTime = ProcessInfo.processInfo.systemUptime
    
    var node = self._lru._dict[key]
    if node != nil {
      self._lru._totalCost -= node!._cost
      self._lru._totalCost += cost
      
      node!._value = object
      
      node!._cost = cost
      
      node!._time = nowTime
      
      self._lru.bringNodeToHead(node!)
    }
    else {
      node = _VMLinkedMapNode(prev: nil, next: nil, key: key, value: object, cost: cost, time: nowTime)
      
      self._lru.insertNodeAtHead(node!)
    }
    
    if self._lru._totalCost > self.costLimit {
      self._queue.async {
        self.trim(forCost: self.costLimit)
      }
    }
    
    if self._lru._totalCount > self.countLimit, let tailNode = self._lru.removeTail() {
      if self._lru._releaseAsynchronously {
        let releaseQueue = _VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
        releaseQueue.async {
          _ = tailNode.classForCoder
        }
      }
      else if self._lru._releaseOnMainThread && pthread_main_np() == 0 {
        DispatchQueue.main.async {
          _ = tailNode.classForCoder
        }
      }
    }
    
    self._lock.unlock()
  }
  
  func object(forKey key: Key?) -> Value? {
    guard let key = key else {
      return nil
    }
    
    self._lock.lock()
    
    let node = self._lru._dict[key]
    if node != nil {
      node!._time = ProcessInfo.processInfo.systemUptime
      
      self._lru.bringNodeToHead(node!)
    }
    
    let objectResult = node != nil ? node!._value as? Value : nil
    
    self._lock.unlock()
    
    return objectResult
  }
  
  func removeObject(forKey key: Key?) {
    guard let key = key else {
      return
    }
    
    self._lock.lock()
    
    let node = self._lru._dict[key]
    if node != nil {
      self._lru.removeNode(node!)
      
      if self._lru._releaseAsynchronously {
        let releaseQueue = _VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
        releaseQueue.async {
          _ = node!.classForCoder
        }
      }
      else if self._lru._releaseOnMainThread && pthread_main_np() == 0 {
        DispatchQueue.main.async {
          _ = node!.classForCoder
        }
      }
    }
    
    self._lock.unlock()
  }
  
  func removeAllObjects() {
    self._lock.lock()
    
    self._lru.removeAll()
    
    self._lock.unlock()
  }
  
  func trim(forCost costLimit: UInt) {
    self._trim(forCost: costLimit)
  }
  
  func trim(forCount countLimit: UInt) {
    self._trim(forCount: countLimit)
  }
  
  func trim(forAge ageLimit: TimeInterval) {
    self._trim(forAge: ageLimit)
  }
}

extension VMMemoryCache {
  
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
      
      self._trim(forCost: self.costLimit)
      self._trim(forCount: self.countLimit)
      self._trim(forAge: self.ageLimit)
    }
  }
  
  private func _trim(forCost costLimit: UInt) {
    var trimNotFinished = true
    self._lock.lock()
    
    if costLimit == 0 {
      self._lru.removeAll()
      trimNotFinished = false
    }
    else if self._lru._totalCost <= costLimit {
      trimNotFinished = false
    }
    
    self._lock.unlock()
    
    guard trimNotFinished else {
      return
    }
    
    var holder = [_VMLinkedMapNode]()
    while trimNotFinished {
      let tryLockResult = self._lock.try()
      
      if tryLockResult {
        if self._lru._totalCost > costLimit {
          if let tailNode = self._lru.removeTail() {
            holder.append(tailNode)
          }
        }
        else {
          trimNotFinished = false
        }
        
        self._lock.unlock()
      }
      else {
        usleep(10 * 1000) // sleep 10 ms
      }
    }
    
    if !holder.isEmpty {
      let releaseQueue = _VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
      releaseQueue.async {
        holder.removeAll()
      }
    }
  }
  
  private func _trim(forCount countLimit: UInt) {
    var trimNotFinished = true
    self._lock.lock()
    
    if countLimit == 0 {
      self._lru.removeAll()
      trimNotFinished = false
    }
    else if self._lru._totalCount <= countLimit {
      trimNotFinished = false
    }
    
    self._lock.unlock()
    
    guard trimNotFinished else {
      return
    }
    
    var holder = [_VMLinkedMapNode]()
    while trimNotFinished {
      let tryLockResult = self._lock.try()
      
      if tryLockResult {
        if self._lru._totalCount > countLimit {
          if let tailNode = self._lru.removeTail() {
            holder.append(tailNode)
          }
        }
        else {
          trimNotFinished = false
        }
        
        self._lock.unlock()
      }
      else {
        usleep(10 * 1000) // sleep 10 ms
      }
    }
    
    if !holder.isEmpty {
      let releaseQueue = _VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
      releaseQueue.async {
        holder.removeAll()
      }
    }
  }
  
  private func _trim(forAge ageLimit: TimeInterval) {
    let nowTime = ProcessInfo.processInfo.systemUptime
    
    var trimNotFinished = true
    self._lock.lock()
    
    if ageLimit <= 0 {
      self._lru.removeAll()
      trimNotFinished = false
    }
    else if (self._lru._tail == nil || (nowTime - self._lru._tail!._time) <= ageLimit) {
      trimNotFinished = false
    }
    
    self._lock.unlock()
    
    guard trimNotFinished else {
      return
    }
    
    var holder = [_VMLinkedMapNode]()
    while trimNotFinished {
      let tryLockResult = self._lock.try()
      
      if tryLockResult {
        if (self._lru._tail != nil && (nowTime - self._lru._tail!._time) > ageLimit) {
          if let tailNode = self._lru.removeTail() {
            holder.append(tailNode)
          }
        }
        else {
          trimNotFinished = false
        }
        
        self._lock.unlock()
      }
      else {
        usleep(10 * 1000) // sleep 10 ms
      }
    }
    
    if !holder.isEmpty {
      let releaseQueue = _VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
      releaseQueue.async {
        holder.removeAll()
      }
    }
  }
}

extension VMMemoryCache {
  
  private func _addObserver() {
    NotificationCenter.default.addObserver(forName: UIApplication.didReceiveMemoryWarningNotification, object: nil, queue: nil) { [weak self] (notification) in
      self?._appDidReceiveMemoryWarning(notification)
    }
    NotificationCenter.default.addObserver(forName: UIApplication.didEnterBackgroundNotification, object: nil, queue: nil) { [weak self] (notification) in
      self?._appDidEnterBackground(notification)
    }
  }
  
  private func _removeObserver() {
    NotificationCenter.default.removeObserver(self, name: UIApplication.didReceiveMemoryWarningNotification, object: nil)
    NotificationCenter.default.removeObserver(self, name: UIApplication.didEnterBackgroundNotification, object: nil)
  }
  
  private func _appDidReceiveMemoryWarning(_ notification: Notification) {
    if self.shouldRemoveAllOnMemoryWarning {
      self.removeAllObjects()
    }
  }
  
  private func _appDidEnterBackground(_ notification: Notification) {
    if self.shouldRemoveAllWhenEnterBackground {
      self.removeAllObjects()
    }
  }
}

#endif
