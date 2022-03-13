//
//  VMMemoryCache.swift
//  Kirogi
//
//  Created by Max on 2022/3/12.
//

#if canImport(Foundation) && canImport(UIKit)

import Foundation
import UIKit

func VMMemoryCacheGetReleaseQueue(_ releaseOnMainThread: Bool) -> DispatchQueue {
  return releaseOnMainThread ? DispatchQueue.main : DispatchQueue.global(qos: .utility)
}

fileprivate class _VMLinkedMapNode: NSObject {
  
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

fileprivate class _VMLinkedMap: NSObject {
  
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
        let releaseQueue = VMMemoryCacheGetReleaseQueue(self._releaseOnMainThread)
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

public class VMMemoryCache: NSObject {
  
  public var name: String?
  
  public var totalCost: UInt {
    pthread_mutex_lock(&self._lock)
    
    let totalCost = self._lru._totalCost
    
    pthread_mutex_unlock(&self._lock)
    
    return totalCost
  }
  
  public var totalCount: UInt {
    pthread_mutex_lock(&self._lock)
    
    let totalCount = self._lru._totalCount
    
    pthread_mutex_unlock(&self._lock)
    
    return totalCount
  }
  
  public var costLimit: UInt
  
  public var countLimit: UInt
  
  public var ageLimit: TimeInterval
  
  public var autoTrimInterval: TimeInterval
  
  public var shouldRemoveAllOnMemoryWarning: Bool
  
  public var shouldRemoveAllWhenEnterBackground: Bool
  
  public var releaseOnMainThread: Bool {
    get {
      pthread_mutex_lock(&self._lock)
      
      let releaseOnMainThread = self._lru._releaseOnMainThread
      
      pthread_mutex_unlock(&self._lock)
      
      return releaseOnMainThread
    }
    set {
      pthread_mutex_lock(&self._lock)
      
      self._lru._releaseOnMainThread = newValue
      
      pthread_mutex_unlock(&self._lock)
    }
  }
  
  public var releaseAsynchronously: Bool {
    get {
      pthread_mutex_lock(&self._lock)
      
      let releaseAsynchronously = self._lru._releaseAsynchronously
      
      pthread_mutex_unlock(&self._lock)
      
      return releaseAsynchronously
    }
    set {
      pthread_mutex_lock(&self._lock)
      
      self._lru._releaseAsynchronously = newValue
      
      pthread_mutex_unlock(&self._lock)
    }
  }
  
  private let _queue: DispatchQueue
  
  private var _lock: pthread_mutex_t = pthread_mutex_t()
  
  private var _lru: _VMLinkedMap
  
  public override init() {
    self.costLimit = UInt.max
    self.countLimit = UInt.max
    self.ageLimit = TimeInterval.greatestFiniteMagnitude
    
    self.autoTrimInterval = 5.0
    
    self.shouldRemoveAllOnMemoryWarning = true
    
    self.shouldRemoveAllWhenEnterBackground = true
    
    self._queue = DispatchQueue(label: "com.max.jian.Kirogi.cache.memory")
    
    pthread_mutex_init(&self._lock, nil)
    
    self._lru = _VMLinkedMap()
    
    super.init()
    
    self.releaseOnMainThread = false
    self.releaseAsynchronously = true
    
    NotificationCenter.default.addObserver(self, selector: #selector(_appDidReceiveMemoryWarning(_:)), name: UIApplication.didReceiveMemoryWarningNotification, object: nil)
    NotificationCenter.default.addObserver(self, selector: #selector(_appDidEnterBackground(_:)), name: UIApplication.didEnterBackgroundNotification, object: nil)
    
    self._trimRecursively()
  }
  
  deinit {
    NotificationCenter.default.removeObserver(self, name: UIApplication.didReceiveMemoryWarningNotification, object: nil)
    NotificationCenter.default.removeObserver(self, name: UIApplication.didEnterBackgroundNotification, object: nil)
    
    self._lru.removeAll()
    
    pthread_mutex_destroy(&self._lock)
  }
  
  public func contains(forKey key: AnyHashable?) -> Bool {
    guard let key = key else {
      return false
    }
    
    pthread_mutex_lock(&self._lock)
    
    let containsResult = self._lru._dict.contains(where: { $0.key == key })
    
    pthread_mutex_unlock(&self._lock)
    
    return containsResult
  }
  
  public func setObject(_ object: Any?, forKey key: AnyHashable?) {
    self.setObject(object, forKey: key, withCost: 0)
  }
  
  public func setObject(_ object: Any?, forKey key: AnyHashable?, withCost cost: UInt) {
    guard let key = key else {
      return
    }
    
    guard let object = object else {
      self.removeObject(forKey: key)
      
      return
    }
    
    pthread_mutex_lock(&self._lock)
    
    let nowTime = CACurrentMediaTime()
    
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
        let releaseQueue = VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
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
    
    pthread_mutex_unlock(&self._lock)
  }
  
  public func object(forKey key: AnyHashable?) -> Any? {
    guard let key = key else {
      return nil
    }
    
    pthread_mutex_lock(&self._lock)
    
    let node = self._lru._dict[key]
    if node != nil {
      node!._time = CACurrentMediaTime()
      
      self._lru.bringNodeToHead(node!)
    }
    
    let value = node != nil ? node!._value : nil
    
    pthread_mutex_unlock(&self._lock)
    
    return value
  }
  
  public func removeObject(forKey key: AnyHashable?) {
    guard let key = key else {
      return
    }
    
    pthread_mutex_lock(&self._lock)
    
    let node = self._lru._dict[key]
    if node != nil {
      self._lru.removeNode(node!)
      
      if self._lru._releaseAsynchronously {
        let releaseQueue = VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
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
    
    pthread_mutex_unlock(&self._lock)
  }
  
  public func removeAllObjects() {
    pthread_mutex_lock(&self._lock)
    
    self._lru.removeAll()
    
    pthread_mutex_unlock(&self._lock)
  }
  
  public func trim(forCost costLimit: UInt) {
    self._trim(forCost: costLimit)
  }
  
  public func trim(forCount countLimit: UInt) {
    self._trim(forCount: countLimit)
  }
  
  public func trim(forAge ageLimit: TimeInterval) {
    self._trim(forAge: ageLimit)
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
    self._queue.async {
      self._trim(forCost: self.costLimit)
      self._trim(forCount: self.countLimit)
      self._trim(forAge: self.ageLimit)
    }
  }
  
  private func _trim(forCost costLimit: UInt) {
    var trimNotFinished = true
    pthread_mutex_lock(&self._lock)
    
    if costLimit == 0 {
      self._lru.removeAll()
      trimNotFinished = false
    }
    else if self._lru._totalCost <= costLimit {
      trimNotFinished = false
    }
    
    pthread_mutex_unlock(&self._lock)
    
    guard trimNotFinished else {
      return
    }
    
    var holder = [_VMLinkedMapNode]()
    while trimNotFinished {
      let tryLockResult = pthread_mutex_trylock(&self._lock)
      
      if tryLockResult == 0 {
        if self._lru._totalCost > costLimit {
          if let tailNode = self._lru.removeTail() {
            holder.append(tailNode)
          }
        }
        else {
          trimNotFinished = false
        }
        
        pthread_mutex_unlock(&self._lock)
      }
      else {
        usleep(10 * 1000) // sleep 10 ms
      }
    }
    
    if !holder.isEmpty {
      let releaseQueue = VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
      releaseQueue.async {
        holder.removeAll()
      }
    }
  }
  
  private func _trim(forCount countLimit: UInt) {
    var trimNotFinished = true
    pthread_mutex_lock(&self._lock)
    
    if countLimit == 0 {
      self._lru.removeAll()
      trimNotFinished = false
    }
    else if self._lru._totalCount <= countLimit {
      trimNotFinished = false
    }
    
    pthread_mutex_unlock(&self._lock)
    
    guard trimNotFinished else {
      return
    }
    
    var holder = [_VMLinkedMapNode]()
    while trimNotFinished {
      let tryLockResult = pthread_mutex_trylock(&self._lock)
      
      if tryLockResult == 0 {
        if self._lru._totalCount > countLimit {
          if let tailNode = self._lru.removeTail() {
            holder.append(tailNode)
          }
        }
        else {
          trimNotFinished = false
        }
        
        pthread_mutex_unlock(&self._lock)
      }
      else {
        usleep(10 * 1000) // sleep 10 ms
      }
    }
    
    if !holder.isEmpty {
      let releaseQueue = VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
      releaseQueue.async {
        holder.removeAll()
      }
    }
  }
  
  private func _trim(forAge ageLimit: TimeInterval) {
    let nowTime = CACurrentMediaTime()
    
    var trimNotFinished = true
    pthread_mutex_lock(&self._lock)
    
    if ageLimit <= 0 {
      self._lru.removeAll()
      trimNotFinished = false
    }
    else if (self._lru._tail == nil || (nowTime - self._lru._tail!._time) <= ageLimit) {
      trimNotFinished = false
    }
    
    pthread_mutex_unlock(&self._lock)
    
    guard trimNotFinished else {
      return
    }
    
    var holder = [_VMLinkedMapNode]()
    while trimNotFinished {
      let tryLockResult = pthread_mutex_trylock(&self._lock)
      
      if tryLockResult == 0 {
        if (self._lru._tail != nil && (nowTime - self._lru._tail!._time) > ageLimit) {
          if let tailNode = self._lru.removeTail() {
            holder.append(tailNode)
          }
        }
        else {
          trimNotFinished = false
        }
        
        pthread_mutex_unlock(&self._lock)
      }
      else {
        usleep(10 * 1000) // sleep 10 ms
      }
    }
    
    if !holder.isEmpty {
      let releaseQueue = VMMemoryCacheGetReleaseQueue(self._lru._releaseOnMainThread)
      releaseQueue.async {
        holder.removeAll()
      }
    }
  }
  
  @objc private func _appDidReceiveMemoryWarning(_ notification: Notification) {
    if self.shouldRemoveAllOnMemoryWarning {
      self.removeAllObjects()
    }
  }
  
  @objc private func _appDidEnterBackground(_ notification: Notification) {
    if self.shouldRemoveAllWhenEnterBackground {
      self.removeAllObjects()
    }
  }
}

#endif
