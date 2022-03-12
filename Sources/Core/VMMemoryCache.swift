//
//  VMMemoryCache.swift
//  Kirogi
//
//  Created by Max on 2022/3/12.
//

#if canImport(Foundation)

import Foundation

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
  
  var _dict: [AnyHashable: Any]
  
  private(set) var _head: _VMLinkedMapNode?
  private(set) var _tail: _VMLinkedMapNode?
  
  var _totalCost: UInt
  
  var _totalCount: UInt
  
  var _releaseOnMainThread: Bool
  var _releaseAsynchronously: Bool
  
  override init() {
    self._dict = [AnyHashable: Any]()
    
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
    self._dict[node._key] = node._value
    
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
      
      self._dict = [AnyHashable: Any]()
      
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

#endif
