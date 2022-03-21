//
//  VMCache.swift
//  Kirogi
//
//  Created by Max on 2022/3/21.
//

#if canImport(Foundation)

import Foundation

public class VMCache<Key: Hashable, Value: Codable>: NSObject {

  private(set) public var name: String?
  
  private(set) public var memoryCache: VMMemoryCache<Key, Value>
  private(set) public var diskCache: VMDiskCache<Key, Value>
  
  public static func initialize(name: String?) -> VMCache? {
    let cache = VMCache(name: name)
    
    return cache
  }
  
  public static func initialize(path: String?) -> VMCache? {
    let cache = VMCache(path: path)
    
    return cache
  }
  
  private convenience init?(name: String?) {
    guard let name = name, !name.isEmpty else {
      return nil
    }
    
    let cacheFolderPath = NSSearchPathForDirectoriesInDomains(.cachesDirectory, .userDomainMask, true).first
    let path = cacheFolderPath?.appendingPathComponent(name)
    
    self.init(path: path)
  }
  
  private init?(path: String?) {
    let diskCache: VMDiskCache<Key, Value>? = .initialize(path: path)
    guard diskCache != nil else {
      return nil
    }
    
    let name = path?.lastPathComponent()
    
    let memoryCache: VMMemoryCache<Key, Value> = .initialize()
    memoryCache.name = name
    
    diskCache?.name = name
    
    self.name = name
    
    self.memoryCache = memoryCache
    self.diskCache = diskCache!
  }
  
  public func contains(forKey key: Key?) -> Bool {
    return self.memoryCache.contains(forKey: key) || self.diskCache.contains(forKey: key)
  }
  
  public func contains(forKey key: Key?, block: ((Key?, Bool) -> Void)?) {
    guard block != nil else {
      return
    }
    
    let containsInMemory = self.memoryCache.contains(forKey: key)
    
    if containsInMemory {
      DispatchQueue.global().async {
        block!(key, containsInMemory)
      }
    }
    else {
      self.diskCache.contains(forKey: key, block: block)
    }
  }
  
  public func setObject(_ object: Value?, forKey key: Key?) {
    self.memoryCache.setObject(object, forKey: key)
    self.diskCache.setObject(object, forKey: key)
  }
  
  public func setObject(_ object: Value?, forKey key: Key?, block: (() -> Void)?) {
    self.memoryCache.setObject(object, forKey: key)
    self.diskCache.setObject(object, forKey: key, block: block)
  }
  
  public func object(forKey key: Key?) -> Value? {
    var object = self.memoryCache.object(forKey: key)
    
    if object == nil {
      object = self.diskCache.object(forKey: key)
      
      if object != nil {
        self.memoryCache.setObject(object!, forKey: key)
      }
    }
    
    return object
  }
  
  public func object(forKey key: Key?, block: ((Key?, Value?) -> Void)?) {
    guard block != nil else {
      return
    }
    
    let objectInMemory = self.memoryCache.object(forKey: key)
    
    if objectInMemory != nil {
      DispatchQueue.global().async {
        block!(key, objectInMemory)
      }
    }
    else {
      self.diskCache.object(forKey: key) { [weak self] (key, objectInDisk) in
        guard let self = self else {
          return
        }
        
        if self.memoryCache.object(forKey: key) == nil && objectInDisk != nil {
          self.memoryCache.setObject(objectInDisk!, forKey: key)
        }
        
        block!(key, objectInDisk)
      }
    }
  }
  
  public func removeObject(forKey key: Key?) {
    self.memoryCache.removeObject(forKey: key)
    self.diskCache.removeObject(forKey: key)
  }
  
  public func removeObject(forKey key: Key?, block: ((Key?) -> Void)?) {
    self.memoryCache.removeObject(forKey: key)
    self.diskCache.removeObject(forKey: key, block: block)
  }
  
  public func removeAllObjects() {
    self.memoryCache.removeAllObjects()
    self.diskCache.removeAllObjects()
  }
  
  public func removeAllObjects(_ block: (() -> Void)?) {
    self.memoryCache.removeAllObjects()
    self.diskCache.removeAllObjects(block)
  }
  
  public func removeAllObjects(_ progress: ((Int, Int) -> Void)?, completion: ((Bool) -> Void)?) {
    self.memoryCache.removeAllObjects()
    self.diskCache.removeAllObjects(progress, completion: completion)
  }
}

#endif
