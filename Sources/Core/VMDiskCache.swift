//
//  VMDiskCache.swift
//  Kirogi
//
//  Created by Max on 2022/3/13.
//

#if canImport(Foundation)

import Foundation

public class VMDiskCache: NSObject {
  
  public var name: String?
  
  private(set) public var path: String
  
  private(set) public var inlineThreshold: UInt
  
  public var costLimit: UInt
  
  public var countLimit: UInt
  
  public var ageLimit: TimeInterval
  
  public var freeDiskSpaceLimit: UInt
  
  public var autoTrimInterval: TimeInterval
  
  public var errorLogsEnabled: Bool
  
  public init?(path: String) {
    
  }
  
  public init?(path: String, inlineThreshold: UInt) {
    
  }
  
  public func contains(forKey key: String?) -> Bool {
    
  }
  
  public func contains(forKey key: String?, completion: ((String?, Bool) -> Void)?) {
    
  }
  
  public func setObject(_ object: Any?, forKey key: String?) {
    
  }
  
  public func setObject(_ object: Any?, forKey key: String?, completion: (() -> Void)?) {
    
  }
  
  public func object(forKey key: String?) -> Any? {
    
  }
  
  public func object(forKey key: String?, completion: ((String?, Any?) -> Void)?) {
    
  }
  
  public func removeObject(forKey key: String?) {
    
  }
  
  public func removeObject(forKey key: String?, completion: ((String?) -> Void)?) {
    
  }
  
  public func removeAllObjects() {
    
  }
  
  public func removeAllObjects(_ completion: (() -> Void)?) {
    
  }
  
  public func removeAllObjects(_ progress: ((Int, Int) -> Void)?, completion: ((Bool) -> Void)?) {
  
  }
  
  public func totalCost() -> Int {
    
  }
  
  public func totalCost(_ completion: ((Int) -> Void)?) {
    
  }
  
  public func totalCount() -> Int {
    
  }
  
  public func totalCount(_ completion: ((Int) -> Void)?) {
    
  }
  
  public func trim(forCost costLimit: UInt) {
    
  }
  
  public func trim(forCost costLimit: UInt, completion: (() -> Void)?) {
    
  }
  
  public func trim(forCount countLimit: UInt) {
    
  }
  
  public func trim(forCount countLimit: UInt, completion: (() -> Void)?) {
    
  }
  
  public func trim(forAge ageLimit: TimeInterval) {
    
  }
  
  public func trim(forAge ageLimit: TimeInterval, completion: (() -> Void)?) {
    
  }
}

#endif
