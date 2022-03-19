//
//  NSLock+Kirogi.swift
//  Kirogi
//
//  Created by Max on 2022/3/19.
//

#if canImport(Foundation)

import Foundation

extension NSLock {
  
  @discardableResult
  func locked<T>(_ function: () throws -> T) rethrows -> T {
    self.lock()
    defer {
      self.unlock()
    }
        
    return try function()
  }
  
  @discardableResult
  func tryLocked<T>(_ function: (Bool) throws -> T) rethrows -> T {
    let tryLockResult = self.try()
    defer {
      if tryLockResult {
        self.unlock()
      }
    }
        
    return try function(tryLockResult)
  }
}

#endif
