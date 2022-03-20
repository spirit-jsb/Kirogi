//
//  Hasher+Kirogi.swift
//  Kirogi
//
//  Created by Max on 2022/3/21.
//

#if canImport(Foundation)

import Foundation

extension Hasher {
  
  /// https://github.com/apple/swift/blob/main/stdlib/public/core/SipHash.swift
  internal struct _State {
    // "somepseudorandomlygeneratedbytes"
    private var v0: UInt64 = 0x736f6d6570736575
    private var v1: UInt64 = 0x646f72616e646f6d
    private var v2: UInt64 = 0x6c7967656e657261
    private var v3: UInt64 = 0x7465646279746573
    // The fields below are reserved for future use. They aren't currently used.
    private var v4: UInt64 = 0
    private var v5: UInt64 = 0
    private var v6: UInt64 = 0
    private var v7: UInt64 = 0
  }
  
  static func nonrandomHasher() -> Hasher {
    let offset = MemoryLayout<Hasher>.size - MemoryLayout<_State>.size
    
    var hasher = Hasher()
    
    withUnsafeMutableBytes(of: &hasher) { (pointer) in
      pointer.baseAddress!.storeBytes(of: _State(), toByteOffset: offset, as: _State.self)
    }
    
    return hasher
  }
}

#endif
