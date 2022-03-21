//
//  HasherExtensionTests.swift
//  KirogiTests
//
//  Created by Max on 2022/3/22.
//

import XCTest
@testable import Kirogi

class HasherExtensionTests: XCTestCase {
  
  func test_nonrandomHasher() {
    let key = "kirogi_key"
    
    var nonrandomHasher = Hasher.nonrandomHasher()
    key.hash(into: &nonrandomHasher)
    
    XCTAssertEqual(nonrandomHasher.finalize(), 9123922099170162061)
  }
}
