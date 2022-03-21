//
//  OperatorExtensionTests.swift
//  KirogiTests
//
//  Created by Max on 2022/3/22.
//

import XCTest
@testable import Kirogi

class OperatorExtensionTests: XCTestCase {
   
  func test_incrementOperator() {
    var num: Int32 = 5
    
    XCTAssertEqual(num++, 5)
  }
}
