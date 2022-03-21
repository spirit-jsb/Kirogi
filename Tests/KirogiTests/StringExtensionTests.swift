//
//  StringExtensionTests.swift
//  KirogiTests
//
//  Created by Max on 2022/3/21.
//

import XCTest
@testable import Kirogi

class StringExtensionTests: XCTestCase {
  
  func test_lastPathComponent() {
    XCTAssertEqual("/tmp/scratch.tiff".lastPathComponent, "scratch.tiff")
    XCTAssertEqual("/tmp/scratch".lastPathComponent, "scratch")
    XCTAssertEqual("/tmp/".lastPathComponent, "tmp")
    XCTAssertEqual("scratch///".lastPathComponent, "scratch")
    XCTAssertEqual("/".lastPathComponent, "/")
  }
  
  func test_md5() {
    XCTAssertEqual("hello kirogi".md5(), "91eea5ec5a4f486d7f51723c8d12d650")
  }
  
  func test_stringByAppendingPathComponent() {
    XCTAssertEqual("/tmp".stringByAppendingPathComponent("scratch.tiff"), "/tmp/scratch.tiff")
    XCTAssertEqual("/tmp/".stringByAppendingPathComponent("scratch.tiff"), "/tmp/scratch.tiff")
    XCTAssertEqual("/".stringByAppendingPathComponent("scratch.tiff"), "/scratch.tiff")
    XCTAssertEqual("".stringByAppendingPathComponent("scratch.tiff"), "scratch.tiff")
  }
}
