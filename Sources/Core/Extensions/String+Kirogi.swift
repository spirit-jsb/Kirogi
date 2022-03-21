//
//  String+Kirogi.swift
//  Kirogi
//
//  Created by Max on 2022/3/17.
//

#if canImport(Foundation) && canImport(CryptoKit)

import Foundation
import CryptoKit

extension String {
  
  var lastPathComponent: String {
    return (self as NSString).lastPathComponent
  }
  
  func md5() -> String? {
    return self.data(using: .utf8).flatMap { Insecure.MD5.hash(data: $0) }.flatMap { $0.map { String(format: "%02x", $0) }.joined() }
  }
  
  func stringByAppendingPathComponent(_ aString: String) -> String {
    return (self as NSString).appendingPathComponent(aString) as String
  }
}

#endif
