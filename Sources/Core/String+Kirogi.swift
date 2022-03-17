//
//  String+Kirogi.swift
//  Kirogi
//
//  Created by Max on 2022/3/17.
//

#if canImport(Foundation)

import Foundation

extension String {
  
  func appendingPathComponent(_ aString: String) -> String {
    return URL(fileURLWithPath: self).appendingPathComponent(aString).path
  }
}

#endif
