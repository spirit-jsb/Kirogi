//
//  Operator+Kirogi.swift
//  Kirogi
//
//  Created by max on 2022/3/18.
//

#if canImport(Foundation)

import Foundation

postfix operator ++
postfix func ++ (left: inout Int32) -> Int32 {
  defer {
    left += 1
  }
  return left
}

#endif
