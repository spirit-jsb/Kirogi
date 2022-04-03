//
//  CSQLite+Kirogi.swift
//  Kirogi
//
//  Created by max on 2022/4/3.
//

#if canImport(Foundation) && canImport(CSQLite)

import Foundation
import CSQLite

let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

#endif
